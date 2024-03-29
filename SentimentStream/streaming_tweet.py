"""A streaming python pipeline to read in pubsub tweets and perform classification"""

from __future__ import absolute_import

import argparse
import datetime
import json
import logging
import random

import numpy as np

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.transforms.util import BatchElements
from flair.models import TextClassifier
from flair.data import Sentence

classifier = TextClassifier.load('sentiment')

# from googleapiclient import discovery

def estimate(messages):

    # Be able to cope with a single string as well. Load because messages from pubsub are JSON strings
    if not isinstance(messages, list):
        messages = [json.loads(messages)]
    else:
        messages = [json.loads(instance) for instance in messages]

    # Estimate the sentiment of the 'text' of each tweet
    transformedSentences = [Sentence(instance['text']) for instance in messages]
    ([classifier.predict(sentence) for sentence in transformedSentences])

    # Join them together
    for i, instance in enumerate(messages):
        instance['sentiment'] = transformedSentences[i].to_dict()['labels'][0]['value']
        instance['sentiment_confidence'] = transformedSentences[0].to_dict()['labels'][0]['confidence']

    logging.info(str(len(messages)) + " -processed in sentiment batch")
    return messages

def estimater(messages):

    # Be able to cope with a single string as well
    if not isinstance(messages, list):
        messages = [json.loads(messages)]
    else:
        messages = [json.loads(instance) for instance in messages]

    logging.info(str(len(messages)) + " -processed in raw tweet batch")
    return messages

def aggregate_format(key_values):
    # Aggregate tweets per 30 second window
    (key, values) = key_values
    logging.info("here are the key_values")
    logging.info(key_values)

    mean_sentiment = np.mean([(x['sentiment_confidence'] * (-1 if x['sentiment'] == 'NEGATIVE' else 1)) for x in values])
    mean_timestamp = datetime.datetime.utcfromtimestamp(np.mean([
        (datetime.datetime.strptime(x["created_at"], '%Y-%m-%d %H:%M:%S')- datetime.datetime.fromtimestamp(
            0)).total_seconds() for x in values
    ]))

    logging.info("mean sentiment" + str(mean_sentiment) + " | mean timestamp" + str(mean_timestamp))

    # Return in correct format, according to BQ schema
    return {"created_at": mean_timestamp.strftime('%Y-%m-%d %H:%M:%S'), "sentiment_confidence": float(mean_sentiment)}

class TopicOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_topic',
            type=str,
            help='Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>."',
            default="projects/twitter-streams-345620/topics/twitterstream")

def run(argv=None):
    # Main pipeline run def

    # Make explicit BQ schema for output tables
    bigqueryschema_sentiment_json = '{"fields": [' \
                          '{"name":"text","type":"STRING"},' \
                          '{"name":"tweet_id","type":"STRING"},' \
                          '{"name":"user_id","type":"STRING"},' \
                          '{"name":"verified","type":"BOOLEAN"},' \
                          '{"name":"location","type":"STRING"},' \
                          '{"name":"favorite_count","type":"INTEGER"},' \
                          '{"name":"retweet_count","type":"INTEGER"},' \
                          '{"name":"created_at","type":"DATETIME"},' \
                          '{"name":"sentiment","type":"STRING"},' \
                          '{"name":"sentiment_confidence","type":"FLOAT"}' \
                          ']}'
    bigqueryschemasentiment = parse_table_schema_from_json(bigqueryschema_sentiment_json)

    bigqueryschema_raw_json = '{"fields": [' \
                          '{"name":"text","type":"STRING"},' \
                          '{"name":"tweet_id","type":"STRING"},' \
                          '{"name":"user_id","type":"STRING"},' \
                          '{"name":"verified","type":"BOOLEAN"},' \
                          '{"name":"favorite_count","type":"INTEGER"},' \
                          '{"name":"retweet_count","type":"INTEGER"},' \
                          '{"name":"location","type":"STRING"},' \
                          '{"name":"created_at","type":"DATETIME"}' \
                          ']}'
    bigqueryschemaraw = parse_table_schema_from_json(bigqueryschema_raw_json)

    bigqueryschema_mean_json = '{"fields": [' \
                            '{"name":"created_at","type":"DATETIME"},' \
                            '{"name":"sentiment_confidence","type":"FLOAT"}' \
                             ']}'
    bigqueryschema_mean = parse_table_schema_from_json(bigqueryschema_mean_json)

    """Build and run the pipeline."""

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    # Run on Cloud Dataflow by default
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

    p = beam.Pipeline(options=pipeline_options)

    topic_options = PipelineOptions().view_as(TopicOptions)

    # Read from PubSub into a PCollection.
    if topic_options.input_topic:
        lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
            topic=topic_options.input_topic.get(),
            with_attributes=False,
        )
        logging.info("topic entered")

    # Window them, and batch them into batches of 50 (not too large)
    inbound_tweets = (lines
                     | 'assign window key' >> beam.WindowInto(window.FixedWindows(30))
                     | 'batch into n batches' >> BatchElements(min_batch_size=49, max_batch_size=50)
                     )

    raw_tweets = (inbound_tweets
                 | 'extract features of tweets' >> beam.FlatMap(lambda messages: estimater(messages))
                 )

    # Write to Bigquery
    raw_tweets | 'store raw twitter posts' >> beam.io.WriteToBigQuery(
        table="all_colorado_tweets",
        dataset="twitter_dataset",
        schema=bigqueryschemaraw,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project="twitter-streams-345620"
    )

    sentiment_analyzed_tweets = (inbound_tweets
                                    | 'predict sentiment' >> beam.FlatMap(lambda messages: estimate(messages))
                                )


    # Analyze sentiment ['Positive', 'Negative'] & sentiment confidence -1 <= x <= 1
    (sentiment_analyzed_tweets
        #| 'predict sentiment' >> beam.FlatMap(lambda messages: estimate(messages))
        | 'store sentiment analyzed twitter posts' >> beam.io.WriteToBigQuery(
            table="all_tweets_sentiment",
            dataset="twitter_dataset",
            schema=bigqueryschemasentiment,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            project="twitter-streams-345620"
        )
    )

    # Average out and log the mean value for every 30 second interval
    (sentiment_analyzed_tweets
     | 'pair with key' >> beam.Map(lambda x: (1, x))
     | 'group by key' >> beam.GroupByKey()
     | 'aggregate and format' >> beam.Map(aggregate_format)
     | 'store aggregated sentiment' >> beam.io.WriteToBigQuery(
                table="average_tweets_sentiment",
                dataset="twitter_dataset",
                schema=bigqueryschema_mean,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                project="twitter-streams-345620"))

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()