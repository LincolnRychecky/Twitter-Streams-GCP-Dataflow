"""A streaming python pipeline to read in pubsub tweets and perform classification"""

from __future__ import absolute_import

import argparse
import datetime
import json
import logging

import numpy as np

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.transforms.util import BatchElements

# from googleapiclient import discovery

def estimate(messages):
    # from flair.models import TextClassifier
    # from flair.data import Sentence
    # classifier = TextClassifier.load('sentiment')

    # # Be able to cope with a single string as well
    if not isinstance(messages, list):
        messages = [messages]

    # Messages from pubsub are JSON strings
    # instances = list(map(lambda message: json.loads(message), messages))

    # # Estimate the sentiment of the 'text' of each tweet
    # transformedSentences = Sentence([instance["text"] for instance in instances])
    # ([classifier.predict(sentence) for sentence in transformedSentences])

    # # Join them together
    # for i, instance in enumerate(instances):
    #     instance['sentiment'] = transformedSentences[i].to_dict()['labels'][0]['value']
    #     instance['sentiment_confidence'] = transformedSentences[0].to_dict()['labels'][0]['confidence']

    for instance in messages:
        instance['sentiment'] = "Negative"
        instance['sentiment_confidence'] = 0.0

    logging.info("first message in batch")
    # logging.info(instances[0])
    logging.info(messages[0])

    return messages


def run(argv=None):
    # Main pipeline run def

    # Make explicit BQ schema for output tables
    bigqueryschema_json = '{"fields": [' \
                          '{"name":"id","type":"STRING"},' \
                          '{"name":"text","type":"STRING"},' \
                          '{"name":"sentiment","type":"STRING"},' \
                          '{"name":"sentiment_confidence","type":"FLOAT"}' \
                          ']}'
    bigqueryschema = parse_table_schema_from_json(bigqueryschema_json)

    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'),
        default="projects/twitter-streams-345620/subscriptions/gcf-Send-to-Table-us-central1-twitterstream"

    )
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>."'),
        default="projects/twitter-streams-345620/topics/twitterstream"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    # Run on Cloud Dataflow by default
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'twitter-streams-345620'
    google_cloud_options.staging_location = 'gs://twitterstreams_dataflow_bucket/staging/staging'
    google_cloud_options.temp_location = 'gs://twitterstreams_dataflow_bucket/temp'
    google_cloud_options.region = 'us-central1'

    p = beam.Pipeline(options=pipeline_options)

    # Read from PubSub into a PCollection.
    if known_args.input_subscription:
        lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription,
            with_attributes=False,
            id_label="id"
        )
    else:
        lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
            topic=known_args.input_topic,
            with_attributes=False,
            id_label="id")

    # Window them, and batch them into batches of 50 (not too large)
    output_tweets = (lines
                     | 'assign window key' >> beam.WindowInto(window.FixedWindows(10))
                     | 'batch into n batches' >> BatchElements(min_batch_size=49, max_batch_size=50)
                     | 'predict sentiment' >> beam.FlatMap(lambda messages: estimate(messages))
                     )

    # Write to Bigquery
    output_tweets | 'store twitter posts' >> beam.io.WriteToBigQuery(
        table="twitter-streams-345620.twitter_dataset.tweets_sentiment",
        dataset="twitter_dataset",
        schema=bigqueryschema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project="twitter-streams-345620"
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()