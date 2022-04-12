  python -m streaming_tweet \
    --runner DataflowRunner \
    --project twitter-streams-345620 \
    --staging_location gs://twitterstreams_dataflow_bucket/staging \
    --temp_location gs://twitterstreams_dataflow_bucket/temp \
    --template_location gs://twitterstream_sentiment_bucket/templates/sentinentTemplate \
    --region us-central1