  python -m streaming_tweet \
    --runner DataflowRunner \
    --project twitter-streams-345620 \
    --staging_location gs://twitterstream_sentiment_bucket/staging \
    --temp_location gs://twitterstream_sentiment_bucket/temp \
    --template_location gs://twitterstream_sentiment_bucket/templates/sentinentTemplate \
    --input_topic projects/twitter-streams-345620/topics/twitterstream \
    --region us-central1