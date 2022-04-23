# Twitter-Streams
Course Final Project for CSCI 6502 Big Data Analytics: Systems, Algorithms, and Applications

This project uses the Twitter Streaming API and Google Cloud to build an end to end pipeline for processing a twitter stream.

(twitter stream) streams_producer.py -> Google Pub Sub Topic -> (apache Beam pipeline) streaming_tweet.py -> Google Big Query Dataset -> Google Data Studio

The necessary setup shell scripts are included for staging, deploying, and starting a Google Cloud Data Flow job that automatically provisions a subscription when given a topic to connect to.

This pipeline as implemented in streaming_tweet.py serves as a template for streaming twitter data into Google Cloud Big Query
