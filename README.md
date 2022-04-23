# Twitter-Streams
Course Final Project for CSCI 6502 Big Data Analytics: Systems, Algorithms, and Applications

This project uses the Twitter Streaming API and Google Cloud to build an end to end pipeline for processing a twitter stream.

(twitter stream) streams_producer.py -> Google Pub Sub Topic -> (apache Beam pipeline) streaming_tweet.py -> Google Big Query Dataset -> Google Data Studio

