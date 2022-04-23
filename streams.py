import os
import json
import datetime
from google.cloud import pubsub_v1
import tweepy 

# # To set your enviornment variables in your terminal run the following line:
# # export 'BEARER_TOKEN'='<your_bearer_token>'
# # bearer_token = os.environ.get("BEARER_TOKEN")
bearer_token = "AAAAAAAAAAAAAAAAAAAAAMaMawEAAAAALMK3JQ0J05eVatForvOL83Vqet8%3D3K0R9TqHsIpMuFOU1JRC9VeStnPOaqVkj9zXiiBpzUsGK7Yxau"

credentials_path = '/Users/lincolnrychecky/Desktop/Twitter-Streams/twitter-streams-345620-910278d14e83.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# # connect to Google Cloud Pub/Sub exchange
client = pubsub_v1.PublisherClient()
topic_path = client.topic_path("twitter-streams-345620", "twitterstream")

# Store OAuth authentication credentials in relevant variables
access_token = "1250474930178670593-6X1ienbYpdlrf8l9TgAGL21aXHtK5q"
access_token_secret = "2UGfIQgCyJmp4gf7VZncs7mrIrGqu2WHpiQNANpbTRfP6"
consumer_key = "M2n1GHsbBLnQcUWsK49C1bs1k"
consumer_secret = "4psN3zgR0OxZH4QwXVPklE0szcSjGWazY6dczN3sTy8VYyTlzP"

# Pass OAuth details to tweepy's OAuth handler
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("twitter-streams-345620", "twitterstream")

# Function to write data to
def write_to_pubsub(status):
    data = status._json
    try:
        if data["lang"] == "en":

            # publish to the topic encoded in utf-8
            publisher.publish(topic_path, data=json.dumps({
                "text": status.retweeted_status.extended_tweet['full_text'] if hasattr(status, 'retweeted_status') and hasattr(status.retweeted_status, 'extended_tweet') else status.extended_tweet['full_text'] if hasattr(status, 'extended_tweet') else status.text,
                "tweet_id": data["id_str"],
                "user_id": data["user"]["id_str"],
                "verified": data["user"]["verified"],
                "location": data["user"]["location"],
                "created_at": datetime.datetime.fromtimestamp(int(status.timestamp_ms)/1000).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"))
            
    except Exception as e:
        print(e)
        raise


# Streaming Tweets
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.Stream):
    def on_status(self, status):
        write_to_pubsub(status)
        print("Message sent")
        

# Initialize Stream listener
l = MyStreamListener(consumer_key, consumer_secret, access_token, access_token_secret)

# # Create you Stream object with authentication
# stream = tweepy.Stream(consumer_key, consumer_secret, access_token, access_token_secret)

# Filter Twitter Streams to capture data by the keywords:
l.filter(track = ['colorado'])