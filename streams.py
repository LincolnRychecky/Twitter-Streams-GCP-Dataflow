import requests
import os
import json
from google.cloud import pubsub_v1
import tweepy 

# # To set your enviornment variables in your terminal run the following line:
# # export 'BEARER_TOKEN'='<your_bearer_token>'
# # bearer_token = os.environ.get("BEARER_TOKEN")
bearer_token = "AAAAAAAAAAAAAAAAAAAAAMaMawEAAAAALMK3JQ0J05eVatForvOL83Vqet8%3D3K0R9TqHsIpMuFOU1JRC9VeStnPOaqVkj9zXiiBpzUsGK7Yxau"

credentials_path = '/Users/raeganrychecky/Desktop/Twitter-Streams/twitter-streams-345620-910278d14e83.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# # connect to Google Cloud Pub/Sub exchange
# client = pubsub_v1.PublisherClient()
# topic_path = client.topic_path("twitter-streams-345620", "twitterstream")

# Store OAuth authentication credentials in relevant variables
access_token = "1250474930178670593-6X1ienbYpdlrf8l9TgAGL21aXHtK5q"
access_token_secret = "2UGfIQgCyJmp4gf7VZncs7mrIrGqu2WHpiQNANpbTRfP6"
consumer_key = "M2n1GHsbBLnQcUWsK49C1bs1k"
consumer_secret = "4psN3zgR0OxZH4QwXVPklE0szcSjGWazY6dczN3sTy8VYyTlzP"

# Pass OAuth details to tweepy's OAuth handler
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Streaming Tweets
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.Stream):
    def on_status(self, status):
        print(status.text)

# Initialize Stream listener
l = MyStreamListener(consumer_key, consumer_secret, access_token, access_token_secret)

# # Create you Stream object with authentication
# stream = tweepy.Stream(consumer_key, consumer_secret, access_token, access_token_secret)

# Filter Twitter Streams to capture data by the keywords:
l.filter(track = ['clinton', 'trump', 'sanders', 'cruz'])


# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("twitter-streams-345620", "twitterstream")

# Function to write data to
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
          
            # publish to the topic, don't forget to encode everything at utf8!
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            
    except Exception as e:
        print(e)
        raise



# def bearer_oauth(r):
#     """
#     Method required by bearer token authentication.
#     """
#     r.headers["Authorization"] = f"Bearer {bearer_token}"
#     r.headers["User-Agent"] = "v2FilteredStreamPython"
#     return r


# def get_rules():
#     response = requests.get(
#         "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth, 
#     )
#     if response.status_code != 200:
#         raise Exception(
#             "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
#         )
#     print(json.dumps(response.json()))
#     return response.json()


# def delete_all_rules(rules):
#     if rules is None or "data" not in rules:
#         return None

#     ids = list(map(lambda rule: rule["id"], rules["data"]))
#     payload = {"delete": {"ids": ids}}
#     response = requests.post(
#         "https://api.twitter.com/2/tweets/search/stream/rules",
#         auth=bearer_oauth,
#         json=payload
#     )
#     if response.status_code != 200:
#         raise Exception(
#             "Cannot delete rules (HTTP {}): {}".format(
#                 response.status_code, response.text
#             )
#         )
#     print(json.dumps(response.json()))


# def set_rules(delete):
#     # You can adjust the rules if needed
#     # sample_rules = [
#     #     {"value": "dog has:images", "tag": "dog pictures"},
#     #     {"value": "cat has:images -grumpy", "tag": "cat pictures"},
#     # ]
#     sample_rules = [
#         {"value": "colorado lang:en"}
#     ]
#     payload = {"add": sample_rules}
#     response = requests.post(
#         "https://api.twitter.com/2/tweets/search/stream/rules",
#         auth=bearer_oauth,
#         json=payload,
#     )
#     if response.status_code != 201:
#         raise Exception(
#             "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
#         )
#     print(json.dumps(response.json()))


# def get_stream(set):
#     response = requests.get(
#         "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
#     )
#     print(response.status_code)
#     if response.status_code != 200:
#         raise Exception(
#             "Cannot get stream (HTTP {}): {}".format(
#                 response.status_code, response.text
#             )
#         )
#     for response_line in response.iter_lines():
#         if response_line:
#             json_response = json.loads(response_line)
#             print(json.dumps(json_response['data'], indent=4, sort_keys=True))
#             # print(json.dumps(json_response, indent=4, sort_keys=True))
#             client.publish(topic_path, data=json.dumps(json_response['data'], indent=4, sort_keys=True).encode('utf-8'))

# def main():
#     rules = get_rules()
#     delete = delete_all_rules(rules)
#     set = set_rules(delete)
#     get_stream(set)


# if __name__ == "__main__":
    # main()