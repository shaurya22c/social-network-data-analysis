import tweepy
import sys
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

# TWITTER API CONFIGURATIONS
consumer_key = "a0EuavCBCteJ4HDZulEt2aMGn"
consumer_secret = "10WeFjnIDNcLyATjzt0y5xODxmnudbuLxDD0iDMq9z2itMKYQl"
access_token = "1003508069542707200-lAstoqrb5wFFTV9AyzTHiv0PUpbQkn"
access_secret = "FFsCLgjS1TmNHIZ4cJDp9wJ20TocIxzCS8LyhPt7JQQUv"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # Default Zookeeper Port Address
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True

# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=['#vaccine'])