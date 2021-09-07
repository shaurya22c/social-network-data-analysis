from kafka import KafkaConsumer
import json
# from elasticsearch import Elasticsearch
from textblob import TextBlob
# es = Elasticsearch()

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer, extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into elasticsearch
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:

        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"])
        print(tweet)
        # add text and sentiment info to elasticsearch
        # es.index(index="tweet",
        #          doc_type="test-type",
        #          body={"author": dict_data["user"]["screen_name"],
        #                "date": dict_data["created_at"],
        #                "message": dict_data["text"]})
        print('\n')

if __name__ == "__main__":
    main()