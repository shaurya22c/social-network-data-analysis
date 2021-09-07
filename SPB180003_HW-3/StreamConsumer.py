import json
from textblob import TextBlob
from kafka import KafkaConsumer
import sys
import re
from elasticsearch import Elasticsearch
es = Elasticsearch()


# Function to remove special characters from the link
def preprocess_tweets_data(tweet):
    processed_tweet = re.sub("[@,\/#!$%\^&\*;:{}=_`~()+']", " ", tweet)
    return " ".join(processed_tweet.split());

def main():
    # Here we initialize object of KafkaConsumer which will consume tweets produced by the producer
    kafkaconsumer = KafkaConsumer("twitter")

    for msg in kafkaconsumer:

        data = json.loads(msg.value)
        sentiment = 'Neutral'

        # checking if the language of data is English
        if data['lang']=='en':
            #preprocessing the tweets
            preprocessed_tweets = preprocess_tweets_data(data["text"])
            tweet = TextBlob(preprocessed_tweets)
            print(tweet)

            #calculating the polarity of tweet
            polarity =tweet.sentiment.polarity
            print("Polarity =  " + str(polarity))

            if polarity>0:
                sentiment='Positive'
            elif polarity<0:
                sentiment='Negative'

            print("Sentiment =  " + str(sentiment))

            # adding each tweet info to elastic search
            es.index(index=sys.argv[1],
            doc_type="test-type",
            body={"author": data["user"]["screen_name"],
            "date": data["created_at"],
            "message": data["text"],
            "sentiment": sentiment})
            print('\n')

if __name__ == "__main__":
    main()