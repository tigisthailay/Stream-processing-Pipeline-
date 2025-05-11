import json
import finnhub
import io
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

#setting up Finnhub client connection to test if tickers specified in config exist
def load_client(token):
    return finnhub.Client(api_key=token)

#setting up a Kafka connection
def load_producer(kafka_server):
    return KafkaProducer(bootstrap_servers=kafka_server)
