#Main file for Finnhub API & Kafka integration
import os
import json
import websocket
from Scripts.utils import *

#class that ingests upcoming messages from Finnhub websocket into Kafka
class FinnhubProducer:
    finnhub_client = load_client(os.environ['FINNHUB_TOKEN'])
    
    def __init__(self):

        self.finnhub_client = load_client(os.environ['FINNHUB_TOKEN'])
        self.producer = load_producer(f"{os.environ['KAFKA_SERVER']}:{os.environ['KAFKA_PORT']}")
        self.symbols = os.environ['FINNHUB_Symbols']
        

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={os.environ["FINNHUB_TOKEN"]}',
                              on_message = self.on_message,
                              on_error = self.on_error,
                              on_close = self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_message(self, ws, message):
        message = json.loads(message)
        self.producer.send(os.environ['KAFKA_TOPIC_NAME'], message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self, ws):
        for symbol in self.symbols:
            print(f'Subscription succeeded')
            self.ws.send(f'{"type":"subscribe","symbol":"{symbol}"}')

if __name__ == "__main__":
    FinnhubProducer()