#Main file for Finnhub API & Kafka integration
import os
import json
import websocket
from utils import *
from dotenv import load_dotenv

load_dotenv()

#class that ingests upcoming messages from Finnhub websocket into Kafka
class FinnhubProducer:
    finnhub_client = load_client(os.getenv('FINNHUB_TOKEN'))
    
    def __init__(self):
        self.finnhub_client = load_client(os.getenv('FINNHUB_TOKEN'))
        self.producer = load_producer('172.25.0.12:9092')#f"{os.getenv('KAFKA_SERVER')}:{os.getenv('KAFKA_PORT')}")
        symbol_list = os.environ['FINNHUB_Symbols']
        self.symbols = symbol_list.split(',')

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(f'wss://ws.finnhub.io?token={os.getenv("FINNHUB_TOKEN")}',
                                on_message = self.on_message,
                                on_error = self.on_error,
                                on_close = self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_message(self, ws, message):

        try:
            # Check if the message is a ping and ignore it
            if "ping" in message:
                print("Received ping message. Ignoring.")
                return

            # Parse the message into JSON
            message = json.loads(message)

            # Check if the message contains trade data
            if message.get('type') == 'trade' and 'data' in message:
                trade_data = message['data'][0]  # Only using the first trade data item

                # Extract relevant data points
                stock_symbol = trade_data.get('s', 'N/A')
                price = trade_data.get('p', 0.0)
                timestamp = trade_data.get('t', 0)
                volume = trade_data.get('v', 0)

                # Log the trade data
                print(f"Received trade data for {stock_symbol}: Price={price}, Volume={volume}, Timestamp={timestamp}")

                # Serialize the trade data to JSON and send to Kafka
                message_to_send = json.dumps(trade_data).encode('utf-8')  # Encoding to bytes
                self.producer.send(os.getenv('KAFKA_TOPIC_NAME'), value=message_to_send)
            else:
                print("Received message of type:", message.get('type'))

        except Exception as e:
            print(f"Error processing message: {e}")


    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("### closed ###")

    def on_open(self, ws):
        for symbol in self.symbols:
            print(f'Symbol_++++++++++++++++++++++: {symbol}')
            print(f'Subscription succeeded')
            self.ws.send(f'{{"type":"subscribe","symbol":"{symbol}"}}')


if __name__ == "__main__":
    FinnhubProducer()