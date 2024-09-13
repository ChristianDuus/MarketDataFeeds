import websocket
import json
import hmac
import hashlib
import time
import base64
import requests
import config

# For loading credentials
import os
import json

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"

# Load API credentials from the 'client_secret.json' file
with open('coinbase_auth.json') as f:
    credentials = json.load(f)

api_key = credentials['api_key']
api_secret = credentials['api_secret']
passphrase = credentials['passphrase']

# Generate authentication signature
def generate_signature():
    timestamp = str(time.time())
    message = timestamp + 'GET' + '/users/self/verify'
    hmac_key = base64.b64decode(api_secret)
    signature = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256).digest()
    signature_b64 = base64.b64encode(signature).decode('utf-8')
    return timestamp, signature_b64

# Open Coinbase WebSocket connection and authenticate
def on_open(ws):
    timestamp, signature_b64 = generate_signature()

    # Dynamically pull product IDs from config.py
    coinbase_pairs = config.exchanges['coinbase']['pairs']

    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {
                "name": "level2",  # You can subscribe to other channels as needed
                "product_ids": coinbase_pairs
            }
        ],
        "signature": signature_b64,
        "key": api_key,
        "passphrase": passphrase,
        "timestamp": timestamp
    }

    ws.send(json.dumps(subscribe_message))
    print(f"Coinbase WebSocket connection opened and subscription sent for pairs: {coinbase_pairs}")

# Handle incoming messages from Coinbase
def on_message(ws, message):
    data = json.loads(message)
    process_message(data)

# Handle WebSocket errors
def on_error(ws, error):
    print(f"Coinbase WebSocket error: {error}")

# Handle WebSocket closure
def on_close(ws):
    print("Coinbase WebSocket closed.")

# Process incoming messages from Coinbase
def process_message(data):
    print(f"Received data from Coinbase: {data}")

# Function to start Coinbase WebSocket connection
def connect():
    ws = websocket.WebSocketApp(
        COINBASE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()
