import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket

BINANCE_PAIRS = ["btcusdt", "ethusdt"]  # Temporarily set to a single pair

# Script to ensure we can subscribe to a static set of two pairs from Binance WebSocket

def main():
    try:
        binance_ws = BinanceWebSocket(
            symbols=[symbol for symbol in BINANCE_PAIRS],
            on_message_callback=on_message,
            on_error_callback=on_error,
            on_close_callback=on_close,
            on_open_callback=on_open
        )
        binance_ws.start()
    except Exception as e:
        print(f"Exception in main: {e}")

def on_message(ws, message):
    try:
        print("Received message:", message)
    except Exception as e:
        print(f"Exception in on_message: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")

if __name__ == "__main__":
    main()
