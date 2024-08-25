import websocket
import json
from config import BINANCE_PAIRS


class BinanceWebSocket:
    def __init__(self, symbols, on_message_callback, on_error_callback, on_close_callback, on_open_callback):
        self.symbols = symbols
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback

    def start(self):
        # Update the streams to include the order book (depth) stream
        streams = "/".join([f"{symbol}@ticker/{symbol}@depth5" for symbol in self.symbols])
        ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=self.on_message_callback,
            on_error=self.on_error_callback,
            on_close=self.on_close_callback,
            on_open=self.on_open_callback
        )
        self.ws.run_forever()


# Example callback functions
def on_message(ws, message):
    # Parse the incoming message
    data = json.loads(message)
    stream = data['stream']

    # Handle ticker data
    if 'ticker' in stream:
        print(f"Ticker data received: {data['data']}")

    # Handle order book (depth) data
    elif 'depth' in stream:
        print(f"Order book data received: {data['data']}")
        # Example processing:
        bids = data['data']['bids']
        asks = data['data']['asks']
        print("Bids:", bids)
        print("Asks:", asks)


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws):
    print("WebSocket closed")


def on_open(ws):
    print("WebSocket connection opened")


# Entry point for starting the WebSocket
def start_binance_websocket():
    binance_ws = BinanceWebSocket(
        symbols=BINANCE_SYMBOLS,
        on_message_callback=on_message,
        on_error_callback=on_error,
        on_close_callback=on_close,
        on_open_callback=on_open
    )
    binance_ws.start()
