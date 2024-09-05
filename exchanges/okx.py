# exchanges/okx.py
import websocket
import json
import threading
import time


class OKXWebSocket:
    def __init__(self, symbols, on_message_callback, on_error_callback, on_close_callback, on_open_callback):
        self.symbols = symbols
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.ws = None
        self.thread = None
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback
        self.keep_running = True

    def request_snapshot(self, ws):
        """Subscribe to order book snapshots once upon connection."""
        # Subscribe to market data for each symbol
        for symbol in self.symbols:
            params = {
                "op": "subscribe",
                "args": [{"channel": "books", "instId": symbol}]
            }
            ws.send(json.dumps(params))
            print(f"Subscribed to snapshot of {symbol} on OKX.")

        print("Initial snapshots requested; maintaining order book in memory.")

    def on_open(self, ws):
        print("WebSocket connection opened to OKX.")
        self.on_open_callback(ws)

        # Request snapshots once upon connection
        snapshot_thread = threading.Thread(target=self.request_snapshot, args=(ws,))
        snapshot_thread.start()

    def on_message(self, ws, message):
        """Log all messages received from OKX."""
        print(f"Received message from OKX: {message}")
        self.on_message_callback(ws, message)

    def on_close(self, ws):
        print("WebSocket connection closed for OKX.")
        self.keep_running = False  # Stop the snapshot requests
        self.on_close_callback(ws)

    def on_error(self, ws, error):
        print(f"Error encountered in OKX WebSocket: {error}")
        self.on_error_callback(ws, error)

    def connect(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.on_open(ws),
            on_message=self.on_message,
            on_close=self.on_close,
            on_error=self.on_error
        )
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.start()

    def close(self):
        if self.ws:
            self.ws.close()
        if self.thread:
            self.thread.join()

    def start(self):
        self.connect()
