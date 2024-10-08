import websocket
import json
import threading

class BinanceWebSocket:
    def __init__(self, symbols, on_message_callback, on_error_callback, on_close_callback, on_open_callback):
        self.symbols = symbols
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.ws = None
        self.thread = None
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback

    def on_open(self, ws):
        print("WebSocket connection opened to Binance.")
        # Subscribe to market data for each symbol
        for symbol in self.symbols:
            params = {
                "method": "SUBSCRIBE",
                "params": [f"{symbol.lower()}@depth"],  # Adjust to the proper Binance WebSocket topic
                "id": 1
            }
            ws.send(json.dumps(params))
            print(f"Subscribed to {symbol} on Binance.")
        self.on_open_callback(ws)

    def on_message(self, ws, message):
        self.on_message_callback(ws, message)

    def on_close(self, ws):
        print("WebSocket connection closed for Binance.")
        self.on_close_callback(ws)

    def on_error(self, ws, error):
        print(f"Error encountered in Binance WebSocket: {error}")
        self.on_error_callback(ws, error)

    def connect(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
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
