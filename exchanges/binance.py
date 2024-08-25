import websocket
import json

class BinanceWebSocket:
    def __init__(self, symbols, on_message_callback, on_error_callback, on_close_callback, on_open_callback):
        self.symbols = symbols
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback
    
    def start(self):
        streams = "/".join([f"{symbol}@ticker" for symbol in self.symbols])
        ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=self.on_message_callback,
            on_error=self.on_error_callback,
            on_close=self.on_close_callback,
            on_open=self.on_open_callback
        )
        self.ws.run_forever()
