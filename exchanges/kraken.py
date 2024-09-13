import websocket
import json
import threading


def process_message(data):
    if isinstance(data, list) and len(data) > 1:
        message = data[1]
        symbol = data[-1]  # Extract the symbol (e.g., 'XBT/USD')

        # Handle initial snapshot ('as' for asks, 'bs' for bids) or updates ('a' for asks, 'b' for bids)
        asks = message.get('as', message.get('a', []))
        bids = message.get('bs', message.get('b', []))

        return symbol, bids, asks
    else:
        print(f"Non-list message received from Kraken: {data}")
        return None


class KrakenWebSocket:
    def __init__(self, symbols, on_message_callback, on_error_callback, on_close_callback, on_open_callback):
        self.url = "wss://ws.kraken.com"
        self.symbols = symbols
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback
        self.ws = None
        self.order_book = {symbol: {'bids': [], 'asks': []} for symbol in symbols}  # Memory to store the full book

    def update_order_book(self, symbol, bids, asks):
        # Update bids
        for bid in bids:
            price, volume = bid[:2]
            if float(volume) == 0:
                self.order_book[symbol]['bids'] = [b for b in self.order_book[symbol]['bids'] if b[0] != price]
            else:
                self.order_book[symbol]['bids'] = [b for b in self.order_book[symbol]['bids'] if b[0] != price]
                self.order_book[symbol]['bids'].append(bid)
        self.order_book[symbol]['bids'].sort(key=lambda x: float(x[0]), reverse=True)

        # Update asks
        for ask in asks:
            price, volume = ask[:2]
            if float(volume) == 0:
                self.order_book[symbol]['asks'] = [a for a in self.order_book[symbol]['asks'] if a[0] != price]
            else:
                self.order_book[symbol]['asks'] = [a for a in self.order_book[symbol]['asks'] if a[0] != price]
                self.order_book[symbol]['asks'].append(ask)
        self.order_book[symbol]['asks'].sort(key=lambda x: float(x[0]))

    def on_open(self, ws):
        # Subscribe to the Kraken feed for the given symbols
        subscribe_message = {
            "event": "subscribe",
            "pair": self.symbols,
            "subscription": {
                "name": "book",
                "depth": 10  # Adjust depth here based on your requirements
            }
        }
        ws.send(json.dumps(subscribe_message))
        self.on_open_callback(ws)

    def on_message(self, ws, message):
        data = json.loads(message)
        if isinstance(data, list) and len(data) > 1:
            symbol = data[-1]  # Extract the symbol
            message_data = data[1]
            bids = message_data.get('bs', message_data.get('b', []))
            asks = message_data.get('as', message_data.get('a', []))

            # Update the full book in memory
            self.update_order_book(symbol, bids, asks)

            # Pass the full book to the callback
            full_bids = self.order_book[symbol]['bids'][:10]  # Use full order book
            full_asks = self.order_book[symbol]['asks'][:10]
            self.on_message_callback(ws, json.dumps({"symbol": symbol, "bids": full_bids, "asks": full_asks}))

    def on_error(self, ws, error):
        self.on_error_callback(ws, error)

    def on_close(self, ws):
        self.on_close_callback(ws)

    def start(self):
        self.ws = websocket.WebSocketApp(self.url,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        wst = threading.Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()

    def close(self):
        if self.ws:
            self.ws.close()
