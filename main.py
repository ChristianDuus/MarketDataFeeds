import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket
from exchanges.okx import OKXWebSocket
from exchanges.kraken import KrakenWebSocket
from exchanges.coinbase import connect as coinbase_connect
import config

# Set the depth of the order book (number of levels to retrieve)
depth = 5

# Set the frequency for updates in seconds
update_freq = 10

# Set up Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('ServiceAccountCredentials.json', scope)
client = gspread.authorize(creds)

# Open the Google Sheet
sheet = client.open_by_key('1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU').sheet1

order_books = {}
last_update_times = {}

# Initialize order_books and last_update_times for all active exchanges and pairs
for exchange, config_data in config.exchanges.items():
    if config_data['enabled']:
        for pair in config_data['pairs']:
            order_books[pair] = {'bids': [], 'asks': []}
            last_update_times[pair] = 0


def on_message(ws, message, exchange):
    try:
        data = json.loads(message)

        # Process Kraken data and handle symbol, bids, and asks
        if exchange == 'kraken':
            symbol = data.get('symbol')
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            if bids or asks:
                update_google_sheet(symbol, bids[:depth], asks[:depth], exchange)

        # Handle Binance symbol extraction
        if exchange == 'binance':
            symbol = data.get('s').lower()  # Binance symbols are lowercase
        # Handle OKX symbol extraction
        elif exchange == 'okx':
            if 'arg' in data and 'instId' in data['arg']:
                symbol = data['arg']['instId'].upper()  # OKX symbols are uppercase
            else:
                print(f"Error: Missing 'instId' in OKX message: {data}")
                return  # Skip processing if symbol is missing

        if symbol not in order_books:
            print(f"Warning: Unrecognized symbol '{symbol}'")
            return

        # Handle Binance order book updates
        if exchange == 'binance' and data.get('e') == 'depthUpdate':
            bids = data['b']
            asks = data['a']
        # Handle OKX order book updates
        elif exchange == 'okx':
            if 'data' in data:
                order_book_data = data['data'][0]  # Assuming we're working with the first item
                bids = order_book_data['bids']
                asks = order_book_data['asks']
            else:
                print(f"Unexpected OKX message format: {data}")
                return

        # Log bids and asks to verify they're correctly parsed
        # print(f"Parsed bids for {symbol}: {bids[:depth]}")
        # print(f"Parsed asks for {symbol}: {asks[:depth]}")

        # Update order book data in memory
        order_books[symbol]['bids'] = bids
        order_books[symbol]['asks'] = asks

        # Send the parsed data to the update function
        update_google_sheet(symbol, bids, asks, exchange)

    except Exception as e:
        print(f"Exception in on_message for {exchange}: {e}")


def update_google_sheet(symbol, bids, asks, exchange):
    # print(f"Preparing to update Google Sheets for {symbol} on {exchange}")
    try:
        global last_update_times
        current_time = time.time()

        # Log when the function is called
        print(f"Attempting to update Google Sheet for {symbol} on {exchange}")

        # Check if the update is too frequent
        if current_time - last_update_times[symbol] < update_freq:
            print(
                f"Skipping update for {symbol} on {exchange}. "
                f"Time since last update: {current_time - last_update_times[symbol]:.2f} seconds"
            )
            return

        # Log the bids and asks before processing
        # print(f"Received bids for {symbol}: {bids}")
        # print(f"Received asks for {symbol}: {asks}")

        # Limit the number of bids and asks based on the 'depth'
        limited_bids = bids[:depth]
        limited_asks = asks[:depth]

        # Ensure the number of bids and asks matches the depth
        while len(limited_bids) < depth:
            limited_bids.append([None, None])
        while len(limited_asks) < depth:
            limited_asks.append([None, None])

        # Prepare data in the format for Google Sheets
        data = []
        for i in range(depth):
            bid = limited_bids[i]
            ask = limited_asks[i]
            data.append([
                f'Level {i+1}',
                bid[0] if bid[0] is not None else 'N/A',
                bid[1] if bid[1] is not None else 'N/A',
                ask[0] if ask[0] is not None else 'N/A',
                ask[1] if ask[1] is not None else 'N/A'
            ])

        start_row = (list(order_books.keys()).index(symbol) * (depth + 3)) + 2

        # Log the data that is going to be sent to Google Sheets
        # print(f"Data to update for {symbol} on {exchange}: {data}")

        end_row = start_row + depth - 1
        # print(f"Updating Google Sheet for {symbol} from row {start_row} to {end_row}")

        # Push data to Google Sheets
        sheet.batch_clear([f'B{start_row}:F{end_row}'])
        sheet.update(range_name=f'A{start_row-1}', values=[[f'{symbol.upper()} {exchange.upper()} Market Data']])
        sheet.update(range_name=f'B{start_row-1}', values=[['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity']])
        sheet.update(range_name=f'B{start_row}:F{end_row}', values=data)

        last_update_times[symbol] = current_time
        # print(f"Google Sheet updated for {symbol} on {exchange} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update_times[symbol]))}")

    except Exception as e:
        print(f"Exception in update_google_sheet: {e}")

def on_error(ws, error, exchange):
    print(f"Error on {exchange}: {error}")

def on_close(ws, *args):
    print(f"WebSocket closed")

def on_open(ws, exchange):
    print(f"WebSocket connection opened to {exchange}")

def main():
    websockets = []

    try:
        # Connect to Binance if enabled in config
        if config.exchanges['binance']['enabled']:
            binance_pairs = config.exchanges['binance']['pairs']
            binance_ws = BinanceWebSocket(
                symbols=binance_pairs,
                on_message_callback=lambda ws, msg: on_message(ws, msg, 'binance'),
                on_error_callback=lambda ws, err: on_error(ws, err, 'binance'),
                on_close_callback=lambda ws: on_close(ws),
                on_open_callback=lambda ws: on_open(ws, 'binance')
            )
            binance_ws.start()
            websockets.append(binance_ws)
            print("Connected to Binance.")

        # Connect to OKX if enabled in config
        if config.exchanges['okx']['enabled']:
            okx_pairs = config.exchanges['okx']['pairs']
            okx_ws = OKXWebSocket(
                symbols=okx_pairs,
                on_message_callback=lambda ws, msg: on_message(ws, msg, 'okx'),
                on_error_callback=lambda ws, err: on_error(ws, err, 'okx'),
                on_close_callback=lambda ws: on_close(ws),
                on_open_callback=lambda ws: on_open(ws, 'okx')
            )
            okx_ws.start()
            websockets.append(okx_ws)
            print("Connected to OKX.")

        if config.exchanges['kraken']['enabled']:
            kraken_pairs = config.exchanges['kraken']['pairs']  # Ensure the symbol list is provided
            if not kraken_pairs:
                raise ValueError("No pairs provided for Kraken in the config.")

            print(f"Kraken pairs: {kraken_pairs}")  # Add a print to verify pairs
            kraken_ws = KrakenWebSocket(
                symbols=kraken_pairs,  # Make sure symbols are passed correctly
                on_message_callback=lambda ws, msg: on_message(ws, msg, 'kraken'),
                on_error_callback=lambda ws, err: on_error(ws, err, 'kraken'),
                on_close_callback=lambda ws: on_close(ws),
                on_open_callback=lambda ws: on_open(ws, 'kraken')
            )
            kraken_ws.start()
            websockets.append(kraken_ws)
            print("Connected to Kraken.")

        # Connect to Coinbase if enabled in config
        if config.exchanges['coinbase']['enabled']:
            print("Starting Coinbase WebSocket...")
            coinbase_connect()  # Start the Coinbase WebSocket connection
            print("Connected to Coinbase.")

        while True:
            pass

    except KeyboardInterrupt:
        print("Terminating WebSocket connections...")
        for ws in websockets:
            ws.close()

if __name__ == "__main__":
    main()
