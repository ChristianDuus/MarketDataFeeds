import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket
from config import BINANCE_PAIRS

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

order_books = {pair: {'bids': [], 'asks': []} for pair in BINANCE_PAIRS}
last_update_times = {pair: 0 for pair in BINANCE_PAIRS}


def update_google_sheet(symbol, bids, asks):
    try:
        global last_update_times
        current_time = time.time()

        if current_time - last_update_times[symbol] < update_freq:
            print(
                f"Skipping update for {symbol}. "
                f"Time since last update: {current_time - last_update_times[symbol]:.2f} seconds"
            )
            return

        while len(bids) < depth:
            bids.append([None, None])
        while len(asks) < depth:
            asks.append([None, None])

        data = []
        for i in range(depth):
            bid = bids[i]
            ask = asks[i]
            data.append([
                f'Level {i+1}',
                bid[0] if bid[0] is not None else 'N/A',
                bid[1] if bid[1] is not None else 'N/A',
                ask[0] if ask[0] is not None else 'N/A',
                ask[1] if ask[1] is not None else 'N/A'
            ])

        start_row = (BINANCE_PAIRS.index(symbol) * (depth + 3)) + 2

        print(f"Data to update for {symbol}: {data}")

        end_row = start_row + depth - 1
        sheet.batch_clear([f'B{start_row}:F{end_row}'])

        # Use the new recommended order or named arguments
        sheet.update(range_name=f'A{start_row-1}', values=[[f'{symbol.upper()} Market Data']])
        sheet.update(range_name=f'B{start_row-1}',
                     values=[['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity']]
                     )
        sheet.update(range_name=f'B{start_row}:F{end_row}', values=data)

        last_update_times[symbol] = current_time
        print(
            f"Google Sheet updated for {symbol} at "
            f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update_times[symbol]))}"
        )

    except Exception as e:
        print(f"Exception in update_google_sheet: {e}")

# Enhanced on_message function to process the received message


def on_message(ws, message):
    try:
        data = json.loads(message)
        stream = data.get('stream', '')

        # Debug: Log the full message for analysis
        print("Received message:", message)

        # If the stream is for the order book (depth)
        if 'depth' in stream:
            symbol = stream.split('@')[0]

            if symbol not in order_books:
                print(f"Warning: Unrecognized symbol '{symbol}'")
                return

            bids = data['data']['bids']
            asks = data['data']['asks']

            # Update order book data in memory
            order_books[symbol]['bids'] = bids
            order_books[symbol]['asks'] = asks

            # Update Google Sheet with the current order book
            update_google_sheet(symbol, bids, asks)
    except Exception as e:
        print(f"Exception in on_message: {e}")


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws):
    print("WebSocket closed")


def on_open(ws):
    print("WebSocket connection opened")


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


if __name__ == "__main__":
    main()
