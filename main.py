import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket
from config import BINANCE_PAIRS

# Set the depth of the order book (number of levels to retrieve)
depth = 5  # You can change this to any number of levels you want

# Set the frequency for updates in seconds
update_freq = 5  # Update every 5 seconds (you can adjust this as needed)


# Set up Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('ServiceAccountCredentials.json', scope)
client = gspread.authorize(creds)

# Open the Google Sheet
sheet = client.open_by_key('1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU').sheet1  # Update with your actual sheet name

# Variable to track the last update time
last_update_time = 0


# Function to update the Google Sheet with bid and ask prices
def update_google_sheet(symbol, bids, asks):
    global last_update_time

    # Check if enough time has passed since the last update
    current_time = time.time()
    if current_time - last_update_time < update_freq:
        print(f"Skipping update. Time since last update: {current_time - last_update_time:.2f} seconds")
        return

    # Ensure there are `depth` levels by filling missing levels with placeholders
    while len(bids) < depth:
        bids.append([None, None])
    while len(asks) < depth:
        asks.append([None, None])

    # Prepare the data for update
    data = []
    for i in range(depth):
        bid = bids[i]
        ask = asks[i]
        data.append([
            f'Level {i+1}',
            bid[0] if bid[0] is not None else 'N/A',  # Ensure there's a placeholder if data is missing
            bid[1] if bid[1] is not None else 'N/A',
            ask[0] if ask[0] is not None else 'N/A',
            ask[1] if ask[1] is not None else 'N/A'
        ])

    # Debugging: Print the final data structure before updating
    print(f"Data to update: {data}")

    # Ensure the data size matches the expected size (depth rows, 5 columns)
    if len(data) != depth or any(len(row) != 5 for row in data):
        raise ValueError(f"Data size does not match the expected {depth}x5 range. Data: {data}")

    # Clear previous data
    end_row = depth + 1  # +1 to account for the header row
    sheet.batch_clear([f'B2:F{end_row}'])  # Update to F to reflect 5 columns

    # Set headers (ensure to pass values first, range second)
    sheet.update(values=[[f'{symbol} Market Data']], range_name='A1')
    sheet.update(values=[['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity']], range_name='B1')

    # Update the Google Sheet with the new data (values first, range second)
    sheet.update(values=data, range_name=f'B2:F{end_row}')  # Update to F to reflect 5 columns

    # Update the last update time
    last_update_time = current_time
    print(f"Google Sheet updated at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_update_time))}")


# WebSocket message handler to update Google Sheets
def on_message(ws, message):
    data = json.loads(message)
    stream = data['stream']

    # If the stream is for the order book (depth)
    if 'depth' in stream:
        symbol = stream.split('@')[0].upper()
        bids = data['data']['bids']
        asks = data['data']['asks']
        update_google_sheet(symbol, bids, asks)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")


def main():
    # Start the Binance WebSocket
    binance_ws = BinanceWebSocket(
        symbols=BINANCE_PAIRS,
        on_message_callback=on_message,
        on_error_callback=on_error,
        on_close_callback=on_close,
        on_open_callback=on_open
    )
    binance_ws.start()


if __name__ == "__main__":
    main()