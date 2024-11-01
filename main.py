import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket
from exchanges.okx import OKXWebSocket
from exchanges.kraken import KrakenWebSocket
from exchanges.coinbase import connect as coinbase_connect
from config import normalize_pair  # Updated import
from data_utils import format_order_data
import config
from collections import defaultdict

# Set the depth of the order book (number of levels to retrieve)
depth = 5

# Set the frequency for updates in seconds
update_freq = 10

aggregated_books = defaultdict(lambda: {'bids': [], 'asks': []})

# Set up Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('ServiceAccountCredentials.json', scope)
client = gspread.authorize(creds)

# Open the Google Sheet
sheet = client.open_by_key('1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU').sheet1

order_books = {}
last_update_times = {}
google_sheet_cache = {}


def normalize_pair(pair: str, exchange: str) -> str:
    """Normalize pair names based on exchange-specific formats."""
    if not pair:
        print(f"Warning: Received an invalid pair value for {exchange}. Pair is: {pair}")
        return None

    pair = pair.strip()  # Strip spaces just in case

    print(f"Normalizing pair '{pair}' for exchange '{exchange}'")

    if exchange == 'binance' or exchange == 'okx':
        # Binance and OKX format is 'btcusdt' (lowercase, no dashes)
        return pair.replace('-', '').lower()
    elif exchange == 'kraken':
        # Kraken uses uppercase format with 'XBT' for 'BTC'
        pair = pair.replace('/', '').upper()
        if 'BTC' in pair:
            pair = pair.replace('BTC', 'XBT')
        return pair.lower()  # Ensure it's lowercase to match Binance/OKX
    else:
        print(f"Warning: Unrecognized exchange '{exchange}' or unsupported format for pair '{pair}'")
        return None


def aggregate_books(pair, book, exchange):
    """Aggregate order books for a given pair from multiple exchanges."""
    normalized_pair = normalize_pair(pair, exchange)
    if normalized_pair is None:
        print(f"Error: Could not normalize pair '{pair}' for exchange '{exchange}'. Skipping subscription.")
        return

    # Ensure pair exists in aggregated_books with bids and asks initialized
    if normalized_pair not in aggregated_books:
        aggregated_books[normalized_pair] = {'bids': {}, 'asks': {}}

    exchange_code = exchange[:2].capitalize()  # Use the first two letters of the exchange name

    # Aggregate bids
    for entry in book.get('bids', []):
        try:
            price, quantity = float(entry[0]), float(entry[1])
        except (ValueError, IndexError):
            print(f"Invalid bid entry {entry} for {normalized_pair} from {exchange}. Skipping.")
            continue

        price_str = f"{price:.8f}"  # Format price as string to be a dictionary key
        if quantity > 0:  # Only include valid levels with quantities
            if price_str in aggregated_books[normalized_pair]['bids']:
                aggregated_books[normalized_pair]['bids'][price_str]['quantity'] += quantity
                aggregated_books[normalized_pair]['bids'][price_str]['contributors'].add(exchange_code)
            else:
                aggregated_books[normalized_pair]['bids'][price_str] = {
                    'quantity': quantity,
                    'contributors': {exchange_code}
                }

    # Aggregate asks
    for entry in book.get('asks', []):
        try:
            price, quantity = float(entry[0]), float(entry[1])
        except (ValueError, IndexError):
            print(f"Invalid ask entry {entry} for {normalized_pair} from {exchange}. Skipping.")
            continue

        price_str = f"{price:.8f}"  # Format price as string to be a dictionary key
        if quantity > 0:  # Only include valid levels with quantities
            if price_str in aggregated_books[normalized_pair]['asks']:
                aggregated_books[normalized_pair]['asks'][price_str]['quantity'] += quantity
                aggregated_books[normalized_pair]['asks'][price_str]['contributors'].add(exchange_code)
            else:
                aggregated_books[normalized_pair]['asks'][price_str] = {
                    'quantity': quantity,
                    'contributors': {exchange_code}
                }

    # Sort and limit to top 'depth' levels
    aggregated_books[normalized_pair]['bids'] = dict(sorted(
        aggregated_books[normalized_pair]['bids'].items(),
        key=lambda x: -float(x[0])
    )[:depth])

    aggregated_books[normalized_pair]['asks'] = dict(sorted(
        aggregated_books[normalized_pair]['asks'].items(),
        key=lambda x: float(x[0])
    )[:depth])

    print(
        f"Aggregated book for {normalized_pair}: Bids = {len(aggregated_books[normalized_pair]['bids'])}, "
        f"Asks = {len(aggregated_books[normalized_pair]['asks'])}"
    )

def initialize_order_books():
    for exchange, config_data in config.exchanges.items():
        if config_data['enabled']:
            for pair in config_data['pairs']:
                normalized_pair = normalize_pair(pair, exchange)
                if normalized_pair:
                    unique_key = f"{exchange}_{normalized_pair}"  # Construct unique_key
                    order_books[unique_key] = {'bids': [], 'asks': []}  # Use unique_key for order_books
                    last_update_times[unique_key] = 0  # Use unique_key for last_update_times
                else:
                    print(f"Warning: Could not normalize pair '{pair}' for exchange '{exchange}'. Skipping.")


def process_order_book(symbol, bids, asks):
    """Process order book data."""
    print(f"Processing order book for {symbol}")
    for bid in bids:
        print(f"Bid: {bid}")
    for ask in asks:
        print(f"Ask: {ask}")


def on_message_kraken(ws, message):
    try:
        parsed_data = json.loads(message)
        print(f"Kraken message received: {parsed_data}")

        if 'event' in parsed_data:
            if parsed_data['event'] == 'subscriptionStatus':
                print(f"Kraken subscription event: {parsed_data}")
            elif parsed_data['event'] == 'heartbeat':
                print("Kraken heartbeat received, no action needed.")
            else:
                print(f"Unhandled Kraken event type: {parsed_data['event']}")
                print(f"Full Kraken message: {json.dumps(parsed_data, indent=2)}")

        elif isinstance(parsed_data, dict) and 'bids' in parsed_data and 'asks' in parsed_data:
            symbol = parsed_data.get('symbol')
            normalized_symbol = normalize_pair(symbol, 'kraken')  # Updated reference

            bids = [bid[:2] for bid in parsed_data.get('bids', [])]
            asks = [ask[:2] for ask in parsed_data.get('asks', [])]

            formatted_data = format_order_data('kraken', {
                "bids": bids,
                "asks": asks,
                "timestamp": parsed_data.get("timestamp")
            })

            if formatted_data["bids"] or formatted_data["asks"]:
                print(f"Updating Kraken order book for {normalized_symbol}: Bids = {len(bids)}, Asks = {len(asks)}")
                unique_key = f"kraken_{normalized_symbol}"
                update_google_sheet(unique_key, formatted_data["bids"], formatted_data["asks"], 'kraken')
            else:
                print(f"No bids or asks found for {normalized_symbol}")
        else:
            print(f"Unhandled Kraken message structure: {parsed_data}")
    except Exception as e:
        print(f"Error processing Kraken message: {str(e)}")


def on_message(ws, message, exchange):
    """Process incoming WebSocket messages and handle order book updates."""
    try:
        parsed_data = json.loads(message)
        print(f"Received message from {exchange}: {parsed_data}")

        # Filter messages to process only those with valid order book data
        if exchange == 'binance':
            if 'b' in parsed_data and 'a' in parsed_data:
                symbol = parsed_data.get('s', '').lower()
                normalized_symbol = normalize_pair(symbol, 'binance')

                # Process and format bids and asks
                bids = [[float(bid[0]), float(bid[1])] for bid in parsed_data.get('b', [])]
                asks = [[float(ask[0]), float(ask[1])] for ask in parsed_data.get('a', [])]
                formatted_data = {"bids": bids, "asks": asks}

            else:
                print("Non-order book message received from Binance, ignoring.")
                return

        elif exchange == 'okx':
            if 'arg' in parsed_data and 'data' in parsed_data:
                symbol = parsed_data['arg'].get('instId', '').lower()
                normalized_symbol = normalize_pair(symbol, 'okx')
                data_entry = parsed_data.get('data', [{}])[0]
                formatted_data = {"bids": data_entry.get("bids", []), "asks": data_entry.get("asks", [])}

            else:
                print("Non-order book message received from OKX, ignoring.")
                return

        # Process order book data if available
        if formatted_data and (formatted_data["bids"] or formatted_data["asks"]):
            unique_key = f"{exchange}_{normalized_symbol}"
            print(f"Processing order book for '{unique_key}': Bids = {len(formatted_data['bids'])}, Asks = {len(formatted_data['asks'])}")

            if config.aggregation_enabled:
                print(f"Aggregation is enabled for pair '{normalized_symbol}'")
                aggregate_books(normalized_symbol, formatted_data, exchange)
                push_aggregated_data_to_spreadsheet(normalized_symbol)
            else:
                print(f"Aggregation disabled - pushing data to sheet for '{unique_key}'")
                update_google_sheet(unique_key, formatted_data["bids"], formatted_data["asks"], exchange)
        else:
            print(f"Error: No valid order book data for '{normalized_symbol}' on '{exchange}'.")

    except Exception as e:
        print(f"Error encountered in {exchange} WebSocket: {str(e)}")

def update_google_sheet(symbol, bids, asks, exchange, update_interval=10):
    """Update Google Sheets with order book data with a reduced update frequency."""
    try:
        global last_update_times, google_sheet_cache, sheet
        current_time = time.time()

        unique_key = symbol

        # Initialize the cache entry if it doesn't exist
        if unique_key not in google_sheet_cache:
            google_sheet_cache[unique_key] = {"bids": [], "asks": []}

        # Update the cache with the latest order book data
        google_sheet_cache[unique_key] = {
            "bids": bids[:depth],  # Limit to specified depth
            "asks": asks[:depth]
        }

        # Only update the sheet if enough time has passed since the last update
        if current_time - last_update_times.get(unique_key, 0) >= update_interval:
            # Prepare data for Google Sheets
            data = [
                [f'Level {i + 1}', bid[0] or 'N/A', bid[1] or 'N/A', ask[0] or 'N/A', ask[1] or 'N/A']
                for i, (bid, ask) in enumerate(zip(google_sheet_cache[unique_key]["bids"], google_sheet_cache[unique_key]["asks"]))
            ]

            # Calculate start and end rows for this symbol's data in the sheet
            try:
                start_row = (list(order_books.keys()).index(unique_key) * (depth + 3)) + 2
                end_row = start_row + depth - 1
            except ValueError:
                print(f"Error: Could not find '{unique_key}' in order_books keys for row indexing.")
                return

            # Initialize header in Google Sheets if this symbol is new
            if last_update_times.get(unique_key, 0) == 0:
                try:
                    sheet.update(range_name=f'A{start_row - 1}', values=[[f'{unique_key.upper()} {exchange.upper()} Market Data']])
                    sheet.update(range_name=f'B{start_row - 1}', values=[['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity']])
                except Exception as e:
                    print(f"Exception during initial header update for '{unique_key}': {e}")
                    return

            # Update Google Sheets with the cached order book data
            try:
                sheet.update(range_name=f'B{start_row}:F{end_row}', values=data)
                last_update_times[unique_key] = current_time
                print(f"Google Sheet updated for {unique_key} on {exchange}")
            except Exception as e:
                print(f"Exception during data update for '{unique_key}': {e}")

    except Exception as e:
        print(f"General exception in update_google_sheet: {e}")


def push_aggregated_data_to_spreadsheet(normalized_pair):
    """Push aggregated order book data to the Google Sheet."""
    try:
        if normalized_pair not in aggregated_books:
            print(f"Error: Aggregated book for pair '{normalized_pair}' not found.")
            return

        current_time = time.time()

        bids = list(aggregated_books[normalized_pair]['bids'].items())[:depth]
        asks = list(aggregated_books[normalized_pair]['asks'].items())[:depth]

        formatted_bids = [
            [
                f'Level {i + 1}',
                bid_price,  # Bid price
                bid_data['quantity'],  # Bid quantity
                ', '.join(sorted(bid_data['contributors']))  # Data source(s)
            ]
            for i, (bid_price, bid_data) in enumerate(bids)
        ]

        formatted_asks = [
            [
                f'Level {i + 1}',
                ask_price,  # Ask price
                ask_data['quantity'],  # Ask quantity
                ', '.join(sorted(ask_data['contributors']))  # Data source(s)
            ]
            for i, (ask_price, ask_data) in enumerate(asks)
        ]

        while len(formatted_bids) < depth:
            formatted_bids.append([f'Level {len(formatted_bids) + 1}', 'N/A', 'N/A', 'N/A'])

        while len(formatted_asks) < depth:
            formatted_asks.append([f'Level {len(formatted_asks) + 1}', 'N/A', 'N/A', 'N/A'])

        data = [
            [f'Level {i + 1}', formatted_bids[i][1], formatted_bids[i][2], formatted_bids[i][3], formatted_asks[i][1],
             formatted_asks[i][2], formatted_asks[i][3]]
            for i in range(depth)
        ]

        start_row = (list(order_books.keys()).index(f'binance_{normalized_pair}') * (depth + 4)) + 2
        end_row = start_row + depth - 1

        sheet.batch_clear([f'B{start_row}:H{end_row}'])
        sheet.update(values=[[f'{normalized_pair.upper()} Aggregated Order Book']], range_name=f'A{start_row - 1}')
        sheet.update(values=[['Level', 'Bid Price', 'Bid Quantity', 'Source', 'Ask Price', 'Ask Quantity', 'Source']],
                     range_name=f'B{start_row - 1}')
        sheet.update(values=data, range_name=f'B{start_row}:H{end_row}')

        last_update_times[normalized_pair] = current_time
        print(f"Aggregated order book for {normalized_pair} successfully pushed to Google Sheets.")

    except Exception as e:
        print(f"Error in push_aggregated_data_to_spreadsheet: {e}")


def on_error(ws, error, exchange):
    print(f"Error on {exchange}: {error}")


def on_close(ws, *args):
    print(f"WebSocket closed")


def on_open(ws, exchange):
    print(f"WebSocket connection opened to {exchange}")


def main():
    initialize_order_books()
    websockets = []

    try:
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
            kraken_pairs = config.exchanges['kraken']['pairs']
            kraken_ws = KrakenWebSocket(
                symbols=kraken_pairs,
                on_message_callback=lambda ws, msg: on_message_kraken(ws, msg),  # Use dedicated Kraken handler
                on_error_callback=lambda ws, err: on_error(ws, err, 'kraken'),
                on_close_callback=lambda ws: on_close(ws),
                on_open_callback=lambda ws: on_open(ws, 'kraken')
            )
            kraken_ws.start()
            websockets.append(kraken_ws)
            print("Connected to Kraken.")

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