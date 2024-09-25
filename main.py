import json
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
from exchanges.binance import BinanceWebSocket
from exchanges.okx import OKXWebSocket
from exchanges.kraken import KrakenWebSocket
from exchanges.coinbase import connect as coinbase_connect
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

    exchange_code = exchange[:2].capitalize()  # Use the first two letters of the exchange name

    # Aggregate bids
    for price, quantity in book.get('bids', []):
        price_str = f"{price:.8f}"  # Keep price as string for dictionary key
        if float(quantity) > 0:  # Only include valid levels with quantities
            if price_str in aggregated_books[normalized_pair]['bids']:
                aggregated_books[normalized_pair]['bids'][price_str]['quantity'] += float(quantity)
                aggregated_books[normalized_pair]['bids'][price_str]['contributors'].add(exchange_code)
            else:
                aggregated_books[normalized_pair]['bids'][price_str] = {'quantity': float(quantity),
                                                                        'contributors': {exchange_code}}

    # Aggregate asks
    for price, quantity in book.get('asks', []):
        price_str = f"{price:.8f}"  # Keep price as string for dictionary key
        if float(quantity) > 0:  # Only include valid levels with quantities
            if price_str in aggregated_books[normalized_pair]['asks']:
                aggregated_books[normalized_pair]['asks'][price_str]['quantity'] += float(quantity)
                aggregated_books[normalized_pair]['asks'][price_str]['contributors'].add(exchange_code)
            else:
                aggregated_books[normalized_pair]['asks'][price_str] = {'quantity': float(quantity),
                                                                        'contributors': {exchange_code}}

    # Limit to top 'depth' bids/asks sorted by price (convert keys back to floats for sorting)
    aggregated_books[normalized_pair]['bids'] = dict(sorted(
        aggregated_books[normalized_pair]['bids'].items(),
        key=lambda x: -float(x[0])
    )[:depth])

    aggregated_books[normalized_pair]['asks'] = dict(sorted(
        aggregated_books[normalized_pair]['asks'].items(),
        key=lambda x: float(x[0])
    )[:depth])

    print(
        f"Aggregated book for {normalized_pair}: Bids = {len(aggregated_books[normalized_pair]['bids'])}, Asks = {len(aggregated_books[normalized_pair]['asks'])}")


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
    """Dedicated handler for Kraken WebSocket messages."""
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
            symbol = parsed_data['symbol']
            bids = parsed_data['bids']
            asks = parsed_data['asks']
            print(f"Processing order book for symbol: {symbol}")

            normalized_pair = normalize_pair(symbol, 'kraken')
            bids = [bid[:2] for bid in bids]  # Keep only price and size
            asks = [ask[:2] for ask in asks]  # Keep only price and size

            if bids or asks:
                print(f"Updating Kraken order book for {symbol}: Bids = {len(bids)}, Asks = {len(asks)}")
                unique_key = f"kraken_{normalized_pair}"
                update_google_sheet(unique_key, bids, asks, 'kraken')
            else:
                print(f"No bids or asks found for {symbol}")
        else:
            print(f"Unhandled Kraken message structure: {parsed_data}")
    except Exception as e:
        print(f"Error processing Kraken message: {str(e)}")


def on_message(ws, message, exchange):
    """Process incoming WebSocket messages and handle order book updates."""
    try:
        parsed_data = json.loads(message)
        print(f"Received message from {exchange}: {parsed_data}")

        pair = None
        order_book = None

        if exchange == 'binance':
            pair = parsed_data.get('s', '').lower()

            print(f"Debug: Bids structure from Binance: {parsed_data.get('b', [])}")
            print(f"Debug: Asks structure from Binance: {parsed_data.get('a', [])}")

            try:
                bids = [[float(bid[0]), float(bid[1])] for bid in parsed_data.get('b', []) if
                        isinstance(bid, list) and len(bid) == 2]
                asks = [[float(ask[0]), float(ask[1])] for ask in parsed_data.get('a', []) if
                        isinstance(ask, list) and len(ask) == 2]
                print(f"Processed Bids: {bids}")
                print(f"Processed Asks: {asks}")
            except (IndexError, ValueError, TypeError) as e:
                print(f"Error processing bids/asks: {e}")
                return

            order_book = {
                'bids': bids,
                'asks': asks
            }

        elif exchange == 'okx':
            pair = parsed_data['arg']['instId'].lower()
            order_book = parsed_data.get('data', [{}])[0]

        if pair:
            normalized_pair = normalize_pair(pair, exchange)
            unique_key = f"{exchange}_{normalized_pair}"

            if order_book:
                print(
                    f"Debug: Order book data for '{unique_key}' - Bids: {order_book['bids']}, Asks: {order_book['asks']}")

                if config.aggregation_enabled:
                    print(f"Debug: Aggregation is enabled for pair '{normalized_pair}'")
                    aggregate_books(pair, order_book, exchange)
                    push_aggregated_data_to_spreadsheet(normalized_pair)
                else:
                    print(f"Debug: Aggregation disabled - pushing data to sheet for '{unique_key}'")
                    update_google_sheet(unique_key, order_book['bids'], order_book['asks'], exchange)
            else:
                print(f"Error: No order book data found for pair '{unique_key}' on '{exchange}'.")
    except Exception as e:
        print(f"Error encountered in {exchange} WebSocket: {str(e)}")


def update_google_sheet(symbol, bids, asks, exchange):
    """Update Google Sheets with order book data."""
    try:
        global last_update_times
        current_time = time.time()

        unique_key = symbol

        if unique_key not in order_books:
            print(f"Error: Symbol '{unique_key}' not found in order_books for exchange '{exchange}'.")
            return

        limited_bids = bids[:depth]
        limited_asks = asks[:depth]

        while len(limited_bids) < depth:
            limited_bids.append([None, None])
        while len(limited_asks) < depth:
            limited_asks.append([None, None])

        data = [[f'Level {i + 1}', bid[0] or 'N/A', bid[1] or 'N/A', ask[0] or 'N/A', ask[1] or 'N/A'] for i, (bid, ask)
                in enumerate(zip(limited_bids, limited_asks))]

        start_row = (list(order_books.keys()).index(unique_key) * (depth + 3)) + 2
        end_row = start_row + depth - 1

        if last_update_times[unique_key] == 0:
            sheet.update(range_name=f'A{start_row - 1}',
                         values=[[f'{unique_key.upper()} {exchange.upper()} Market Data']])
            sheet.update(range_name=f'B{start_row - 1}',
                         values=[['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity']])

        sheet.update(range_name=f'B{start_row}:F{end_row}', values=data)

        last_update_times[unique_key] = current_time
        print(f"Google Sheet updated for {unique_key} on {exchange}")

    except Exception as e:
        print(f"Exception in update_google_sheet: {e}")


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