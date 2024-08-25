import json
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Load the config file
with open("config.json") as config_file:
    config = json.load(config_file)

# Retrieve the path from the config
json_keyfile_name = config.get("GOOGLE_APPLICATION_CREDENTIALS")

def setup_google_sheets(json_keyfile_name, sheet_id):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    return sheet

# Continue with the rest of your code
sheet_id = "1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU"
sheet = setup_google_sheets(json_keyfile_name, sheet_id)

from exchanges.binance import BinanceWebSocket
from config import BINANCE_SYMBOLS

def update_sheet(sheet, data):
    try:
        print("Attempting to update the sheet")
        ticker_data = data['data']  # Access the 'data' key
        sheet.append_row([ticker_data['E'], ticker_data['s'], ticker_data['c']])
        print("Sheet updated successfully")
    except Exception as e:
        print(f"Failed to update sheet: {e}")

def on_message(ws, message):
    data = json.loads(message)
    print("Received data:", data)
    update_sheet(sheet, data)

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")

if __name__ == "__main__":
    binance_ws = BinanceWebSocket(BINANCE_SYMBOLS, on_message, on_error, on_close, on_open)
    binance_ws.start()
