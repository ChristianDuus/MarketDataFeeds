from exchanges.binance import BinanceWebSocket
from config import BINANCE_SYMBOLS, SHEET_NAME, JSON_KEYFILE_NAME
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def setup_google_sheets(json_keyfile_name, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    return sheet

def update_sheet(sheet, data):
    sheet.append_row([data['E'], data['s'], data['c']])

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
    sheet = setup_google_sheets(JSON_KEYFILE_NAME, SHEET_NAME)
    
    # Binance WebSocket
    binance_ws = BinanceWebSocket(BINANCE_SYMBOLS, on_message, on_error, on_close, on_open)
    binance_ws.start()
