import gspread
from oauth2client.service_account import ServiceAccountCredentials


def setup_google_sheets(json_keyfile_name, sheet_id):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    return sheet


def test_google_sheet_connection():
    json_keyfile_name = "C:/Users/cduus/MarketDataFeeds/ServiceAccountCredentials.json"
    sheet_id = "1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU"

    sheet = setup_google_sheets(json_keyfile_name, sheet_id)

    try:
        sheet.append_row(["Test Timestamp", "Test Symbol", "Test Price"])
        print("Test row added successfully.")
    except Exception as e:
        print(f"Error during test write: {e}")


test_google_sheet_connection()
