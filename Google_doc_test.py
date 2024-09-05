import gspread
from oauth2client.service_account import ServiceAccountCredentials

"""
def test_google_sheets():
    try:
        # Set up Google Sheets API
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('ServiceAccountCredentials.json', scope)
        client = gspread.authorize(creds)

        # Open the Google Sheet
        sheet = client.open_by_key('1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU').sheet1

        # Test fetching data from the sheet
        data = sheet.get_all_records()
        print("Current Google Sheet data:", data)
    except Exception as e:
        print(f"Failed to access Google Sheets: {e}")

# Run the test
test_google_sheets() 
"""


def test_google_sheets_update():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('ServiceAccountCredentials.json', scope)
    client = gspread.authorize(creds)
    try:
        static_data = [
            ['Level', 'Bid Price', 'Bid Quantity', 'Ask Price', 'Ask Quantity'],
            ['Level 1', '50000', '1.5', '50001', '1.2'],
            ['Level 2', '49999', '0.8', '50002', '0.9']
        ]
        sheet = client.open_by_key('1rzcGKK4dMGWhthJQWn7wSSLpaVehNs5zV1GmSZ1yVZU').sheet1
        sheet.update(range_name='A1', values=static_data)
        print("Static data updated in Google Sheet.")
    except Exception as e:
        print(f"Failed to update Google Sheets with static data: {e}")

test_google_sheets_update()
