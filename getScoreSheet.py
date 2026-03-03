import json
import os
import gspread
from google.oauth2.service_account import Credentials

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


if "GOOGLE_CREDENTIALS" in os.environ:
    # Running in GitHub Actions
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
else:
    # Running locally
    creds = Credentials.from_service_account_file(
        "secrets//autopr-489119-5c3d04856538.json",
        scopes=scope
    )

client = gspread.authorize(creds)

spreadsheet = client.open_by_key("1UJzab8BwMgScaYoLqTBKk_8sDy7Y7cNp3YqrrZhy38I")

sheet = spreadsheet.worksheet("working_data")

# Get all rows as dictionaries (uses row 1 as headers)
data = sheet.get_all_records()

print(data)