import csv
import json
import os
import gspread
from google.oauth2.service_account import Credentials

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

if "GOOGLE_CREDENTIALS" in os.environ:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
else:
    creds = Credentials.from_service_account_file(
        "secrets/autopr-489119-5c3d04856538.json",
        scopes=scope
    )

client = gspread.authorize(creds)
spreadsheet = client.open_by_key("1UJzab8BwMgScaYoLqTBKk_8sDy7Y7cNp3YqrrZhy38I")

# -------------------------------------------------------
# 1. Pull master schedule (admin-approved games)
# -------------------------------------------------------
master_sheet = spreadsheet.worksheet("master_games")
master_data = master_sheet.get_all_records()

master_file = "docs/master_games.csv"
if master_data:
    headers = list(master_data[0].keys())
    with open(master_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(master_data)
    print(f"'{master_file}' updated — {len(master_data)} games.")
else:
    print("No master game data found.")

# -------------------------------------------------------
# 2. Pull user-submitted scores (separate sheet)
# -------------------------------------------------------
try:
    sub_sheet = spreadsheet.worksheet("submitted_scores")
    sub_data = sub_sheet.get_all_records()

    sub_file = "docs/submitted_scores.csv"
    if sub_data:
        sub_headers = list(sub_data[0].keys())
        with open(sub_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sub_headers)
            writer.writeheader()
            writer.writerows(sub_data)
        print(f"'{sub_file}' updated — {len(sub_data)} submissions.")
    else:
        print("No submissions found (empty sheet is OK).")
except gspread.exceptions.WorksheetNotFound:
    print("'submitted_scores' worksheet not found — skipping.")

# -------------------------------------------------------
# 3. Pull predictions/votes
# -------------------------------------------------------
try:
    pred_sheet = spreadsheet.worksheet("predictions")
    pred_data = pred_sheet.get_all_records()

    pred_file = "docs/predictions.csv"
    if pred_data:
        pred_headers = list(pred_data[0].keys())
        with open(pred_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=pred_headers)
            writer.writeheader()
            writer.writerows(pred_data)
        print(f"'{pred_file}' updated — {len(pred_data)} votes.")
    else:
        print("No predictions found (empty is OK).")
except gspread.exceptions.WorksheetNotFound:
    print("'predictions' worksheet not found — skipping.")
