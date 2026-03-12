"""
getScoreSheet.py
Pulls master_games, submitted_scores, and predictions from Google Sheets,
writes them to docs/, then merges any admin-approved submitted scores into
master_games.csv so the website immediately reflects user-submitted results.

Records are NOT taken from user input — scrapeTEAMS.py recalculates them
from game results on every run.
"""

import csv
import json
import os

import gspread
from google.oauth2.service_account import Credentials
from schools import canonical_name

MASTER_CSV = "docs/master_games.csv"
SUB_CSV    = "docs/submitted_scores.csv"
PRED_CSV   = "docs/predictions.csv"

MASTER_FIELDS = [
    "game_id", "header", "home_team", "away_team", "home_rank", "away_rank",
    "home_score", "away_score", "home_record", "away_record",
    "bg_image", "photo_cred", "game_datetime", "status",
    "posted_to_instagram", "caption",
]

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

if "GOOGLE_CREDENTIALS" in os.environ:
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
else:
    creds = Credentials.from_service_account_file(
        "secrets/autopr-489119-5c3d04856538.json",
        scopes=scope,
    )

client      = gspread.authorize(creds)
spreadsheet = client.open_by_key("1UJzab8BwMgScaYoLqTBKk_8sDy7Y7cNp3YqrrZhy38I")


# ── 1. Pull master schedule ───────────────────────────────────────────────────

master_sheet = spreadsheet.worksheet("master_games")
master_data  = master_sheet.get_all_records()

if master_data:
    headers = list(master_data[0].keys())
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(master_data)
    print(f"'{MASTER_CSV}' updated — {len(master_data)} games.")
else:
    print("No master game data found.")


# ── 2. Pull user-submitted scores ─────────────────────────────────────────────

sub_data = []
try:
    sub_sheet = spreadsheet.worksheet("submitted_scores")
    sub_data  = sub_sheet.get_all_records()

    if sub_data:
        sub_headers = list(sub_data[0].keys())
        with open(SUB_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sub_headers)
            writer.writeheader()
            writer.writerows(sub_data)
        print(f"'{SUB_CSV}' updated — {len(sub_data)} submissions.")
    else:
        print("No submissions found (empty sheet is OK).")
except gspread.exceptions.WorksheetNotFound:
    print("'submitted_scores' worksheet not found — skipping.")


# ── 3. Pull predictions / votes ───────────────────────────────────────────────

try:
    pred_sheet = spreadsheet.worksheet("predictions")
    pred_data  = pred_sheet.get_all_records()

    if pred_data:
        pred_headers = list(pred_data[0].keys())
        with open(PRED_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=pred_headers)
            writer.writeheader()
            writer.writerows(pred_data)
        print(f"'{PRED_CSV}' updated — {len(pred_data)} votes.")
    else:
        print("No predictions found (empty is OK).")
except gspread.exceptions.WorksheetNotFound:
    print("'predictions' worksheet not found — skipping.")


# ── 4. Merge approved submissions into master_games.csv ───────────────────────
#
# A submission is eligible when its `status` column (set by admin in the sheet)
# equals "approved".  We apply ONLY the score fields — never the record fields,
# because scrapeTEAMS.py owns records.

def apply_approved_submissions(master_csv, submissions):
    """
    For each approved submission, find the matching game in master_csv by
    game_id and overwrite its home_score, away_score, and status=final.
    Scores from approved submissions take precedence over scraped scores.
    """
    approved = [s for s in submissions if str(s.get("status", "")).lower() == "approved"]
    if not approved:
        print("No approved submissions to apply.")
        return

    # Load current master rows
    try:
        with open(master_csv, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"[WARN] {master_csv} not found — cannot apply submissions.")
        return

    if not rows:
        return

    fields = list(rows[0].keys())

    # Build index by game_id
    idx = {row["game_id"]: row for row in rows if row.get("game_id")}

    applied = 0
    for sub in approved:
        gid = str(sub.get("game_id", "")).strip()
        if gid not in idx:
            print(f"  [WARN] Approved submission references unknown game_id: {gid!r}")
            continue

        row = idx[gid]
        try:
            hs = int(sub.get("home_score", ""))
            as_ = int(sub.get("away_score", ""))
        except (ValueError, TypeError):
            print(f"  [WARN] Submission for {gid} has non-integer scores — skipping.")
            continue

        row["home_score"] = str(hs)
        row["away_score"] = str(as_)
        row["status"]     = "final"
        # Preserve existing photo if submission supplies one and master doesn't
        if sub.get("image_url") and not row.get("bg_image"):
            row["bg_image"] = sub["image_url"]
        if sub.get("photo_credit") and not row.get("photo_cred"):
            row["photo_cred"] = sub["photo_credit"]
        applied += 1
        print(f"  Applied approved score for {row['home_team']} vs {row['away_team']}: {hs}–{as_}")

    if applied:
        with open(master_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Applied {applied} approved submission(s) to {master_csv}.")


apply_approved_submissions(MASTER_CSV, sub_data)