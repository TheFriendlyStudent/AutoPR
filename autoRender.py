import csv
import datetime
import os
import re
from io import BytesIO
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
import requests
from PIL import Image

from renderGraphic import render_image
from dotenv import load_dotenv
import boto3

load_dotenv(dotenv_path=".env")

# -----------------------------
# R2 / S3 setup
# -----------------------------
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# -----------------------------
# Utilities
# -----------------------------
def upload_to_r2(file_path, file_name=None):
    if file_name is None:
        file_name = os.path.basename(file_path)
    s3.upload_file(file_path, BUCKET_NAME, file_name, ExtraArgs={"ContentType": "image/png"})
    return f"{PUBLIC_URL_BASE}/{file_name}"

def delete_from_r2(file_name):
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)

def convert_drive_link(url):
    if "drive.google.com" not in url:
        return None
    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if not match:
        return None
    file_id = match.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def download_image(url, save_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(response.content)
    return save_path

# -----------------------------
# Render a single game
# -----------------------------
def render_game(row, template_png="graphic.png"):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")
    output_png = f"C:\\Users\\vasub\\AutoPR\\output_{timestamp}.png"

    # Skip empty datetime
    game_date_time_str = row.get("game_datetime")
    if not game_date_time_str:
        return None

    eastern = pytz.timezone("US/Eastern")
    central = pytz.timezone("US/Central")
    game_dt_naive = datetime.datetime.strptime(game_date_time_str, "%m/%d/%Y %H:%M:%S")
    game_dt_est = eastern.localize(game_dt_naive)
    now_cst = datetime.datetime.now(central)
    if game_dt_est > now_cst.astimezone(eastern):
        return None

    if row.get("is_test", "").lower() != "true":
        bg_url = row.get("bg_image")
        background_path = bg_url
        local_bg = None

        drive_direct = convert_drive_link(bg_url)
        if drive_direct:
            local_bg = f"C:\\Users\\vasub\\AutoPR\\temp_bg_{timestamp}.jpg"
            download_image(drive_direct, local_bg)
            background_path = local_bg

        # Render the image
        render_image(
            output_path=output_png,
            home_won=int(row.get("home_score", 0)) > int(row.get("away_score", 0)),
            title_text=row.get("header", ""),
            caption_text=row.get("caption", ""),
            home_score=int(row.get("home_score", 0)),
            away_score=int(row.get("away_score", 0)),
            home_rank=row.get("home_rank", ""),
            away_rank=row.get("away_rank", ""),
            home_record=row.get("home_record", ""),
            away_record=row.get("away_record", ""),
            home_team=row.get("home_team", ""),
            away_team=row.get("away_team", ""),
            photo_text="PHOTO: @" + row.get("photo_cred", ""),
            template_png=template_png,
            background_image=background_path
        )

        # Upload
        url = upload_to_r2(output_png)

        # Cleanup
        os.remove(output_png)
        if local_bg:
            os.remove(local_bg)

        return url
    return None

# -----------------------------
# Multithreaded CSV processing
# -----------------------------
def render_from_csv(csv_path, template_png="graphic.png", max_threads=4):
    urls = []
    with open(csv_path, newline='', encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(render_game, row, template_png): row for row in rows}
        for future in as_completed(futures):
            print(f"Rendering game: {row['game_datetime']}")
            print(f"Home logo: assets/logos/{row['home_team'].replace(' ', '_')}.jpg")
            print(f"Away logo: assets/logos/{row['away_team'].replace(' ', '_')}.jpg")
            try:
                url = future.result()
                if url:
                    urls.append(url)
                    print(f"[OK] Rendered & uploaded: {url}")
            except Exception as e:
                row = futures[future]
                print(f"[ERROR] Failed to render {row.get('game_datetime')}: {e}")

    return urls