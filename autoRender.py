import csv
import datetime
import os
import re
import uuid
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
import requests
from PIL import Image
import boto3

from renderGraphic import render_image
from dotenv import load_dotenv

load_dotenv(".env")

# -----------------------------
# R2 / S3 setup
# -----------------------------
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )



# -----------------------------
# Utilities
# -----------------------------

def delete_from_r2(client, file_name):
    try:
        client.delete_object(
            Bucket=BUCKET_NAME,
            Key=file_name
        )
        return True
    except Exception as e:
        print(f"[ERROR] Failed to delete {file_name} from R2: {e}")
        return False
    
def upload_to_r2(client, file_path, file_name):
    client.upload_file(
        file_path,
        BUCKET_NAME,
        file_name,
        ExtraArgs={"ContentType": "image/png"}
    )
    return f"{PUBLIC_URL_BASE}/{file_name}"


def convert_drive_link(url):
    if "drive.google.com" not in url:
        return None

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
    if not match:
        return None

    file_id = match.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def download_image(url, save_path):
    r = requests.get(url)
    r.raise_for_status()

    with open(save_path, "wb") as f:
        f.write(r.content)

    return save_path


# -----------------------------
# Render a single game
# -----------------------------
def render_game(row, template_png="graphic.png"):

    # Unique working directory for this thread
    with tempfile.TemporaryDirectory() as tmpdir:

        client = create_s3_client()

        uid = str(uuid.uuid4())

        output_png = os.path.join(tmpdir, f"{uid}.png")
        bg_local = None

        game_date_time_str = row.get("game_datetime")
        if not game_date_time_str:
            return None

        eastern = pytz.timezone("US/Eastern")
        central = pytz.timezone("US/Central")

        game_dt_naive = datetime.datetime.strptime(
            game_date_time_str,
            "%m/%d/%Y %H:%M:%S"
        )

        game_dt_est = eastern.localize(game_dt_naive)
        now_cst = datetime.datetime.now(central)

        if game_dt_est > now_cst.astimezone(eastern):
            return None

        if row.get("is_test", "").lower() == "true":
            return None

        # -----------------------------
        # Background image
        # -----------------------------
        bg_url = row.get("bg_image")
        background_path = bg_url

        drive_direct = convert_drive_link(bg_url)

        if drive_direct:
            bg_local = os.path.join(tmpdir, f"bg_{uid}.jpg")
            download_image(drive_direct, bg_local)
            background_path = bg_local

        # -----------------------------
        # Render image
        # -----------------------------
        render_image(
            output_path=output_png,
            home_won=int(row.get("home_score", 0)) >
            int(row.get("away_score", 0)),
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

        # -----------------------------
        # Upload
        # -----------------------------
        r2_name = f"{uid}.png"

        url = upload_to_r2(
            client,
            output_png,
            r2_name
        )

        return url


# -----------------------------
# Multithreaded CSV processing
# -----------------------------
def render_from_csv(csv_path, template_png="graphic.png", max_threads=4):

    urls = []

    with open(csv_path, newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    with ThreadPoolExecutor(max_workers=max_threads) as executor:

        futures = {
            executor.submit(render_game, row, template_png): row
            for row in rows
        }

        for future in as_completed(futures):

            row = futures[future]

            try:
                print(f"Rendering game: {row.get('game_datetime')}")
                print(
                    f"Home logo: assets/logos/{row.get('home_team','').replace(' ', '_')}.jpg"
                )
                print(
                    f"Away logo: assets/logos/{row.get('away_team','').replace(' ', '_')}.jpg"
                )

                url = future.result()

                if url:
                    urls.append(url)
                    print(f"[OK] Uploaded: {url}")

            except Exception as e:
                print(
                    f"[ERROR] Failed to render {row.get('game_datetime')}: {e}"
                )

    return urls