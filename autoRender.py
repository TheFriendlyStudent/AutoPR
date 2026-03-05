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


def delete_from_r2(client, file_name):
    try:
        client.delete_object(Bucket=BUCKET_NAME, Key=file_name)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to delete {file_name} from R2: {e}")
        return False


def upload_to_r2(client, file_path, file_name):
    client.upload_file(
        file_path, BUCKET_NAME, file_name,
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


def mark_posted(csv_path, game_id):
    """Mark a game as posted to Instagram in the master CSV."""
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            if row.get("game_id") == game_id:
                row["posted_to_instagram"] = "true"
            rows.append(row)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Marked {game_id} as posted.")


def render_game(row, template_png="graphic.png"):
    """Render a single game graphic and upload to R2. Returns (game_id, url) or None."""

    # Only render games that are final and not yet posted
    if row.get("status", "").lower() != "final":
        return None
    if row.get("posted_to_instagram", "").lower() == "true":
        return None

    game_date_time_str = row.get("game_datetime")
    if not game_date_time_str:
        return None

    eastern = pytz.timezone("US/Eastern")
    central = pytz.timezone("US/Central")

    game_dt_naive = datetime.datetime.strptime(
        game_date_time_str.strip(), "%m/%d/%Y %H:%M:%S"
    )
    game_dt_est = eastern.localize(game_dt_naive)
    now_cst = datetime.datetime.now(central)

    if game_dt_est > now_cst.astimezone(eastern):
        return None

    with tempfile.TemporaryDirectory() as tmpdir:
        client = create_s3_client()
        uid = str(uuid.uuid4())
        output_png = os.path.join(tmpdir, f"{uid}.png")
        bg_local = None

        bg_url = row.get("bg_image", "")
        background_path = bg_url or None
        drive_direct = convert_drive_link(bg_url) if bg_url else None

        if drive_direct:
            bg_local = os.path.join(tmpdir, f"bg_{uid}.jpg")
            download_image(drive_direct, bg_local)
            background_path = bg_local

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

        r2_name = f"{uid}.png"
        url = upload_to_r2(client, output_png, r2_name)
        return (row.get("game_id"), url)


def render_from_csv(csv_path, template_png="graphic.png", max_threads=4):
    """
    Render all eligible games from master_games.csv.
    Returns list of (game_id, url) tuples.
    """
    results = []

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
                print(f"Processing: {row.get('home_team')} vs {row.get('away_team')} [{row.get('game_datetime')}]")
                result = future.result()
                if result:
                    game_id, url = result
                    results.append((game_id, url))
                    print(f"[OK] Rendered & uploaded: {url}")
            except Exception as e:
                print(f"[ERROR] {row.get('home_team')} vs {row.get('away_team')}: {e}")

    return results
