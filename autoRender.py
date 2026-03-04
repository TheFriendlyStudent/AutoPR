import csv
import datetime
import os
from tkinter import Image
import pytz 

from renderGraphic import render_image
import boto3

ACCOUNT_ID = "431c103f6e281a32903d97ba785cd492"
ACCESS_KEY = "d94ec471afb6b8bb01584eb9941dd922"
SECRET_KEY = "3e9aa8b28e2f1fc3daa3ac316e4329fa575c202048bd5200c4aeef72693bc819"
BUCKET_NAME = "autopr-images"
PUBLIC_URL_BASE = "https://pub-09044e0f73124f66b358b016f903929b.r2.dev"

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

def upload_to_r2(file_path, file_name=None):
    if file_name is None:
        file_name = os.path.basename(file_path)
    s3.upload_file(
        file_path,
        BUCKET_NAME,
        file_name,
        ExtraArgs={"ContentType": "image/png"}
    )
    return f"{PUBLIC_URL_BASE}/{file_name}"

def render_from_csv(csv_path, template_png="graphic.png"):
    urls = []
    with open(csv_path, newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_png = f"C:\\Users\\vasub\\AutoPR\\output_{timestamp}.png"
            game_date_time_str = row['game_datetime']  # from CSV

            # Define the EST and CST timezones
            eastern = pytz.timezone("US/Eastern")
            central = pytz.timezone("US/Central")

            # Parse the CSV string (which is in EST) and localize it
            game_dt_naive = datetime.strptime(game_date_time_str, "%m/%d/%Y %H:%M:%S")
            game_dt_est = eastern.localize(game_dt_naive)

            # Convert to CST for comparison with your local time (or convert now to EST)
            now_cst = datetime.now(central)
            if game_dt_est > now_cst.astimezone(eastern):
                print("Skipping future game")
                continue
            if game_dt_est <= now_cst.astimezone(eastern) and row.get("is_test", "").lower() != "true":   
                render_image(
                    output_path=output_png,
                    home_won=int(row["home_score"]) > int(row["away_score"]),
                    title_text=row["header"],
                    caption_text=row["caption"],
                    home_score=int(row["home_score"]),
                    away_score=int(row["away_score"]),
                    home_rank=row["home_rank"],
                    away_rank=row["away_rank"],
                    home_record=row["home_record"],
                    away_record=row["away_record"],
                    home_team=row["home_team"],
                    away_team=row["away_team"],
                    photo_text="PHOTO: @" + row["photo_cred"],
                    template_png=template_png,
                    background_image=row["bg_image"]
                )
                url = upload_to_r2(output_png)
                urls.append(url)
                os.remove(output_png)
    return urls

def delete_from_r2(file_name):
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)