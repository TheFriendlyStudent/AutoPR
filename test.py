import os
import time
import boto3
import requests
from autoRender import delete_from_r2, render_from_csv
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# -----------------------------
# CONFIGURATION
# -----------------------------
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")

MY_APP_ID = os.getenv("MY_APP_ID")
MY_APP_SECRET = os.getenv("MY_APP_SECRET")
MY_ACCESS_TOKEN = os.getenv("MY_ACCESS_TOKEN")

PAGE_ID = "1025441090651921"  # Facebook Page ID


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

client = create_s3_client()

# -----------------------------
# STEP 0: Get Instagram Business ID from Page
# -----------------------------
page_response = requests.get(
    f"https://graph.facebook.com/v19.0/{PAGE_ID}",
    params={
        "fields": "instagram_business_account",
        "access_token": MY_ACCESS_TOKEN
    }
)
page_response.raise_for_status()
page_data = page_response.json()

if "instagram_business_account" not in page_data:
    raise ValueError("Page does not have an Instagram Business account linked.")

IG_USER_ID = page_data["instagram_business_account"]["id"]
print("Instagram Business ID:", IG_USER_ID)

# -----------------------------
# STEP 1: Render images
# -----------------------------
urls = render_from_csv("docs/games.csv")  # should return list of public URLs

# -----------------------------
# STEP 2: Upload images as carousel children
# -----------------------------
creation_ids = []
for url in urls[:10]:  # max 10 images for carousel
    response = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
        data={
            "image_url": url,
            "is_carousel_item": "true",
            "access_token": MY_ACCESS_TOKEN
        }
    )
    print("Status:", response.status_code)
    print("Response:", response.text) 
    response.raise_for_status()
    creation_id = response.json().get("id")
    if not creation_id:
        raise ValueError(f"No ID returned for image: {url}")
    creation_ids.append(creation_id)
print("Uploaded images:", creation_ids)

# -----------------------------
# STEP 3: Create carousel container
# -----------------------------
carousel_response = requests.post(
    f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
    data={
        "media_type": "CAROUSEL",
        "children": ",".join(creation_ids),
        "caption": "Automated Score Report",
        "access_token": MY_ACCESS_TOKEN
    }
)
carousel_response.raise_for_status()
carousel_id = carousel_response.json()["id"]
print("Carousel container ID:", carousel_id)

# -----------------------------
# STEP 4: Publish carousel
# -----------------------------
publish_response = requests.post(
    f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
    data={
        "creation_id": carousel_id,
        "access_token": MY_ACCESS_TOKEN
    }
)

# Get the returned carousel ID
carousel_id = publish_response.json().get("id")
print("Instagram carousel published! ID:", carousel_id)

# --- Poll for permalink (optional, for reassurance) ---
max_attempts = 10
attempt = 0
permalink = None

while attempt < max_attempts:
    resp = requests.get(
        f"https://graph.facebook.com/v19.0/{carousel_id}",
        params={
            "fields": "id,permalink,media_type",
            "access_token": MY_ACCESS_TOKEN
        }
    ).json()

    permalink = resp.get("permalink")
    if permalink:
        print("Post is live at:", permalink)
        break

    attempt += 1
    time.sleep(3)  # wait a few seconds before retry

if not permalink:
    print("Warning: permalink not available yet. Post may still be processing.") 
    publish_response = requests.post( f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
    data={
        "creation_id": carousel_id,
        "access_token": MY_ACCESS_TOKEN
    }
)

status_url = f"https://graph.facebook.com/v19.0/{carousel_id}"
params = {
    "fields": "status_code",
    "access_token": MY_ACCESS_TOKEN
}

max_attempts = 30  # ~60 seconds
attempt = 0

time.sleep(2)  # initial delay before first check

print("Instagram carousel published! ID:", carousel_id)

# -----------------------------
# STEP 5: Cleanup local R2 images
# -----------------------------
for url in urls:
    file_name = url.split("/")[-1]
    delete_from_r2(client,file_name)