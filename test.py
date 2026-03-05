import os
import time
import boto3
import requests
from autoRender import delete_from_r2, render_from_csv, mark_posted
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")

MY_APP_ID = os.getenv("MY_APP_ID")
MY_APP_SECRET = os.getenv("MY_APP_SECRET")
MY_ACCESS_TOKEN = os.getenv("MY_ACCESS_TOKEN")

PAGE_ID = "1025441090651921"

MASTER_CSV = "docs/master_games.csv"


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

client = create_s3_client()

# --- Step 0: Get Instagram Business ID ---
page_response = requests.get(
    f"https://graph.facebook.com/v19.0/{PAGE_ID}",
    params={"fields": "instagram_business_account", "access_token": MY_ACCESS_TOKEN}
)
page_response.raise_for_status()
page_data = page_response.json()

if "instagram_business_account" not in page_data:
    raise ValueError("Page does not have an Instagram Business account linked.")

IG_USER_ID = page_data["instagram_business_account"]["id"]
print("Instagram Business ID:", IG_USER_ID)

# --- Step 1: Render unposted final games ---
# Returns list of (game_id, url)
rendered = render_from_csv(MASTER_CSV)

if not rendered:
    print("No new games to post. Exiting.")
    exit(0)

game_ids = [r[0] for r in rendered]
urls = [r[1] for r in rendered]

print(f"Rendered {len(urls)} image(s) for posting.")

# --- Step 2: Upload carousel children ---
creation_ids = []
for url in urls[:10]:
    response = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
        data={"image_url": url, "is_carousel_item": "true", "access_token": MY_ACCESS_TOKEN}
    )
    response.raise_for_status()
    creation_id = response.json().get("id")
    if not creation_id:
        raise ValueError(f"No ID returned for image: {url}")
    creation_ids.append(creation_id)

print("Uploaded images:", creation_ids)

# --- Step 3: Create carousel container ---
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
carousel_id = carousel_response.json().get("id")
if not carousel_id:
    raise ValueError("Failed to create carousel container.")
print("Carousel container ID:", carousel_id)

# --- Step 4: Publish ---
publish_response = requests.post(
    f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
    data={"creation_id": carousel_id, "access_token": MY_ACCESS_TOKEN}
)
publish_response.raise_for_status()
published_id = publish_response.json().get("id")
print("Publish request sent. Temporary ID:", published_id)

# --- Step 4a: Poll for permalink ---
max_attempts = 15
sleep_seconds = 4
permalink = None

for attempt in range(max_attempts):
    resp = requests.get(
        f"https://graph.facebook.com/v19.0/{carousel_id}",
        params={"fields": "id,permalink,media_type", "access_token": MY_ACCESS_TOKEN}
    ).json()
    permalink = resp.get("permalink")
    if permalink:
        print("Post is live at:", permalink)
        break
    print(f"Attempt {attempt+1}/{max_attempts}: not ready, waiting {sleep_seconds}s...")
    time.sleep(sleep_seconds)

if not permalink:
    print("Warning: permalink not available yet.")

# --- Step 5: Mark games as posted in master CSV ---
for game_id in game_ids:
    mark_posted(MASTER_CSV, game_id)

print(f"Marked {len(game_ids)} game(s) as posted_to_instagram=true.")

# --- Step 6: Cleanup R2 images ---
for url in urls:
    file_name = url.split("/")[-1]
    delete_from_r2(client, file_name)

print("Done.")
