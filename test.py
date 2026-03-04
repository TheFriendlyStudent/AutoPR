import os

import requests
from autoRender import delete_from_r2, render_from_csv

# -----------------------------
# Facebook / Instagram SDK
# -----------------------------
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.user import User
from facebook_business.adobjects.instagramuser import InstagramUser

# -----------------------------
# CONFIGURATION
# -----------------------------
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")

IG_USER_ID = os.getenv("IG_USER_ID")  # Instagram Business Account ID to post to

MY_APP_ID = os.getenv("MY_APP_ID")
MY_APP_SECRET = os.getenv("MY_APP_SECRET")
MY_ACCESS_TOKEN = os.getenv("MY_ACCESS_TOKEN")

# Initialize Facebook API
FacebookAdsApi.init(MY_APP_ID, MY_APP_SECRET, MY_ACCESS_TOKEN)

# Optional: access account / user info
my_account = AdAccount("act_771986928687472")
me = User(fbid='me')
page = Page("1025441090651921")
ig_account = page.api_get(fields=["instagram_business_account"])
ig_user = InstagramUser(IG_USER_ID)
ig_info = ig_user.api_get(fields=['id', 'username', 'followers_count', 'media_count'])

urls = render_from_csv("docs/games.csv")

# -----------------------------
# POST TO INSTAGRAM (Graph API)
# -----------------------------

collaborator_usernames = ["ctbasketballhub"]  # Instagram usernames of potential collaborators
collaborator_ids = []

for username in collaborator_usernames:
    try:
        r = requests.get(
            f"https://graph.facebook.com/v19.0/{username}",
            params={"access_token": MY_ACCESS_TOKEN, "fields": "id"}
        )
        r.raise_for_status()
        collaborator_ids.append(r.json()["id"])
    except Exception as e:
        print(f"Could not get ID for {username}: {e}")

# Step 1: Create media objects for carousel
creation_ids = []
for url in urls:
    response = requests.post(
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
        params={
            "image_url": url,
            "is_carousel_item": True,
            "access_token": MY_ACCESS_TOKEN,
            "collaborators": collaborator_ids  
        }
    )
    response.raise_for_status()
    creation_ids.append(response.json()["id"])
    if len(creation_ids) >= 10:  # Instagram carousel limit
        break

# Step 2: Create the carousel container
carousel_response = requests.post(
    f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
    params={
        "media_type": "CAROUSEL",
        "children": ",".join(creation_ids),
        "caption": "Automated Score Report",
        "access_token": MY_ACCESS_TOKEN
    }
)
carousel_response.raise_for_status()
carousel_id = carousel_response.json()["id"]

# Step 3: Publish carousel
publish_response = requests.post(
    f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
    params={
        "creation_id": carousel_id,
        "access_token": MY_ACCESS_TOKEN
    }
)
publish_response.raise_for_status()
print(f"Instagram carousel published! ID: {carousel_id}")

for url in urls:
    file_name = url.split("/")[-1]
    delete_from_r2(file_name)