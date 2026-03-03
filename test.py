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
ACCOUNT_ID = "431c103f6e281a32903d97ba785cd492"
ACCESS_KEY = "d94ec471afb6b8bb01584eb9941dd922"
SECRET_KEY = "3e9aa8b28e2f1fc3daa3ac316e4329fa575c202048bd5200c4aeef72693bc819"
BUCKET_NAME = "autopr-images"
PUBLIC_URL_BASE = "https://pub-09044e0f73124f66b358b016f903929b.r2.dev"

IG_USER_ID = "17841468652872390" 

MY_APP_ID = '1598679764670845'
MY_APP_SECRET = '019abfe854d55e46f7a2135e1d454cdb'
MY_ACCESS_TOKEN = 'EAAWtZCaN7mX0BQyRdOF9F09DV1OHNmVpvKIBMO5grrQbAU5BY7KFfkl8hj9zqJwZAbIAAHtPIidawZChIhH4cZA46llmtwZCxZAeKv2s8SLNJ3ykm9j1DlUZBVtncKady2HS8m8zqb41v7pAZAfZB9JvuYd6dtyZBkFIYiZA7xZC0rad1gqb3HeBFXl1zqeXZCDjiOkZBBw3EmXd8QWx16oZBZCMrojh1mVhgb4ZAZC460DOJfXW5ZCFmNvsttiQkpWET5PK4WbnzZCmXZB7ZBwvBbe3vb5VCRfm88'

# Initialize Facebook API
FacebookAdsApi.init(MY_APP_ID, MY_APP_SECRET, MY_ACCESS_TOKEN)

# Optional: access account / user info
my_account = AdAccount("act_771986928687472")
me = User(fbid='me')
page = Page("1025441090651921")
ig_account = page.api_get(fields=["instagram_business_account"])
ig_user = InstagramUser(IG_USER_ID)
ig_info = ig_user.api_get(fields=['id', 'username', 'followers_count', 'media_count'])

urls = render_from_csv("games.csv")

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