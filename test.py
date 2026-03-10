"""
test.py
Posts today's final unposted CIAC games to Instagram as carousel(s).

Rules:
  - Only games whose date matches today (US/Eastern) are eligible.
  - Instagram allows max 10 images per carousel, so if there are more than
    10 games we create multiple consecutive carousel posts.
  - Each batch is labelled "Part N" in the caption when there is more than one.
"""

import datetime
import os
import time

import boto3
import pytz
import requests
from dotenv import load_dotenv

from autoRender import delete_from_r2, render_from_csv, mark_posted

load_dotenv(dotenv_path=".env")

ACCOUNT_ID      = os.getenv("ACCOUNT_ID")
ACCESS_KEY      = os.getenv("ACCESS_KEY")
SECRET_KEY      = os.getenv("SECRET_KEY")
BUCKET_NAME     = os.getenv("BUCKET_NAME")
PUBLIC_URL_BASE = os.getenv("PUBLIC_URL_BASE")

MY_ACCESS_TOKEN = os.getenv("MY_ACCESS_TOKEN")

PAGE_ID      = "1025441090651921"
MASTER_CSV   = "docs/master_games.csv"
CAROUSEL_MAX = 10   # Instagram hard limit per carousel post
BATCH_PAUSE  = 5    # seconds between consecutive carousel posts


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def chunked(lst, size):
    """Yield successive chunks of `size` from lst."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def post_carousel(ig_user_id, creation_ids, caption, access_token):
    """
    Assemble and publish a carousel from already-uploaded child container IDs.
    Returns the permalink string, or None if polling timed out.
    """
    # Create carousel container
    r = requests.post(
        f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
        data={
            "media_type":   "CAROUSEL",
            "children":     ",".join(creation_ids),
            "caption":      caption,
            "access_token": access_token,
        },
    )
    r.raise_for_status()
    carousel_id = r.json().get("id")
    if not carousel_id:
        raise ValueError("Failed to create carousel container.")
    print(f"  Carousel container ID: {carousel_id}")

    # Publish
    pub = requests.post(
        f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
        data={"creation_id": carousel_id, "access_token": access_token},
    )
    pub.raise_for_status()
    print(f"  Publish sent. Temp ID: {pub.json().get('id')}")

    # Poll for permalink (up to ~60 s)
    for attempt in range(15):
        resp = requests.get(
            f"https://graph.facebook.com/v19.0/{carousel_id}",
            params={"fields": "id,permalink,media_type", "access_token": access_token},
        ).json()
        permalink = resp.get("permalink")
        if permalink:
            print(f"  Live: {permalink}")
            return permalink
        print(f"  Polling {attempt + 1}/15…")
        time.sleep(4)

    print("  Warning: permalink not available yet.")
    return None


def main():
    client = create_s3_client()

    # ── Resolve Instagram Business Account ID ─────────────────────────────────
    page_resp = requests.get(
        f"https://graph.facebook.com/v19.0/{PAGE_ID}",
        params={"fields": "instagram_business_account", "access_token": MY_ACCESS_TOKEN},
    )
    page_resp.raise_for_status()
    page_data = page_resp.json()

    if "instagram_business_account" not in page_data:
        raise ValueError("Page does not have an Instagram Business account linked.")

    ig_user_id = page_data["instagram_business_account"]["id"]
    print(f"Instagram Business ID: {ig_user_id}")

    # ── Render today's unposted final games ───────────────────────────────────
    # today_only=True ensures we never post games from other dates
    rendered = render_from_csv(MASTER_CSV, today_only=True)

    if not rendered:
        print("No today's final unposted games to post. Exiting.")
        return

    print(f"\n{len(rendered)} game(s) ready to post.")

    # ── Post in batches of CAROUSEL_MAX ───────────────────────────────────────
    batches     = list(chunked(rendered, CAROUSEL_MAX))
    total_batches = len(batches)
    total_posted  = 0

    eastern    = pytz.timezone("US/Eastern")
    date_label = datetime.datetime.now(eastern).strftime("%B %-d, %Y")

    for batch_num, batch in enumerate(batches, start=1):
        game_ids = [r[0] for r in batch]
        urls     = [r[1] for r in batch]

        print(f"\n── Batch {batch_num}/{total_batches}: {len(urls)} image(s) ──")

        # Upload each image as a carousel child container
        creation_ids = []
        for url in urls:
            resp = requests.post(
                f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
                data={
                    "image_url":        url,
                    "is_carousel_item": "true",
                    "access_token":     MY_ACCESS_TOKEN,
                },
            )
            resp.raise_for_status()
            cid = resp.json().get("id")
            if not cid:
                raise ValueError(f"No container ID returned for: {url}")
            creation_ids.append(cid)

        print(f"  Uploaded {len(creation_ids)} child container(s).")

        # Build caption
        caption = f"CIAC Basketball Scores — {date_label}"
        if total_batches > 1:
            caption += f" (Part {batch_num} of {total_batches})"

        post_carousel(ig_user_id, creation_ids, caption, MY_ACCESS_TOKEN)

        # Mark these games as posted before cleaning up images
        for game_id in game_ids:
            mark_posted(MASTER_CSV, game_id)

        # Clean up R2 images for this batch
        for url in urls:
            file_name = url.split("/")[-1]
            delete_from_r2(client, file_name)

        total_posted += len(game_ids)
        print(f"  ✓ {len(game_ids)} game(s) marked posted and images cleaned up.")

        # Pause between posts to respect rate limits
        if batch_num < total_batches:
            print(f"  Pausing {BATCH_PAUSE}s before next batch…")
            time.sleep(BATCH_PAUSE)

    print(f"\nDone. {total_posted} game(s) posted across {total_batches} carousel(s).")


if __name__ == "__main__":
    main()