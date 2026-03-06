"""
scrapeCIAC.py
Scrapes the CIAC master schedule for Boys and Girls Basketball (current season),
then merges new games into docs/master_games.csv without overwriting existing entries.

Run manually or add to GitHub Actions workflow.
"""

import csv
import re
import uuid
from datetime import datetime

import requests
from bs4 import BeautifulSoup

MASTER_CSV = "docs/master_games.csv"
CURRENT_SEASON = "2526"  # Update each season: 2526 = 2025-26

SPORTS = [
    {
        "url": f"http://casciac.org/scripts/schedulelm{CURRENT_SEASON}.cgi?sport=boys-basketball&levels=V",
        "header": "CIAC Boys Basketball",
        "sport_key": "boys-basketball",
    },
    {
        "url": f"http://casciac.org/scripts/schedulelm{CURRENT_SEASON}.cgi?sport=girls-basketball&levels=V",
        "header": "CIAC Girls Basketball",
        "sport_key": "girls-basketball",
    },
]

MASTER_FIELDS = [
    "game_id", "header", "home_team", "away_team", "home_rank", "away_rank",
    "home_score", "away_score", "home_record", "away_record",
    "bg_image", "photo_cred", "game_datetime", "status",
    "posted_to_instagram", "caption"
]


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def load_master():
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        return []


def save_master(rows):
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def make_game_key(home, away, dt_str):
    """Dedup key: normalize names + date only."""
    date_part = dt_str.split(" ")[0] if dt_str else ""
    return f"{normalize(home)}|{normalize(away)}|{date_part}"


def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def parse_ciac_date(date_str, time_str, year):
    """
    Convert CIAC's 'Mon., 3/6' + '7:00 p.m.' into 'MM/DD/YYYY HH:MM:SS'.
    Falls back to 19:00 if time is missing.
    """
    try:
        # Strip day-of-week: "Mon., 3/6" -> "3/6"
        date_clean = re.sub(r"^[A-Za-z]+\.,\s*", "", date_str).strip()
        month, day = date_clean.split("/")

        # Parse time
        time_clean = time_str.strip().lower().replace(".", "").replace(" ", "")
        dt = datetime.strptime(f"{month}/{day}/{year} {time_clean}", "%m/%d/%Y %I:%M%p")
        return dt.strftime("%m/%d/%Y %H:%M:%S")
    except Exception:
        try:
            date_clean = re.sub(r"^[A-Za-z]+\.,\s*", "", date_str).strip()
            month, day = date_clean.split("/")
            return f"{int(month):02d}/{int(day):02d}/{year} 19:00:00"
        except Exception:
            return ""


def scrape_sport(url, header, year):
    """Scrape a CIAC master schedule page. Returns list of game dicts."""
    games = []
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Could not fetch {url}: {e}")
        return games

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        print(f"[WARN] No table found at {url}")
        return games

    rows = table.find_all("tr")
    current_date = ""

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells:
            continue

        # Date column (first cell contains day like "Mon., 3/6")
        if len(cells) >= 5 and re.search(r"\d+/\d+", cells[0]):
            current_date = cells[0]
            sport_col = cells[1] if len(cells) > 1 else ""
            teams_col = cells[2] if len(cells) > 2 else ""
            time_col = cells[4] if len(cells) > 4 else ""

            # Filter out scrimmages and non-varsity
            if "Scrim" in teams_col or "JV" in sport_col:
                continue
            if "Basketball" not in sport_col:
                continue

            # Parse teams: "Away @ Home" or "Away vs. Home"
            home_team, away_team = "", ""
            if "@" in teams_col:
                parts = teams_col.split("@")
                away_team = parts[0].strip()
                # Facility sometimes appended after team name in parens — strip it
                home_team = re.sub(r"\s*\(.*?\)", "", parts[1]).strip()
            elif " vs." in teams_col.lower():
                parts = re.split(r"\s+vs\.?\s+", teams_col, flags=re.IGNORECASE)
                home_team = parts[0].strip()
                away_team = parts[1].strip() if len(parts) > 1 else ""
            else:
                continue

            if not home_team or not away_team:
                continue

            dt_str = parse_ciac_date(current_date, time_col, year)

            games.append({
                "home_team": home_team,
                "away_team": away_team,
                "game_datetime": dt_str,
                "header": header,
            })

    print(f"  Scraped {len(games)} games from {url}")
    return games


def determine_season_year():
    """Returns the calendar year for the latter half of the season (Jan-Jun)."""
    now = datetime.now()
    # Basketball season spans Dec-Mar; games in Jan-Mar are in the next calendar year
    if now.month >= 7:
        return now.year + 1  # e.g. in Nov 2025 -> 2026
    return now.year  # e.g. in Jan 2026 -> 2026


# -------------------------------------------------------
# Main
# -------------------------------------------------------
def main():
    existing = load_master()
    existing_keys = {
        make_game_key(r["home_team"], r["away_team"], r["game_datetime"])
        for r in existing
        if r.get("home_team") and r.get("away_team")
    }

    year = determine_season_year()
    new_games = []

    for sport in SPORTS:
        print(f"Scraping: {sport['header']}")
        scraped = scrape_sport(sport["url"], sport["header"], year)
        for g in scraped:
            key = make_game_key(g["home_team"], g["away_team"], g["game_datetime"])
            if key not in existing_keys:
                game_id = "ciac_" + str(uuid.uuid4())[:8]
                new_games.append({
                    "game_id": game_id,
                    "header": g["header"],
                    "home_team": g["home_team"],
                    "away_team": g["away_team"],
                    "home_rank": "NR",
                    "away_rank": "NR",
                    "home_score": "",
                    "away_score": "",
                    "home_record": "",
                    "away_record": "",
                    "bg_image": "",
                    "photo_cred": "",
                    "game_datetime": g["game_datetime"],
                    "status": "scheduled",
                    "posted_to_instagram": "false",
                    "caption": "",
                })
                existing_keys.add(key)

    if new_games:
        all_rows = existing + new_games
        save_master(all_rows)
        print(f"\nAdded {len(new_games)} new game(s) to {MASTER_CSV}.")
    else:
        print("\nNo new games found — master_games.csv is up to date.")


if __name__ == "__main__":
    main()
