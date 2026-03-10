"""
scrapeCIAC.py
Scrapes CIAC boys & girls basketball schedules and scores.

Usage:
    python scrapeCIAC.py              # today's games (default)
    python scrapeCIAC.py --week       # this week's games
    python scrapeCIAC.py --all        # entire season
"""

import argparse
import csv
import re
import sys
import uuid
from datetime import datetime

import requests
from bs4 import BeautifulSoup

MASTER_CSV = "docs/master_games.csv"

SPORTS = [
    {
        "base_url": "https://ciac.fpsports.org/DashboardSchedule.aspx",
        "params":   {"L": "1", "SportID": "2_1015_5"},  # Boys Basketball Varsity
        "header":   "CIAC Boys Basketball",
    },
    {
        "base_url": "https://ciac.fpsports.org/DashboardSchedule.aspx",
        "params":   {"L": "1", "SportID": "3_1015_5"},  # Girls Basketball Varsity
        "header":   "CIAC Girls Basketball",
    },
]

MASTER_FIELDS = [
    "game_id", "header", "home_team", "away_team", "home_rank", "away_rank",
    "home_score", "away_score", "home_record", "away_record",
    "bg_image", "photo_cred", "game_datetime", "status",
    "posted_to_instagram", "caption",
]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_master():
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def save_master(rows):
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in MASTER_FIELDS})


# ── Utilities ─────────────────────────────────────────────────────────────────

def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def make_game_key(home, away, dt):
    date = dt.split(" ")[0] if dt else ""
    return f"{normalize(home)}|{normalize(away)}|{date}"


def parse_datetime(date_str, time_str):
    """
    date_str: "SAT 3/7"   time_str: "1:00 PM"
    Returns:  "03/07/2026 13:00:00"
    """
    md = re.search(r"(\d{1,2})/(\d{1,2})", date_str)
    tm = re.match(r"(\d{1,2}):(\d{2})\s*([APap][Mm])", time_str.strip())
    if not md or not tm:
        return ""
    month, day = int(md.group(1)), int(md.group(2))
    hour, minute = int(tm.group(1)), int(tm.group(2))
    ap = tm.group(3).upper()
    if ap == "PM" and hour != 12:
        hour += 12
    elif ap == "AM" and hour == 12:
        hour = 0
    year = datetime.now().year
    try:
        return datetime(year, month, day, hour, minute).strftime("%m/%d/%Y %H:%M:%S")
    except ValueError:
        return ""


def parse_team_div(div):
    """
    Extract (team_name, score) from a <div class="team"> element.

    Real HTML structure:
      <div class="team">
        Avon - IV
        <i class="fa fa-solid fa-house"></i>
        <div class="scoreright"><b>52</b></div>   ← winner score (bold)
      </div>
      <div class="team">
        Stafford - V
        <div class="scoreright">50</div>           ← loser score (plain)
      </div>

    Scheduled games have no <div class="scoreright"> at all.
    """
    # 1. Extract score from <div class="scoreright"> before removing anything
    scoreright = div.find("div", class_="scoreright")
    score = scoreright.get_text(strip=True) if scoreright else ""

    # 2. Remove scoreright div and house icon so they don't pollute the name
    for tag in div.find_all(["i", "div"]):
        tag.decompose()

    # 3. Remaining text is "TeamName - Division" e.g. "Avon - IV"
    text = div.get_text(strip=True)

    # 4. Strip the division suffix (- I, - II, - III, - IV, - V, etc.)
    name = re.sub(r"\s*-\s*[IVX]+\s*$", "", text).strip()

    return name, score


# ── Row parser ────────────────────────────────────────────────────────────────

def parse_row(row):
    """Parse one <tr> into a game dict, or return None to skip."""
    cells = row.find_all("td")
    if not cells:
        return None

    # Skip scrimmages
    gametype_cell = row.find("td", class_="gametype")
    if gametype_cell and "scrimmage" in gametype_cell.get_text(strip=True).lower():
        return None

    # Date + time from cell[0]
    date_span = cells[0].find("span", class_="date")
    time_span = cells[0].find("span", class_="time")
    if not date_span or not time_span:
        return None

    dt_str = parse_datetime(date_span.get_text(strip=True), time_span.get_text(strip=True))
    if not dt_str:
        return None

    # Game link
    a = None
    for cell in cells:
        a = cell.find("a", href=re.compile(r"/dashboardgame\.aspx", re.I))
        if a:
            break
    if not a:
        return None

    # Teams are in <div class="teams"> > <div class="team">
    team_divs = a.find_all("div", class_="team")
    if len(team_divs) < 2:
        return None

    # Home team has the house icon — detect before decomposing anything
    home_idx = 0
    for i, div in enumerate(team_divs):
        if div.find("i", class_=re.compile(r"fa-house")):
            home_idx = i
            break
    away_idx = 1 if home_idx == 0 else 0

    home_name, home_score = parse_team_div(team_divs[home_idx])
    away_name, away_score = parse_team_div(team_divs[away_idx])

    if not home_name or not away_name:
        return None
    if home_name.lower() in ("tbd", "tba") or away_name.lower() in ("tbd", "tba"):
        return None

    has_score = bool(home_score and away_score)

    return {
        "home_team":     home_name,
        "home_score":    home_score,
        "away_team":     away_name,
        "away_score":    away_score,
        "game_datetime": dt_str,
        "status":        "final" if has_score else "scheduled",
    }


# ── Scraper ───────────────────────────────────────────────────────────────────

def scrape_sport(base_url, params, header, quick_filter):
    fetch_params = {**params, "QuickFilter": quick_filter}
    print(f"Fetching {header} (QuickFilter={quick_filter})…", flush=True)

    try:
        resp = requests.get(
            base_url, params=fetch_params,
            headers=REQUEST_HEADERS, timeout=20,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        print(f"  [WARN] No table found", file=sys.stderr)
        return []

    rows = table.find_all("tr")
    print(f"  Found {len(rows)} rows", flush=True)

    games = []
    for row in rows:
        result = parse_row(row)
        if result:
            result["header"] = header
            games.append(result)

    print(f"  Parsed {len(games)} games", flush=True)
    return games


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all",  action="store_true", help="Entire season")
    group.add_argument("--week", action="store_true", help="This week")
    args = parser.parse_args()

    quick_filter = "3" if args.all else "2" if args.week else "1"

    existing = load_master()
    existing_by_key = {}
    for row in existing:
        key = make_game_key(row["home_team"], row["away_team"], row["game_datetime"])
        existing_by_key[key] = row

    new_count = updated_count = 0

    for sport in SPORTS:
        for g in scrape_sport(sport["base_url"], sport["params"], sport["header"], quick_filter):
            key = make_game_key(g["home_team"], g["away_team"], g["game_datetime"])

            if key in existing_by_key:
                row = existing_by_key[key]
                if g["status"] == "final" and row.get("status") != "final":
                    row["home_score"] = g["home_score"]
                    row["away_score"] = g["away_score"]
                    row["status"]     = "final"
                    updated_count += 1
            else:
                existing_by_key[key] = {
                    "game_id":             "ciac_" + str(uuid.uuid4())[:8],
                    "header":              g["header"],
                    "home_team":           g["home_team"],
                    "away_team":           g["away_team"],
                    "home_rank":           "NR",
                    "away_rank":           "NR",
                    "home_score":          g["home_score"],
                    "away_score":          g["away_score"],
                    "home_record":         "",
                    "away_record":         "",
                    "bg_image":            "",
                    "photo_cred":          "",
                    "game_datetime":       g["game_datetime"],
                    "status":              g["status"],
                    "posted_to_instagram": "FALSE",
                    "caption":             "",
                }
                new_count += 1

    save_master(list(existing_by_key.values()))
    print(f"\nDone — {new_count} new, {updated_count} updated. Total: {len(existing_by_key)}")


if __name__ == "__main__":
    main()
