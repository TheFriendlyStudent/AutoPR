"""
scrapeCIAC.py
Scrapes today's CIAC boys & girls basketball scores from the public schedule page
using requests + BeautifulSoup (no Selenium, no browser, no infinite loops).

Usage:
    python scrapeCIAC.py              # scrapes today's games (QuickFilter=1)
    python scrapeCIAC.py --all        # scrapes all games this season (QuickFilter=3)
    python scrapeCIAC.py --week       # scrapes this week's games (QuickFilter=2)
"""

import argparse
import csv
import re
import sys
import uuid
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

MASTER_CSV = "docs/master_games.csv"

SPORTS = [
    {
        "base_url": "https://ciac.fpsports.org/DashboardSchedule.aspx?L=1&SportID=2_1015_-1&QuickFilter=3",
        "params": {"L": "3", "SportID": "2_1015_-1"},   # Boys Basketball
        "header": "CIAC Boys Basketball",
    }
]

MASTER_FIELDS = [
    "game_id", "header", "home_team", "away_team", "home_rank", "away_rank",
    "home_score", "away_score", "home_record", "away_record",
    "bg_image", "photo_cred", "game_datetime", "status",
    "posted_to_instagram", "caption",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_master() -> list[dict]:
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def save_master(rows: list[dict]) -> None:
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in MASTER_FIELDS})


# ── Utilities ─────────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def clean_team(name: str) -> str:
    """Strip level suffix like '- III' or '- V' from team names."""
    return re.sub(r"\s*-\s*[IVX]+\s*$", "", name).strip()


def make_game_key(home: str, away: str, dt: str) -> str:
    date = dt.split(" ")[0] if dt else ""
    return f"{normalize(home)}|{normalize(away)}|{date}"


def parse_game_datetime(raw_date: str, raw_time: str) -> str:
    """
    raw_date examples: 'SAT 3/7', 'FRI 3/6'
    raw_time examples: '7:00 PM', '6:30 PM'
    Returns: 'MM/DD/YYYY HH:MM:SS'
    """
    raw_date = raw_date.strip()
    raw_time = raw_time.strip().upper()

    # Extract just the M/D part (ignore day-of-week prefix)
    md_match = re.search(r"(\d{1,2})/(\d{1,2})", raw_date)
    if not md_match:
        return ""

    month, day = int(md_match.group(1)), int(md_match.group(2))
    year = datetime.now().year

    # Time parse: '7:00 PM' → 19:00
    time_match = re.match(r"(\d{1,2}):(\d{2})\s*(AM|PM)", raw_time)
    if not time_match:
        # Try bare '19:00' (24-hour)
        time_match_24 = re.match(r"(\d{1,2}):(\d{2})", raw_time)
        if time_match_24:
            hour, minute = int(time_match_24.group(1)), int(time_match_24.group(2))
        else:
            hour, minute = 0, 0
    else:
        hour, minute = int(time_match.group(1)), int(time_match.group(2))
        if time_match.group(3) == "PM" and hour != 12:
            hour += 12
        elif time_match.group(3) == "AM" and hour == 12:
            hour = 0

    try:
        dt = datetime(year, month, day, hour, minute)
        return dt.strftime("%m/%d/%Y %H:%M:%S")
    except ValueError:
        return ""


# ── Scraper ───────────────────────────────────────────────────────────────────

def scrape_sport(base_url: str, params: dict, header: str, quick_filter: str) -> list[dict]:
    """
    Fetches the CIAC schedule page and parses game rows from the HTML table.
    Returns a list of game dicts.
    """
    fetch_params = {**params, "QuickFilter": quick_filter}

    print(f"Fetching {header} (QuickFilter={quick_filter})…", flush=True)

    try:
        resp = requests.get(base_url, params=fetch_params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] Request failed: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    games = []

    # The schedule is rendered as a <table> with rows containing game data.
    # Each game row has an anchor tag whose href is /dashboardgame.aspx?…
    # The row structure is:
    #   TD: date+time text  |  TD: (empty/icon)  |  TD: game link  |  TD: (empty)  |  TD: location
    #
    # The game link text looks like:
    #   "Boys Basketball Varsity\n  TeamA - III\n  57\n  TeamB - V 51"
    # or for scheduled games:
    #   "Boys Basketball Varsity\n  TeamA - V\n  TeamB - III"

    table = soup.find("table")
    if not table:
        print(f"  [WARN] No table found on page for {header}", file=sys.stderr)
        return []

    rows = table.find_all("tr")
    print(f"  Found {len(rows)} table rows", flush=True)

    current_date = ""
    current_time = ""

    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        # ── Date / time cell ──────────────────────────────────────────────────
        # The first cell often contains "SAT 3/7\n6:00 PM" (date on first line, time on second)
        # But some rows share a date from a previous row (rowspan), so we track current_date/time.
        first_cell_text = cells[0].get_text(separator="\n").strip() if cells else ""
        date_time_match = re.search(
            r"((?:MON|TUE|WED|THU|FRI|SAT|SUN)\s+\d{1,2}/\d{1,2})\s*\n?\s*(\d{1,2}:\d{2}\s*[APap][Mm])",
            first_cell_text,
        )
        if date_time_match:
            current_date = date_time_match.group(1).strip()
            current_time = date_time_match.group(2).strip()

        # ── Game link cell ────────────────────────────────────────────────────
        # Find a cell with an anchor to /dashboardgame.aspx
        game_link = None
        for cell in cells:
            a = cell.find("a", href=re.compile(r"/dashboardgame\.aspx", re.I))
            if a:
                game_link = a
                break

        if not game_link:
            continue

        link_text = game_link.get_text(separator="\n").strip()
        lines = [l.strip() for l in link_text.splitlines() if l.strip()]

        # Remove the sport/level header line (e.g. "Boys Basketball Varsity")
        lines = [l for l in lines if not re.match(r"(Boys|Girls)\s+Basketball", l, re.I)]

        if len(lines) < 2:
            continue

        # Each line is either:  "TeamName - Level  Score"  or just  "TeamName - Level"
        team_score_re = re.compile(r"^(.+?)\s*-\s*[IVX]+\s*(\d+)?$")

        parsed_teams = []
        for line in lines[:2]:
            m = team_score_re.match(line)
            if m:
                team = clean_team(m.group(1))
                score = m.group(2) or ""
                parsed_teams.append((team, score))
            else:
                # No level suffix — try to strip a trailing score
                score_at_end = re.search(r"\s+(\d+)$", line)
                if score_at_end:
                    team = clean_team(line[:score_at_end.start()])
                    score = score_at_end.group(1)
                else:
                    team = clean_team(line)
                    score = ""
                parsed_teams.append((team, score))

        if len(parsed_teams) < 2:
            continue

        team1_name, team1_score = parsed_teams[0]
        team2_name, team2_score = parsed_teams[1]

        # Skip JV / scrimmage
        lower_text = link_text.lower()
        if "junior varsity" in lower_text or " jv" in lower_text or "scrimmage" in lower_text:
            continue

        # Determine home vs away.
        # CIAC lists home team first in most cases; there's no reliable house icon
        # in the plain-text parse, so we treat the first team as home.
        home_team, home_score = team1_name, team1_score
        away_team, away_score = team2_name, team2_score

        # Status
        has_score = bool(home_score and away_score)
        status = "final" if has_score else "scheduled"

        dt_str = parse_game_datetime(current_date, current_time)

        if not home_team or not away_team:
            continue

        games.append({
            "home_team": home_team,
            "away_team": away_team,
            "game_datetime": dt_str,
            "header": header,
            "home_score": home_score,
            "away_score": away_score,
            "status": status,
        })

    print(f"  Parsed {len(games)} games for {header}", flush=True)
    return games


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scrape CIAC basketball schedules/scores")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all",  action="store_true", help="Scrape all season games")
    group.add_argument("--week", action="store_true", help="Scrape this week's games")
    args = parser.parse_args()

    if args.all:
        quick_filter = "3"
    elif args.week:
        quick_filter = "2"
    else:
        quick_filter = "1"   # today only (default)

    existing = load_master()

    # Build a lookup by game_key for fast dedup / update
    existing_by_key: dict[str, dict] = {}
    for row in existing:
        key = make_game_key(row["home_team"], row["away_team"], row["game_datetime"])
        existing_by_key[key] = row

    new_count = 0
    updated_count = 0
    all_scraped: list[dict] = []

    for sport in SPORTS:
        scraped = scrape_sport(
            sport["base_url"], sport["params"], sport["header"], quick_filter
        )
        all_scraped.extend(scraped)

    for g in all_scraped:
        key = make_game_key(g["home_team"], g["away_team"], g["game_datetime"])

        if key in existing_by_key:
            row = existing_by_key[key]
            # Only update score if we now have one and didn't before
            if g["status"] == "final" and row.get("status") != "final":
                row["home_score"] = g["home_score"]
                row["away_score"] = g["away_score"]
                row["status"] = "final"
                updated_count += 1
        else:
            game_id = "ciac_" + str(uuid.uuid4())[:8]
            new_row = {
                "game_id": game_id,
                "header": g["header"],
                "home_team": g["home_team"],
                "away_team": g["away_team"],
                "home_rank": "NR",
                "away_rank": "NR",
                "home_score": g["home_score"],
                "away_score": g["away_score"],
                "home_record": "",
                "away_record": "",
                "bg_image": "",
                "photo_cred": "",
                "game_datetime": g["game_datetime"],
                "status": g["status"],
                "posted_to_instagram": "FALSE",
                "caption": "",
            }
            existing_by_key[key] = new_row
            new_count += 1

    final_rows = list(existing_by_key.values())
    save_master(final_rows)

    print(f"\nDone — {new_count} new game(s) added, {updated_count} score(s) updated.")
    print(f"Total rows in {MASTER_CSV}: {len(final_rows)}")


if __name__ == "__main__":
    main()