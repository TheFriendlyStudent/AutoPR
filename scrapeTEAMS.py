"""
scrapeTEAMS.py
Scrapes CIAC boys & girls basketball schedules and scores.
Records are calculated from scraped game data, then cross-checked against
the official CIAC rankings page so that any games missing from the local CSV
are reflected in the final record shown on each game card.

Usage:
    python scrapeTEAMS.py              # today's games (default)
    python scrapeTEAMS.py --week       # this week's games
    python scrapeTEAMS.py --all        # entire season (per-school, bypasses 200-row cap)
"""

import argparse
import csv
import re
import sys
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bs4 import BeautifulSoup

MASTER_CSV = "docs/master_games.csv"
MAX_WORKERS = 15
from schools import SCHOOLS, normalize

SPORTS = [
    ("CIAC Boys Basketball", "2_1015_5"),
    ("CIAC Girls Basketball", "3_1015_5"),
]

RANKINGS_URLS = {
    "CIAC Boys Basketball":  "https://content.ciacsports.com/scripts/bbb_rankings2.cgi",
    "CIAC Girls Basketball": "https://content.ciacsports.com/scripts/gbball_rankings2.cgi",
}

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

BASE_URL = "https://ciac.fpsports.org/DashboardSchedule.aspx"


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

def make_game_key(home, away, dt):
    date = dt.split(" ")[0] if dt else ""
    return f"{normalize(home)}|{normalize(away)}|{date}"


def parse_dt_obj(dt_str):
    """Parse 'MM/DD/YYYY HH:MM:SS' into a datetime object for sorting."""
    try:
        return datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
    except ValueError:
        return datetime.max


def game_sort_key(r):
    """Stable sort key shared by calculate_records and the final CSV sort."""
    return (
        parse_dt_obj(r.get("game_datetime", "")),
        r.get("header", ""),
        r.get("home_team", ""),
        r.get("game_id", ""),
    )


def parse_datetime(date_str, time_str):
    """
    Convert CIAC date/time strings to 'MM/DD/YYYY HH:MM:SS'.
    Basketball season spans two calendar years (e.g. 2025-2026):
      Nov-Dec  -> earlier calendar year  (e.g. 2025)
      Jan-Apr  -> later calendar year    (e.g. 2026)
    """
    md = re.search(r"(\d{1,2})/(\d{1,2})", date_str)
    tm = re.match(r"(\d{1,2}):(\d{2})\s*([APap][Mm])", time_str.strip())
    if not md or not tm:
        return ""

    month, day   = int(md.group(1)), int(md.group(2))
    hour, minute = int(tm.group(1)), int(tm.group(2))
    ap = tm.group(3).upper()
    if ap == "PM" and hour != 12:
        hour += 12
    elif ap == "AM" and hour == 12:
        hour = 0

    today = datetime.now()
    year = (today.year - 1) if (month >= 11 and today.month < 7) else today.year

    try:
        return datetime(year, month, day, hour, minute).strftime("%m/%d/%Y %H:%M:%S")
    except ValueError:
        return ""


def parse_team_div(div):
    """Extract (team_name, score) from a <div class="team"> element."""
    scoreright = div.find("div", class_="scoreright")
    score = scoreright.get_text(strip=True) if scoreright else ""

    for tag in div.find_all(["i", "div"]):
        tag.decompose()

    text = div.get_text(strip=True)
    name = re.sub(r"\s*-\s*[IVX]+\s*$", "", text).strip()
    return name, score


# ── Row parser ────────────────────────────────────────────────────────────────

def parse_row(row):
    """Parse one <tr> into a game dict, or return None to skip."""
    cells = row.find_all("td")
    if not cells:
        return None

    gt_cell = row.find("td", class_="gametype")
    if gt_cell and "scrimmage" in gt_cell.get_text(strip=True).lower():
        return None

    date_span = cells[0].find("span", class_="date")
    time_span = cells[0].find("span", class_="time")
    if not date_span or not time_span:
        return None

    dt_str = parse_datetime(date_span.get_text(strip=True), time_span.get_text(strip=True))
    if not dt_str:
        return None

    a = None
    for cell in cells:
        a = cell.find("a", href=re.compile(r"/dashboardgame\.aspx", re.I))
        if a:
            break
    if not a:
        return None

    team_divs = a.find_all("div", class_="team")

    def is_real_team(div):
        text = div.get_text(separator=" ", strip=True)
        return bool(re.search(r"-\s*[IVX]+", text))

    team_divs = [d for d in team_divs if is_real_team(d)]
    if len(team_divs) < 2:
        return None

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

    return {
        "home_team":     home_name,
        "home_score":    home_score,
        "away_team":     away_name,
        "away_score":    away_score,
        "game_datetime": dt_str,
        "status":        "final" if (home_score and away_score) else "scheduled",
    }


# ── Fetchers ──────────────────────────────────────────────────────────────────

def fetch_page(sport_id, quick_filter, school_id=None):
    """Fetch one CIAC schedule page and return a list of parsed game dicts."""
    params = {"L": "1", "SportID": sport_id, "QuickFilter": quick_filter}
    if school_id is not None:
        params["SchoolID"] = school_id
    try:
        resp = requests.get(BASE_URL, params=params, headers=REQUEST_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    [ERROR] {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    return [g for row in table.find_all("tr") if (g := parse_row(row))]


def fetch_all_schools(sport_id):
    """Scrape every school individually (bypasses 200-row cap) using a thread pool."""
    all_games = []
    seen_keys = set()
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_page, sport_id, "3", school_id): school_name
            for school_name, school_id in SCHOOLS
        }
        for future in as_completed(futures):
            completed += 1
            try:
                for g in future.result():
                    key = make_game_key(g["home_team"], g["away_team"], g["game_datetime"])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_games.append(g)
            except Exception as e:
                print(f"  [WARN] {futures[future]}: {e}", file=sys.stderr)
            if completed % 20 == 0:
                print(f"  {completed}/{len(SCHOOLS)} schools done, {len(all_games)} unique games…")

    return all_games


def fetch_today(sport_id):
    """
    Fetch today's games — both scheduled and completed.
    QuickFilter=2 (week) has scheduled games; QuickFilter=1 (today) has scores.
    We merge both and upgrade scheduled -> final when scores arrive.
    """
    today_date = datetime.now().strftime("%m/%d/%Y")
    seen = {}

    for qf in ("2", "1"):
        for g in fetch_page(sport_id, qf):
            if not g["game_datetime"].startswith(today_date):
                continue
            key = make_game_key(g["home_team"], g["away_team"], g["game_datetime"])
            if key not in seen:
                seen[key] = g
            elif g["status"] == "final":
                seen[key].update(g)

    return list(seen.values())


# ── CIAC rankings scraper ─────────────────────────────────────────────────────

def scrape_ciac_records(header):
    """
    Scrape the CIAC unofficial rankings page for the given sport header.
    Returns dict: normalized_name -> {"wins": int, "losses": int, "full_name": str}
    """
    url = RANKINGS_URLS.get(header)
    if not url:
        return {}
    try:
        resp = requests.get(url, timeout=15, headers=REQUEST_HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] Could not scrape CIAC rankings for {header}: {e}", file=sys.stderr)
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    records = {}
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        link = cells[0].find("a")
        if not link:
            continue
        name = link.get_text(strip=True)
        try:
            wins   = int(cells[2].get_text(strip=True))
            losses = int(cells[3].get_text(strip=True))
        except (ValueError, IndexError):
            continue
        records[normalize(name)] = {"wins": wins, "losses": losses, "full_name": name}

    print(f"  CIAC rankings scraped for {header}: {len(records)} teams")
    return records


def find_ciac_record(name, ciac_map):
    """
    Look up a team name in a CIAC rankings map.
    Tries exact normalized match first, then partial containment.
    Returns (wins, losses) or None.
    """
    norm = normalize(name)
    if norm in ciac_map:
        r = ciac_map[norm]
        return r["wins"], r["losses"]
    # Partial match — handles slight name discrepancies
    for key, r in ciac_map.items():
        if norm in key or key in norm:
            return r["wins"], r["losses"]
    return None


# ── Records calculator ────────────────────────────────────────────────────────

def calculate_records(rows, ciac_maps):
    """
    Walk all final non-scrimmage games in chronological order and write each
    team's cumulative W-L record back into that game row.

    After the local walk, any team whose locally-counted total (wins + losses)
    is LESS than what CIAC reports gets their record in their LAST game corrected
    to the authoritative CIAC figure.  This handles the common case where some
    games are missing from the local CSV because they weren't scraped.

    Boys and girls are tracked independently via the `header` field.
    """
    final_rows = [
        r for r in rows
        if r.get("status") == "final" and r.get("home_score") and r.get("away_score")
    ]
    final_rows.sort(key=game_sort_key)

    wins         = defaultdict(int)
    losses       = defaultdict(int)
    game_records = {}
    last_game    = {}   # (header, team) -> game_id of their most recent final game

    for r in final_rows:
        hdr      = r.get("header", "")
        home_key = (hdr, r["home_team"])
        away_key = (hdr, r["away_team"])
        try:
            hs  = int(r["home_score"])
            as_ = int(r["away_score"])
        except (ValueError, TypeError):
            continue

        if hs > as_:
            wins[home_key]   += 1
            losses[away_key] += 1
        elif as_ > hs:
            wins[away_key]   += 1
            losses[home_key] += 1

        game_records[r["game_id"]] = (
            f"{wins[home_key]}-{losses[home_key]}",
            f"{wins[away_key]}-{losses[away_key]}",
        )

        last_game[home_key] = r["game_id"]
        last_game[away_key] = r["game_id"]

    # Write locally-calculated records into every row
    for r in rows:
        if r.get("game_id") in game_records:
            r["home_record"], r["away_record"] = game_records[r["game_id"]]

    # ── Cross-check against CIAC official records ──────────────────────────
    # For each team, if CIAC reports more games played than we counted locally,
    # patch only their LAST game's record to reflect the true season record.
    # Earlier game rows keep their locally-computed running records intact.
    for (hdr, team), gid in last_game.items():
        ciac_map = ciac_maps.get(hdr, {})
        if not ciac_map:
            continue

        local_w     = wins[(hdr, team)]
        local_l     = losses[(hdr, team)]
        local_total = local_w + local_l

        result = find_ciac_record(team, ciac_map)
        if result is None:
            continue
        ciac_w, ciac_l = result
        ciac_total = ciac_w + ciac_l

        if ciac_total <= local_total:
            continue

        print(
            f"  [RECORD PATCH] {team} ({hdr}): "
            f"local {local_w}-{local_l} → CIAC {ciac_w}-{ciac_l}"
        )
        for r in rows:
            if r.get("game_id") != gid:
                continue
            if normalize(r.get("home_team", "")) == normalize(team):
                r["home_record"] = f"{ciac_w}-{ciac_l}"
            if normalize(r.get("away_team", "")) == normalize(team):
                r["away_record"] = f"{ciac_w}-{ciac_l}"
            break

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--all",  action="store_true", help="Entire season (per-school scrape)")
    group.add_argument("--week", action="store_true", help="This week's games")
    args = parser.parse_args()

    existing = load_master()

    # ── Dedup existing CSV rows ───────────────────────────────────────────────
    # If the same game was entered twice (one with a score, one without),
    # keep the scored/final version and discard the blank one.
    existing_by_key = {}
    dupes_removed   = 0
    for r in existing:
        key = make_game_key(r["home_team"], r["away_team"], r["game_datetime"])
        if key not in existing_by_key:
            existing_by_key[key] = r
        else:
            prev = existing_by_key[key]
            # Prefer whichever row has a final score
            if r.get("status") == "final" and prev.get("status") != "final":
                existing_by_key[key] = r
                dupes_removed += 1
            elif r.get("status") != "final" and prev.get("status") == "final":
                dupes_removed += 1  # discard r, keep prev
            else:
                # Both same status — keep the one with more fields filled in
                r_data    = sum(1 for v in r.values()    if str(v).strip())
                prev_data = sum(1 for v in prev.values() if str(v).strip())
                if r_data > prev_data:
                    existing_by_key[key] = r
                dupes_removed += 1

    if dupes_removed:
        print(f"  Removed {dupes_removed} duplicate row(s) from existing CSV")

    new_count = updated_count = 0

    for header, sport_id in SPORTS:
        if args.all:
            print(f"\n{header} — full season ({len(SCHOOLS)} schools, {MAX_WORKERS} threads)…")
            scraped = fetch_all_schools(sport_id)
        elif args.week:
            print(f"\n{header} — this week…")
            scraped = fetch_page(sport_id, "2")
        else:
            print(f"\n{header} — today…")
            scraped = fetch_today(sport_id)

        print(f"  {len(scraped)} games fetched")

        for g in scraped:
            g["header"] = header
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
                    "header":              header,
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

    all_rows = list(existing_by_key.values())

    # Scrape CIAC rankings once per sport to use as authoritative record reference
    ciac_maps = {header: scrape_ciac_records(header) for header, _ in SPORTS}

    # Records are always recalculated from scraped data, then patched from CIAC
    ciac_rows   = [r for r in all_rows if r.get("game_id", "").startswith("ciac_")]
    manual_rows = [r for r in all_rows if not r.get("game_id", "").startswith("ciac_")]
    ciac_rows   = calculate_records(ciac_rows, ciac_maps)

    all_rows_final = sorted(ciac_rows + manual_rows, key=game_sort_key)

    save_master(all_rows_final)
    print(f"\nDone — {new_count} new, {updated_count} updated. Total: {len(all_rows_final)}")


if __name__ == "__main__":
    main()