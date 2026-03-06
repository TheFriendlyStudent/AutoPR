"""
updateRecords.py
Calculates team win/loss records two ways:
  1. Primary: derive from master_games.csv (games you track)
  2. Fallback: scrape CIAC unofficial rankings page for any team
     whose record can't be determined from local data

Writes docs/team_records.csv with columns:
  team, wins, losses, pct, sport
"""

import csv
import re

import requests
from bs4 import BeautifulSoup

MASTER_CSV = "docs/master_games.csv"
RECORDS_CSV = "docs/team_records.csv"

RANKINGS_URLS = {
    "boys-basketball": "https://content.ciacsports.com/scripts/bbb_rankings2.cgi",
    "girls-basketball": "https://content.ciacsports.com/scripts/gbball_rankings2.cgi",
}


# -------------------------------------------------------
# Step 1: Calculate from master_games.csv
# -------------------------------------------------------
def calc_from_master():
    """Returns dict: team_name -> {'wins': int, 'losses': int, 'sport': str}"""
    records = {}
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status", "").lower() != "final":
                    continue
                hs = row.get("home_score", "").strip()
                as_ = row.get("away_score", "").strip()
                if not hs or not as_:
                    continue
                try:
                    hs, as_ = int(hs), int(as_)
                except ValueError:
                    continue

                home = row["home_team"].strip()
                away = row["away_team"].strip()
                header = row.get("header", "")
                sport = "boys-basketball" if "Boys" in header else "girls-basketball"

                for team in [home, away]:
                    if team not in records:
                        records[team] = {"wins": 0, "losses": 0, "sport": sport}

                if hs > as_:
                    records[home]["wins"] += 1
                    records[away]["losses"] += 1
                elif as_ > hs:
                    records[away]["wins"] += 1
                    records[home]["losses"] += 1
                # ties: no change (rare in basketball)

    except FileNotFoundError:
        pass

    return records


# -------------------------------------------------------
# Step 2: Scrape CIAC rankings for remaining teams
# -------------------------------------------------------
def scrape_ciac_records(sport_key):
    """
    Returns dict: normalized_team_name -> {'wins': int, 'losses': int, 'full_name': str}
    Parsed from the CIAC unofficial rankings table.
    """
    url = RANKINGS_URLS.get(sport_key)
    if not url:
        return {}

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[WARN] Could not scrape {url}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    records = {}

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        school_link = cells[0].find("a")
        if not school_link:
            continue

        school_name = school_link.get_text(strip=True)
        try:
            wins = int(cells[2].get_text(strip=True))
            losses = int(cells[3].get_text(strip=True))
        except (ValueError, IndexError):
            continue

        norm = normalize(school_name)
        records[norm] = {
            "wins": wins,
            "losses": losses,
            "full_name": school_name,
        }

    print(f"  CIAC rankings: {len(records)} teams scraped for {sport_key}")
    return records


def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


# -------------------------------------------------------
# Step 3: Merge and write records CSV
# -------------------------------------------------------
def main():
    local_records = calc_from_master()
    print(f"Local records calculated for {len(local_records)} teams.")

    # Determine which teams need CIAC fallback (< 2 games tracked locally)
    local_game_counts = {}
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status", "").lower() != "final":
                    continue
                for t in [row.get("home_team", ""), row.get("away_team", "")]:
                    t = t.strip()
                    if t:
                        local_game_counts[t] = local_game_counts.get(t, 0) + 1
    except FileNotFoundError:
        pass

    sparse_teams = {t for t, c in local_game_counts.items() if c < 3}

    # Scrape CIAC for fallback data
    ciac_bbb = scrape_ciac_records("boys-basketball")
    ciac_gbb = scrape_ciac_records("girls-basketball")

    def get_ciac_record(team_name, sport):
        ciac = ciac_bbb if sport == "boys-basketball" else ciac_gbb
        norm = normalize(team_name)
        # Try exact normalized match first
        if norm in ciac:
            return ciac[norm]
        # Try partial match (handles abbreviations like "Brist.East." -> "Bristol Eastern")
        for key, val in ciac.items():
            if norm in key or key in norm:
                return val
        return None

    # Build final merged records
    final = {}
    for team, data in local_records.items():
        sport = data["sport"]
        if team in sparse_teams:
            ciac_data = get_ciac_record(team, sport)
            if ciac_data:
                final[team] = {
                    "team": team,
                    "wins": ciac_data["wins"],
                    "losses": ciac_data["losses"],
                    "pct": round(ciac_data["wins"] / max(ciac_data["wins"] + ciac_data["losses"], 1), 3),
                    "sport": sport,
                    "source": "ciac"
                }
                continue
        final[team] = {
            "team": team,
            "wins": data["wins"],
            "losses": data["losses"],
            "pct": round(data["wins"] / max(data["wins"] + data["losses"], 1), 3),
            "sport": sport,
            "source": "local"
        }

    # Add any CIAC-only teams not in local data
    for norm, ciac_data in {**ciac_bbb}.items():
        team_name = ciac_data["full_name"]
        if team_name not in final:
            final[team_name] = {
                "team": team_name,
                "wins": ciac_data["wins"],
                "losses": ciac_data["losses"],
                "pct": round(ciac_data["wins"] / max(ciac_data["wins"] + ciac_data["losses"], 1), 3),
                "sport": "boys-basketball",
                "source": "ciac"
            }

    for norm, ciac_data in ciac_gbb.items():
        team_name = ciac_data["full_name"]
        if team_name not in final:
            final[team_name] = {
                "team": team_name,
                "wins": ciac_data["wins"],
                "losses": ciac_data["losses"],
                "pct": round(ciac_data["wins"] / max(ciac_data["wins"] + ciac_data["losses"], 1), 3),
                "sport": "girls-basketball",
                "source": "ciac"
            }

    # Now update master_games.csv records
    update_master_records(final)

    # Write team_records.csv
    fields = ["team", "wins", "losses", "pct", "sport", "source"]
    with open(RECORDS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in sorted(final.values(), key=lambda x: x["team"]):
            writer.writerow({k: row.get(k, "") for k in fields})

    print(f"\nWrote {len(final)} team records to {RECORDS_CSV}.")


def update_master_records(records_dict):
    """Back-fill home_record and away_record in master_games.csv from computed records."""
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
            fields = rows[0].keys() if rows else []
    except (FileNotFoundError, StopIteration):
        return

    changed = 0
    for row in rows:
        home = row.get("home_team", "").strip()
        away = row.get("away_team", "").strip()

        if home in records_dict and not row.get("home_record"):
            r = records_dict[home]
            row["home_record"] = f"{r['wins']}-{r['losses']}"
            changed += 1
        if away in records_dict and not row.get("away_record"):
            r = records_dict[away]
            row["away_record"] = f"{r['wins']}-{r['losses']}"
            changed += 1

    if changed:
        with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Updated {changed} record field(s) in {MASTER_CSV}.")


if __name__ == "__main__":
    main()
