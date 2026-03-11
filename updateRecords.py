"""
updateRecords.py

Two jobs:
  1. Scrape CIAC rankings for every CT team's authoritative season record,
     then scrape MaxPreps for any out-of-state teams not found in CIAC.
     Write all results to docs/team_records.csv.

  2. Recompute home_record / away_record for every row in master_games.csv
     using a chronological running walk anchored to authoritative totals so
     that games missing from the local CSV don't cause under-counts.

How the running-record anchor works
────────────────────────────────────
• Walk all final games in date order, tallying wins/losses locally.
• For each team compare local total vs authoritative total (CIAC or MaxPreps).
• If authoritative total is higher, games are missing. Compute:
    offset_w = auth_wins   - local_wins
    offset_l = auth_losses - local_losses
• Add that constant offset to every running total for that team throughout
  the season, as if the missing games were played before our first entry.

Example — Glastonbury: CIAC says 14-9, local CSV has 9-7
  offset = +5W +2L
  game 1 in CSV: local 1-0  → displayed 6-2
  last game:     local 9-7  → displayed 14-9
"""

import csv
import re
import time
from collections import defaultdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup

MASTER_CSV  = "docs/master_games.csv"
RECORDS_CSV = "docs/team_records.csv"

RANKINGS_URLS = {
    "boys-basketball":  "https://content.ciacsports.com/scripts/bbb_rankings2.cgi",
    "girls-basketball": "https://content.ciacsports.com/scripts/gbball_rankings2.cgi",
}

MAXPREPS_SEARCH = "https://www.maxpreps.com/api/site/search/autocomplete?term={query}&limit=5"
MAXPREPS_TEAM   = "https://www.maxpreps.com/api/team/record?teamId={team_id}&sport=basketball&season=2025-26"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.maxpreps.com/",
}

# ── Normalisation ─────────────────────────────────────────────────────────────

def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_master():
    try:
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
            fieldnames = list(rows[0].keys()) if rows else []
            return rows, fieldnames
    except FileNotFoundError:
        return [], []


def save_master(rows, fieldnames):
    with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ── Datetime helper ───────────────────────────────────────────────────────────

def parse_dt(s):
    try:
        return datetime.strptime(s, "%m/%d/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return datetime.max


# ── CIAC rankings scraper ─────────────────────────────────────────────────────

def scrape_ciac_records(sport):
    """Returns dict: normalized_name → (wins, losses, full_name)"""
    url = RANKINGS_URLS.get(sport)
    if not url:
        return {}
    try:
        resp = requests.get(url, timeout=15, headers=HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] Could not scrape CIAC rankings for {sport}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    out  = {}
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
        out[normalize(name)] = (wins, losses, name)

    print(f"  CIAC {sport}: {len(out)} teams scraped")
    return out


def find_ciac(name, ciac_map):
    """Return (wins, losses) from ciac_map or None."""
    norm = normalize(name)
    if norm in ciac_map:
        w, l, _ = ciac_map[norm]
        return w, l
    for key, (w, l, _) in ciac_map.items():
        if norm in key or key in norm:
            return w, l
    return None


# ── MaxPreps scraper (out-of-state fallback) ──────────────────────────────────

def scrape_maxpreps_record(team_name, is_girls, session):
    """
    Search MaxPreps for `team_name`, find the best basketball match,
    and return (wins, losses) for the current season, or None on failure.

    MaxPreps search returns JSON like:
      [{"teamId": "...", "name": "Trumbull", "state": "CT", "sport": "basketball", ...}, ...]

    The record endpoint returns JSON like:
      {"wins": 14, "losses": 6, "ties": 0, ...}
    """
    sport_slug = "girls-basketball" if is_girls else "boys-basketball"
    query      = requests.utils.quote(team_name)

    try:
        # Step 1: find team ID
        search_url = f"https://www.maxpreps.com/api/site/search/autocomplete?term={query}&limit=10"
        r = session.get(search_url, timeout=10)
        r.raise_for_status()
        results = r.json()

        # Filter to basketball teams, prefer exact name match
        candidates = [
            t for t in results
            if isinstance(t, dict)
            and normalize(t.get("sport", "")) in ("basketball", sport_slug.replace("-", ""))
        ]
        if not candidates:
            # Broaden — accept any result with a matching name
            candidates = [
                t for t in results
                if isinstance(t, dict)
                and normalize(team_name) in normalize(t.get("name", ""))
            ]
        if not candidates:
            return None

        # Pick best match: exact name first, then first result
        exact = [c for c in candidates if normalize(c.get("name", "")) == normalize(team_name)]
        chosen = exact[0] if exact else candidates[0]
        team_id = chosen.get("teamId") or chosen.get("id")
        if not team_id:
            return None

        # Step 2: fetch record for this season
        time.sleep(0.3)  # be polite
        rec_url = (
            f"https://www.maxpreps.com/api/team/record"
            f"?teamId={team_id}&sport={sport_slug}&season=2025-26"
        )
        r2 = session.get(rec_url, timeout=10)
        r2.raise_for_status()
        data = r2.json()

        # MaxPreps may nest under "overall" or return flat
        rec = data.get("overall") or data
        wins   = int(rec.get("wins",   rec.get("w", 0)))
        losses = int(rec.get("losses", rec.get("l", 0)))
        if wins + losses == 0:
            return None
        return wins, losses

    except Exception:
        return None


def scrape_oos_records(oos_teams, ciac_boys, ciac_girls):
    """
    For every out-of-state team, try MaxPreps.
    Returns dict: (header, team) → (wins, losses)
    """
    if not oos_teams:
        return {}

    print(f"\n  Fetching MaxPreps records for {len(oos_teams)} out-of-state team(s)…")
    out = {}
    session = requests.Session()
    session.headers.update(HEADERS)

    for (hdr, team) in sorted(oos_teams, key=lambda x: x[1]):
        is_girls = "girls" in hdr.lower()
        result   = scrape_maxpreps_record(team, is_girls, session)
        if result:
            w, l = result
            out[(hdr, team)] = (w, l)
            print(f"    ✓ {team}: {w}-{l}  (MaxPreps)")
        else:
            print(f"    ✗ {team}: not found on MaxPreps — record will be omitted")

    return out


# ── Step 1: build team_records.csv ───────────────────────────────────────────

def build_records_csv(ciac_boys, ciac_girls, oos_records):
    """Write team_records.csv from CIAC + MaxPreps data."""
    fields = ["team", "wins", "losses", "pct", "sport", "source"]
    rows   = []

    for norm, (w, l, name) in ciac_boys.items():
        total = w + l
        rows.append({
            "team": name, "wins": w, "losses": l,
            "pct": round(w / total, 3) if total else 0.0,
            "sport": "boys-basketball", "source": "ciac",
        })
    for norm, (w, l, name) in ciac_girls.items():
        total = w + l
        rows.append({
            "team": name, "wins": w, "losses": l,
            "pct": round(w / total, 3) if total else 0.0,
            "sport": "girls-basketball", "source": "ciac",
        })
    for (hdr, team), (w, l) in oos_records.items():
        total = w + l
        sport = "girls-basketball" if "girls" in hdr.lower() else "boys-basketball"
        rows.append({
            "team": team, "wins": w, "losses": l,
            "pct": round(w / total, 3) if total else 0.0,
            "sport": sport, "source": "maxpreps",
        })

    rows.sort(key=lambda r: r["team"])
    with open(RECORDS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  Wrote {len(rows)} records to {RECORDS_CSV}")


# ── Step 2: recompute running records in master_games.csv ────────────────────

def recompute_master_records(master_rows, ciac_boys, ciac_girls, oos_records):
    """
    Walk every final game chronologically, compute a running W-L for each team,
    offset by the gap between local counts and authoritative totals (CIAC for CT
    teams, MaxPreps for out-of-state), then write corrected records back.
    """

    # ── Pass 1: count local wins/losses per (header, team) ───────────────────
    local_w = defaultdict(int)
    local_l = defaultdict(int)

    final_rows = [
        r for r in master_rows
        if r.get("status") == "final"
        and r.get("home_score", "").strip()
        and r.get("away_score", "").strip()
    ]
    final_rows.sort(key=lambda r: parse_dt(r.get("game_datetime", "")))

    for r in final_rows:
        hdr = r.get("header", "")
        try:
            hs  = int(r["home_score"])
            as_ = int(r["away_score"])
        except (ValueError, TypeError):
            continue
        hk = (hdr, r["home_team"])
        ak = (hdr, r["away_team"])
        if hs > as_:
            local_w[hk] += 1; local_l[ak] += 1
        elif as_ > hs:
            local_w[ak] += 1; local_l[hk] += 1

    # ── Pass 2: compute per-team offset ──────────────────────────────────────
    offset_w   = defaultdict(int)
    offset_l   = defaultdict(int)
    no_source  = set()   # teams with no authoritative record at all
    patched_ct = 0
    patched_oos = 0

    all_teams = set(list(local_w.keys()) + list(local_l.keys()))
    for (hdr, team) in all_teams:
        ciac_map = ciac_boys if "boys" in hdr.lower() else ciac_girls
        result   = find_ciac(team, ciac_map)

        if result is None:
            # Try MaxPreps result
            result = oos_records.get((hdr, team))
            is_oos = True
        else:
            is_oos = False

        if result is None:
            no_source.add((hdr, team))
            continue

        auth_w, auth_l = result
        ow = max(0, auth_w - local_w[(hdr, team)])
        ol = max(0, auth_l - local_l[(hdr, team)])

        if ow or ol:
            offset_w[(hdr, team)] = ow
            offset_l[(hdr, team)] = ol
            src = "MaxPreps" if is_oos else "CIAC"
            tag = "OOS" if is_oos else "CT"
            print(
                f"  [OFFSET/{tag}] {team}: "
                f"local {local_w[(hdr,team)]}-{local_l[(hdr,team)]}  "
                f"{src} {auth_w}-{auth_l}  offset +{ow}W +{ol}L"
            )
            if is_oos:
                patched_oos += 1
            else:
                patched_ct += 1

    if no_source:
        print(f"\n  {len(no_source)} team(s) with no authoritative record — records omitted:")
        for (hdr, team) in sorted(no_source, key=lambda x: x[1]):
            print(f"    • {team}")
        print()

    print(f"  {patched_ct} CT + {patched_oos} OOS team(s) needed an offset")

    # ── Pass 3: walk again, writing offset-adjusted running records ───────────
    running_w    = defaultdict(int)
    running_l    = defaultdict(int)
    game_records = {}

    for r in final_rows:
        hdr = r.get("header", "")
        try:
            hs  = int(r["home_score"])
            as_ = int(r["away_score"])
        except (ValueError, TypeError):
            continue
        hk = (hdr, r["home_team"])
        ak = (hdr, r["away_team"])

        if hs > as_:
            running_w[hk] += 1; running_l[ak] += 1
        elif as_ > hs:
            running_w[ak] += 1; running_l[hk] += 1

        if hk in no_source:
            h_rec = ""
        else:
            h_rec = f"{running_w[hk] + offset_w[hk]}-{running_l[hk] + offset_l[hk]}"

        if ak in no_source:
            a_rec = ""
        else:
            a_rec = f"{running_w[ak] + offset_w[ak]}-{running_l[ak] + offset_l[ak]}"

        game_records[r["game_id"]] = (h_rec, a_rec)

    # ── Write back ────────────────────────────────────────────────────────────
    updated = 0
    for r in master_rows:
        gid = r.get("game_id")
        if gid in game_records:
            new_h, new_a = game_records[gid]
            if r.get("home_record") != new_h or r.get("away_record") != new_a:
                r["home_record"] = new_h
                r["away_record"] = new_a
                updated += 1

    print(f"  {updated} game row(s) updated in {MASTER_CSV}")
    return master_rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Scraping CIAC rankings…")
    ciac_boys  = scrape_ciac_records("boys-basketball")
    ciac_girls = scrape_ciac_records("girls-basketball")

    # First pass to identify OOS teams before MaxPreps lookups
    print("\nIdentifying out-of-state teams…")
    master_rows, fieldnames = load_master()
    if not master_rows:
        print("  No rows found in master_games.csv — nothing to update.")
        return

    final_rows = [
        r for r in master_rows
        if r.get("status") == "final"
        and r.get("home_score", "").strip()
        and r.get("away_score", "").strip()
    ]
    all_teams = set()
    for r in final_rows:
        hdr = r.get("header", "")
        all_teams.add((hdr, r["home_team"]))
        all_teams.add((hdr, r["away_team"]))

    oos_teams = set()
    for (hdr, team) in all_teams:
        ciac_map = ciac_boys if "boys" in hdr.lower() else ciac_girls
        if find_ciac(team, ciac_map) is None:
            oos_teams.add((hdr, team))

    # Fetch MaxPreps for out-of-state teams
    oos_records = scrape_oos_records(oos_teams, ciac_boys, ciac_girls)

    print("\nBuilding team_records.csv…")
    build_records_csv(ciac_boys, ciac_girls, oos_records)

    print("\nRecomputing running records in master_games.csv…")
    master_rows = recompute_master_records(master_rows, ciac_boys, ciac_girls, oos_records)
    save_master(master_rows, fieldnames)
    print("\nDone.")


if __name__ == "__main__":
    main()