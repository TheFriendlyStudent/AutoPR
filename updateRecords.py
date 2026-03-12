"""
updateRecords.py

For every team that appears in master_games.csv:

1.  Determine if the team is a CT school (present in the SCHOOLS list from
    scrapeTEAMS.py) or out-of-state.

2.  Build a complete game list for the season:
      - Primary:  CIAC schedule page for the team (CT teams only)
      - Fallback: MaxPreps schedule for any game the CIAC page is missing,
                  and for all OOS teams.

3.  Walk that complete game list chronologically.  After each game, record
    the running W-L for that team.  Write those running records back into
    the matching rows of master_games.csv (home_record / away_record).

4.  Write docs/team_records.csv with current-season totals + MaxPreps player
    stats (PPG / RPG / APG) for every team that has a MaxPreps entry.

Key design decisions
────────────────────
- CT/OOS classification comes solely from the SCHOOLS list — not from
  whether the team appears on the CIAC rankings page.
- The CIAC rankings page is NOT used for record calculation at all.
  Records come from actual game results (CIAC schedule page + MaxPreps fallback).
- A game is "missing" from CIAC if MaxPreps shows a result for that date
  that doesn't appear in the CIAC schedule.
- Running records in master_games.csv reflect the true record at the time
  of each game (including any games not in our master CSV).
"""

import csv
import json
import re
import time
from collections import defaultdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Import the authoritative CT school list from scrapeTEAMS.
# CT/OOS classification uses this list — not the CIAC rankings page.
from schools import SCHOOLS, normalize, is_ct_school, ciac_id as find_ciac_school_id
# Remove: from scrapeTEAMS import SCHOOLS, SPORTS
# Remove: CT_SCHOOL_NAMES, CIAC_SCHOOL_IDS buildout, is_ct_school(), find_ciac_school_id()

MASTER_CSV  = "docs/master_games.csv"
RECORDS_CSV = "docs/team_records.csv"

SPORTS = [
    ("CIAC Boys Basketball", "2_1015_5"),
    ("CIAC Girls Basketball", "3_1015_5"),
]


# Build fast-lookup structures from SCHOOLS list
CT_SCHOOL_NAMES = set(re.sub(r"[^a-z0-9]", "", name.lower()) for name, _ in SCHOOLS)
CIAC_SCHOOL_IDS = {re.sub(r"[^a-z0-9]", "", name.lower()): sid for name, sid in SCHOOLS}
CIAC_SPORTS     = {h: sid for h, sid in SPORTS}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.maxpreps.com/",
}

CIAC_BASE     = "https://ciac.fpsports.org/DashboardSchedule.aspx"
RECORDS_FIELDS = ["team", "sport", "wins", "losses", "pct", "source", "players"]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_dt(s):
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime((s or "").strip(), fmt)
        except ValueError:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CSV helpers
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# CIAC individual team schedule scraper
# ─────────────────────────────────────────────────────────────────────────────

def scrape_ciac_schedule(team_name, header):
    """
    Fetch the full season schedule for one team from the CIAC schedule page.
    Returns list of game dicts sorted by date:
      {date, dt, opponent, our_score, opp_score, status}
    """
    school_id = find_ciac_school_id(team_name)
    sport_id  = CIAC_SPORTS.get(header)
    if not school_id or not sport_id:
        return []

    try:
        resp = requests.get(
            CIAC_BASE,
            params={"L": "1", "SportID": sport_id, "QuickFilter": "3",
                    "SchoolID": school_id},
            headers=HEADERS, timeout=20,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"    [WARN] CIAC schedule fetch failed for {team_name}: {e}")
        return []

    soup  = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    games = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        gt = row.find("td", class_="gametype")
        if gt and "scrimmage" in gt.get_text(strip=True).lower():
            continue

        date_span = cells[0].find("span", class_="date")
        if not date_span:
            continue
        raw_date = date_span.get_text(strip=True)
        md = re.search(r"(\d{1,2})/(\d{1,2})", raw_date)
        if not md:
            continue
        month, day = int(md.group(1)), int(md.group(2))
        today = datetime.now()
        year  = (today.year - 1) if (month >= 11 and today.month < 7) else today.year
        try:
            dt = datetime(year, month, day)
        except ValueError:
            continue

        a = None
        for cell in cells:
            a = cell.find("a", href=re.compile(r"/dashboardgame\.aspx", re.I))
            if a:
                break
        if not a:
            continue

        # Find team divs (must look like "Name - I" division format)
        team_divs = [
            d for d in a.find_all("div", class_="team")
            if re.search(r"-\s*[IVX]+", d.get_text(separator=" ", strip=True))
        ]
        if len(team_divs) < 2:
            continue

        def extract(div):
            score_div = div.find("div", class_="scoreright")
            score_txt = score_div.get_text(strip=True) if score_div else ""
            for tag in div.find_all(["i", "div"]):
                tag.decompose()
            name = re.sub(r"\s*-\s*[IVX]+\s*$", "", div.get_text(strip=True)).strip()
            try:
                score = int(score_txt)
            except (ValueError, TypeError):
                score = None
            return name, score

        # Identify home/away by fa-house icon
        home_idx = 0
        for i, div in enumerate(team_divs):
            if div.find("i", class_=re.compile(r"fa-house")):
                home_idx = i
                break
        away_idx = 1 - home_idx

        home_name, home_score = extract(team_divs[home_idx])
        away_name, away_score = extract(team_divs[away_idx])

        # Determine which side is "us"
        us_norm = normalize(team_name)
        if normalize(home_name) == us_norm or us_norm in normalize(home_name) or normalize(home_name) in us_norm:
            is_home   = True
            our_score = home_score
            opp_score = away_score
            opp_name  = away_name
        else:
            is_home   = False
            our_score = away_score
            opp_score = home_score
            opp_name  = home_name

        status = "final" if (our_score is not None and opp_score is not None) else "scheduled"
        games.append({
            "date":      dt.strftime("%m/%d/%Y"),
            "dt":        dt,
            "opponent":  opp_name,
            "our_score": our_score,
            "opp_score": opp_score,
            "is_home":   is_home,
            "status":    status,
        })

    games.sort(key=lambda g: g["dt"])
    return games


# ─────────────────────────────────────────────────────────────────────────────
# MaxPreps helpers
# ─────────────────────────────────────────────────────────────────────────────

def maxpreps_find_team(team_name, is_girls, session):
    """Search MaxPreps and return (team_id, display_name) or None."""
    sport_slug = "girls-basketball" if is_girls else "boys-basketball"
    try:
        r = session.get(
            "https://www.maxpreps.com/api/site/search/autocomplete",
            params={"term": team_name, "limit": 10},
            timeout=10,
        )
        r.raise_for_status()
        results = r.json()
    except Exception:
        return None

    candidates = [
        t for t in results if isinstance(t, dict)
        and normalize(t.get("sport", "")) in ("basketball", sport_slug.replace("-", ""))
    ]
    if not candidates:
        candidates = [
            t for t in results if isinstance(t, dict)
            and normalize(team_name) in normalize(t.get("name", ""))
        ]
    if not candidates:
        return None

    exact = [c for c in candidates if normalize(c.get("name", "")) == normalize(team_name)]
    # Prefer CT match for CT schools
    ct_cands = [c for c in (exact or candidates) if c.get("state", "").upper() == "CT"]
    chosen   = ct_cands[0] if ct_cands else (exact[0] if exact else candidates[0])
    team_id  = chosen.get("teamId") or chosen.get("id")
    return (team_id, chosen.get("name", team_name)) if team_id else None


def maxpreps_schedule(team_id, sport_slug, session):
    """
    Fetch full season schedule from MaxPreps.
    Returns list of {date, dt, opponent, our_score, opp_score, status}.
    """
    try:
        r = session.get(
            "https://www.maxpreps.com/api/team/schedule",
            params={"teamId": team_id, "sport": sport_slug, "season": "2025-26"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []

    entries = data if isinstance(data, list) else data.get("games", data.get("schedule", []))
    games   = []
    for g in entries:
        raw_date = g.get("date") or g.get("gameDate") or g.get("scheduledDate") or ""
        dt = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(raw_date[:len(fmt)], fmt)
                break
            except (ValueError, TypeError):
                pass
        if not dt:
            continue

        scores    = g.get("score") or {}
        is_home   = g.get("isHome", False)
        our_score = scores.get("home") if is_home else scores.get("away")
        opp_score = scores.get("away") if is_home else scores.get("home")
        if our_score is None:
            our_score = g.get("teamScore") or g.get("homeScore" if is_home else "awayScore")
        if opp_score is None:
            opp_score = g.get("opponentScore") or g.get("awayScore" if is_home else "homeScore")

        try:
            our_score = int(our_score) if our_score is not None else None
            opp_score = int(opp_score) if opp_score is not None else None
        except (TypeError, ValueError):
            our_score = opp_score = None

        opp_name = (g.get("opponent") or {}).get("name") or g.get("opponentName") or "Unknown"
        status   = "final" if (our_score is not None and opp_score is not None) else "scheduled"

        games.append({
            "date":      dt.strftime("%m/%d/%Y"),
            "dt":        dt,
            "opponent":  opp_name,
            "our_score": our_score,
            "opp_score": opp_score,
            "status":    status,
        })

    games.sort(key=lambda g: g["dt"])
    return games


def maxpreps_players(team_id, sport_slug, session):
    """Fetch top player stats. Returns list of {name, ppg, rpg, apg}."""
    try:
        r = session.get(
            "https://www.maxpreps.com/api/team/stats/leaders",
            params={"teamId": team_id, "sport": sport_slug, "season": "2025-26"},
            timeout=10,
        )
        r.raise_for_status()
        sdata = r.json()
    except Exception:
        return []

    raw = sdata if isinstance(sdata, list) else sdata.get("players", sdata.get("data", []))
    players, seen = [], set()
    for p in raw[:10]:
        name = (p.get("fullName") or p.get("name") or
                f"{p.get('firstName', '')} {p.get('lastName', '')}".strip())
        if not name or name in seen:
            continue
        seen.add(name)
        parts = name.split()
        short = f"{parts[0][0]}. {' '.join(parts[1:])}" if len(parts) > 1 else name

        def _f(key, alt=None):
            v = p.get(key, p.get(alt) if alt else None)
            try:
                return round(float(v), 1)
            except (TypeError, ValueError):
                return None

        ppg = _f("pointsPerGame", "ppg")
        rpg = _f("reboundsPerGame", "rpg")
        apg = _f("assistsPerGame", "apg")
        if ppg is None and rpg is None and apg is None:
            continue
        players.append({"name": short, "ppg": ppg, "rpg": rpg, "apg": apg})

    players.sort(key=lambda p: p.get("ppg") or 0, reverse=True)
    return players[:8]


# ─────────────────────────────────────────────────────────────────────────────
# Merge CIAC + MaxPreps schedules for one team
# ─────────────────────────────────────────────────────────────────────────────

def merge_schedules(ciac_games, mp_games):
    """
    CIAC is authoritative. MaxPreps fills in scored games whose dates are
    absent from CIAC (i.e. CIAC didn't record a score for that game).
    """
    ciac_dates = {g["date"] for g in ciac_games}
    merged     = list(ciac_games)
    for g in mp_games:
        if g["date"] not in ciac_dates and g["status"] == "final":
            merged.append(g)
    merged.sort(key=lambda g: g["dt"])
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Build full schedule for every team
# ─────────────────────────────────────────────────────────────────────────────

def build_all_schedules(master_rows, session):
    """
    Returns:
        schedules : (header, team) → [game dict, ...]   chronological
        mp_info   : (header, team) → {wins, losses, players, source}
    """
    pairs = set()
    for r in master_rows:
        hdr = r.get("header", "")
        for side in ("home_team", "away_team"):
            if r.get(side):
                pairs.add((hdr, r[side]))

    total     = len(pairs)
    schedules = {}
    mp_info   = {}

    for i, (hdr, team) in enumerate(sorted(pairs, key=lambda x: x[1]), 1):
        is_girls   = "girls" in hdr.lower()
        sport_slug = "girls-basketball" if is_girls else "boys-basketball"
        ct         = is_ct_school(team)

        # ── CIAC schedule (CT only) ───────────────────────────────────────────
        ciac_games = []
        if ct:
            ciac_games = scrape_ciac_schedule(team, hdr)
            time.sleep(0.15)

        # ── MaxPreps (all teams — fills gaps + player stats) ─────────────────
        mp_games  = []
        players   = []
        mp_result = maxpreps_find_team(team, is_girls, session)
        if mp_result:
            team_id, _ = mp_result
            time.sleep(0.25)
            mp_games = maxpreps_schedule(team_id, sport_slug, session)
            time.sleep(0.25)
            players  = maxpreps_players(team_id, sport_slug, session)
            time.sleep(0.1)

        # ── Merge ─────────────────────────────────────────────────────────────
        if ct:
            full = merge_schedules(ciac_games, mp_games)
            src  = "ciac+maxpreps" if mp_games else "ciac"
        else:
            full = mp_games
            src  = "maxpreps" if mp_games else "none"

        # ── Current season W-L from complete schedule ─────────────────────────
        w = l = 0
        for g in full:
            if g["status"] == "final" and g["our_score"] is not None and g["opp_score"] is not None:
                if g["our_score"] > g["opp_score"]:
                    w += 1
                elif g["opp_score"] > g["our_score"]:
                    l += 1

        schedules[(hdr, team)] = full
        mp_info[(hdr, team)]   = {"wins": w, "losses": l, "players": players, "source": src}

        tag = "CT" if ct else "OOS"
        print(f"  [{i:3}/{total}] [{tag}] {team} ({'G' if is_girls else 'B'}): "
              f"CIAC {len(ciac_games)}, MP {len(mp_games)} → {w}-{l}, "
              f"{len(players)} players")

    return schedules, mp_info


# ─────────────────────────────────────────────────────────────────────────────
# Write team_records.csv
# ─────────────────────────────────────────────────────────────────────────────

def write_records_csv(mp_info):
    rows, seen = [], set()
    for (hdr, team), info in sorted(mp_info.items(), key=lambda x: x[0][1]):
        sport = "girls-basketball" if "girls" in hdr.lower() else "boys-basketball"
        key   = (normalize(team), sport)
        if key in seen:
            continue
        seen.add(key)
        total = info["wins"] + info["losses"]
        rows.append({
            "team":    team,
            "sport":   sport,
            "wins":    info["wins"],
            "losses":  info["losses"],
            "pct":     round(info["wins"] / total, 3) if total else 0.0,
            "source":  info["source"],
            "players": json.dumps(info["players"]),
        })
    with open(RECORDS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RECORDS_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  Wrote {len(rows)} records to {RECORDS_CSV}")


# ─────────────────────────────────────────────────────────────────────────────
# Recompute running records in master_games.csv
# ─────────────────────────────────────────────────────────────────────────────

def recompute_master_records(master_rows, schedules):
    """
    For every final game row in master_games.csv, find where it sits in
    each team's full chronological schedule and write the running W-L
    that each team had *after* that game.

    Matching is done by date + scores so we don't rely on fragile name
    matching against the CIAC/MaxPreps opponent name.
    """

    def running_record_at(schedule, target_date, our_score, opp_score):
        """
        Walk schedule up to and including the game on target_date with
        matching scores.  Returns (w, l) or None if not matched.
        """
        w = l = 0
        for g in schedule:
            if g["status"] != "final" or g["our_score"] is None:
                continue
            same_date  = g["date"] == target_date
            same_score = (g["our_score"] == our_score and g["opp_score"] == opp_score)
            before     = g["dt"] < datetime.strptime(target_date, "%m/%d/%Y")

            if before or (same_date and same_score):
                if g["our_score"] > g["opp_score"]:
                    w += 1
                elif g["opp_score"] > g["our_score"]:
                    l += 1
                if same_date and same_score:
                    return (w, l)

        return None   # game not found in schedule

    updated = 0
    for r in master_rows:
        if r.get("status") != "final":
            continue
        hdr    = r.get("header", "")
        dt_obj = parse_dt(r.get("game_datetime", ""))
        if not dt_obj:
            continue
        date_str = dt_obj.strftime("%m/%d/%Y")

        try:
            hs  = int(r["home_score"])
            as_ = int(r["away_score"])
        except (ValueError, TypeError):
            continue

        hk = (hdr, r["home_team"])
        ak = (hdr, r["away_team"])

        h_res = running_record_at(schedules.get(hk, []), date_str, hs, as_)
        a_res = running_record_at(schedules.get(ak, []), date_str, as_, hs)

        new_h = f"{h_res[0]}-{h_res[1]}" if h_res else r.get("home_record", "")
        new_a = f"{a_res[0]}-{a_res[1]}" if a_res else r.get("away_record", "")

        if r.get("home_record") != new_h or r.get("away_record") != new_a:
            r["home_record"] = new_h
            r["away_record"] = new_a
            updated += 1

    print(f"  {updated} game row(s) updated in {MASTER_CSV}")
    return master_rows


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Loading master_games.csv…")
    master_rows, fieldnames = load_master()
    if not master_rows:
        print("  No rows found — nothing to update.")
        return
    print(f"  {len(master_rows)} rows loaded.")

    all_pairs = set()
    for r in master_rows:
        hdr = r.get("header", "")
        for side in ("home_team", "away_team"):
            if r.get(side):
                all_pairs.add((hdr, r[side]))
    print(f"  {len(all_pairs)} unique (sport, team) pairs.")

    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"\nFetching schedules for all teams…")
    schedules, mp_info = build_all_schedules(master_rows, session)

    print("\nWriting team_records.csv…")
    write_records_csv(mp_info)

    print("\nRecomputing running records in master_games.csv…")
    master_rows = recompute_master_records(master_rows, schedules)
    save_master(master_rows, fieldnames)
    print("\nDone.")


if __name__ == "__main__":
    main()