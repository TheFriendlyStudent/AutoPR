import csv
import re
import uuid
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


MASTER_CSV = "docs/master_games.csv"

SPORTS = [
    {
        "url": "https://ciac.fpsports.org/DashboardSchedule.aspx?SportID=2_1015_5&QuickFilter=3",
        "header": "CIAC Boys Basketball",
    },
    {
        "url": "https://ciac.fpsports.org/DashboardSchedule.aspx?SportID=3_1015_5&QuickFilter=3",
        "header": "CIAC Girls Basketball",
    },
]

MASTER_FIELDS = [
    "game_id","header","home_team","away_team","home_rank","away_rank",
    "home_score","away_score","home_record","away_record",
    "bg_image","photo_cred","game_datetime","status",
    "posted_to_instagram","caption"
]


# -------------------------------------------------------
# CSV helpers
# -------------------------------------------------------

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
        writer.writerows(rows)


# -------------------------------------------------------
# Utility
# -------------------------------------------------------

def normalize(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())


def clean_team(name):
    return re.sub(r"\s*-\s*[IVX]+$", "", name).strip()


def make_game_key(home, away, dt):
    date = dt.split(" ")[0] if dt else ""
    return f"{normalize(home)}|{normalize(away)}|{date}"


def parse_datetime(date_str, time_str):

    date_str = date_str.strip()
    time_str = time_str.strip()

    try:
        dt = datetime.strptime(
            f"{date_str} {time_str}",
            "%a %m/%d %I:%M %p"
        )
        dt = dt.replace(year=datetime.now().year)
        return dt.strftime("%m/%d/%Y %H:%M:%S")

    except:
        return ""


# -------------------------------------------------------
# Selenium
# -------------------------------------------------------

def make_driver():

    opts = Options()

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=opts)


# -------------------------------------------------------
# Scraper
# -------------------------------------------------------

def scrape_sport(driver, url, header):

    print("Loading", header)

    driver.get(url)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".teams"))
    )

    rows = driver.find_elements(By.CSS_SELECTOR, "tr")

    games = []

    for row in rows:

        try:

            date = row.find_element(By.CSS_SELECTOR, ".date").text
            time = row.find_element(By.CSS_SELECTOR, ".time").text

            teams = row.find_elements(By.CSS_SELECTOR, ".team")

            if len(teams) != 2:
                continue

            team1 = teams[0]
            team2 = teams[1]

            t1 = clean_team(team1.text)
            t2 = clean_team(team2.text)

            # detect home team by house icon
            if "fa-house" in team1.get_attribute("innerHTML"):
                home = t1
                away = t2
            else:
                home = t2
                away = t1

            row_text = row.text.lower()

            if "scrimmage" in row_text or "jv" in row_text:
                continue

            score_match = re.search(r"(\d+)\s*[-–]\s*(\d+)", row_text)

            home_score = ""
            away_score = ""
            status = "scheduled"

            if score_match:
                home_score = score_match.group(1)
                away_score = score_match.group(2)
                status = "final"

            dt = parse_datetime(date, time)

            games.append({
                "home_team": home,
                "away_team": away,
                "game_datetime": dt,
                "header": header,
                "home_score": home_score,
                "away_score": away_score,
                "status": status
            })

        except:
            continue

    print("Parsed", len(games), "games")

    return games


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def main():

    existing = load_master()

    existing_keys = {
        make_game_key(r["home_team"], r["away_team"], r["game_datetime"])
        for r in existing
    }

    new_games = []

    driver = make_driver()

    try:

        for sport in SPORTS:

            scraped = scrape_sport(driver, sport["url"], sport["header"])

            for g in scraped:

                key = make_game_key(
                    g["home_team"],
                    g["away_team"],
                    g["game_datetime"]
                )

                if key not in existing_keys:

                    game_id = "ciac_" + str(uuid.uuid4())[:8]

                    new_games.append({
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
                        "posted_to_instagram": "false",
                        "caption": "",
                    })

                    existing_keys.add(key)

    finally:

        driver.quit()

    all_rows = existing + new_games

    save_master(all_rows)

    print("Added", len(new_games), "new games")


if __name__ == "__main__":
    main()