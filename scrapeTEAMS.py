"""
scrapeTEAMS.py
Scrapes CIAC boys & girls basketball schedules and scores.

Updates docs/master_games.csv by:
• appending new games
• updating scores when games become final
• recalculating records from scraped data

Usage:
    python scrapeTEAMS.py
    python scrapeTEAMS.py --week
    python scrapeTEAMS.py --all
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
BASE_URL = "https://ciac.fpsports.org/DashboardSchedule.aspx"

MAX_WORKERS = 12


SPORTS = [
    ("CIAC Boys Basketball", "2_1015_5"),
    ("CIAC Girls Basketball", "3_1015_5"),
]


SCHOOLS = [
    ("Abbott Tech",70),("Achievement First",5),("Aerospace",2),
    ("Amistad",7),("Amity",8),("Ansonia",9),("Avon",10),
    ("Bacon Academy",11),("Bassick",12),("Berlin",13),("Bethel",14),
    ("Bloomfield",15),("Bolton",16),("Branford",17),
    ("Bridgeport Central",18),("Brien McMahon",19),("Bristol Central",20),
    ("Bristol Eastern",21),("Brookfield",22),("Bulkeley",23),
    ("Bullard Havens Tech",24),("Bunnell",25),("Canton",26),
    ("Capital Prep",27),("Career Magnet",28),("Cheney Tech",73),
    ("Cheshire",29),("Classical Magnet",61),("Coginchaug",30),
    ("Comp Sci",127),("Conard",31),("Coventry",32),("Cromwell",33),
    ("Crosby",34),("Danbury",35),("Daniel Hand",36),("Darien",37),
    ("Derby",38),("E.O. Smith",51),("East Catholic",39),
    ("East Granby",40),("East Hampton",41),("East Hartford",42),
    ("East Haven",43),("East Lyme",44),("East Windsor",45),
    ("Ellington",48),("Ellis Tech",68),("Enfield",50),
    ("Fairfield Ludlowe",53),("Fairfield Prep",52),("Fairfield Warde",54),
    ("Farmington",55),("Fitch",56),("Foran",80),("Gilbert",57),
    ("Glastonbury",58),("Goodwin Tech",46),("Granby Memorial",59),
    ("Grasso Tech",60),("Greenwich",62),("Griswold",63),
    ("Guilford",64),("Haddam Killingworth",65),("Hale Ray",98),
    ("Hall",183),("Hamden",66),("Harding",164),("Hartford Public",67),
    ("Hillhouse",75),("HMTCA",158),("Holy Cross",71),
    ("Housatonic Regional",72),("Immaculate",74),("Innovation",92),
    ("International",95),("Joel Barlow",78),("Jonathan Law",79),
    ("Kaynor Tech",174),("Kennedy",76),("Killingly",81),
    ("Kolbe Cathedral",82),("Ledyard",83),("Lewis Mills",84),
    ("Lyman Hall",86),("Lyman Memorial",87),("Maloney",89),
    ("Manchester",90),("Masuk",91),("Mercy",93),("Middletown",94),
    ("Montville",96),("Morgan",97),("Naugatuck",99),
    ("New Britain",100),("New Canaan",101),("New Fairfield",102),
    ("New London",104),("New Milford",105),("Newington",103),
    ("Newtown",106),("NFA",113),("Nonnewaug",107),
    ("North Branford",108),("North Haven",109),("Northwest Catholic",110),
    ("Northwestern",111),("Norwalk",112),("Norwich Tech",114),
    ("Notre Dame-West Haven",115),("O'Brien Tech",49),
    ("Old Saybrook",118),("Oxford",120),("Plainfield",122),
    ("Plainville",123),("Platt",117),("Platt Tech",124),
    ("Pomperaug",125),("Portland",126),("Prince Tech",6),
    ("Putnam",128),("RHAM",129),("Ridgefield",130),("Rockville",131),
    ("Rocky Hill",132),("Seymour",134),("Sheehan",135),
    ("Shelton",136),("Shepaug Valley",137),("Simsbury",138),
    ("SMSA",142),("Somers",139),("South Windsor",141),
    ("Southington",140),("St. Bernard",146),("St. Joseph",147),
    ("St. Paul Catholic",149),("Stafford",143),("Stamford",144),
    ("Staples",145),("Stonington",148),("Stratford",150),
    ("Suffield",151),("Terryville",152),("Thomaston",154),
    ("Tolland",155),("Torrington",156),("Tourtellotte",157),
    ("Trumbull",159),("University",160),("Valley Regional",161),
    ("Vinal Tech",162),("Waterford",166),("Watertown",167),
    ("WCA",165),("Weaver",168),("West Haven",170),("Westbrook",169),
    ("Westhill",171),("Weston",172),("Wethersfield",173),
    ("Wheeler",175),("Whitney Tech",47),("Wilbur Cross",176),
    ("Wilby",177),("Wilcox Tech",69),("Wilton",178),
    ("Windham",179),("Windham Tech",180),("Windsor",181),
    ("Windsor Locks",182),("Wolcott",184),("Wolcott Tech",119),
    ("Woodland",185),("Woodstock Academy",186),("Wright Tech",77),
    ("Xavier",187)
]


MASTER_FIELDS = [
"game_id","header","home_team","away_team","home_rank","away_rank",
"home_score","away_score","home_record","away_record",
"bg_image","photo_cred","game_datetime","status",
"posted_to_instagram","caption"
]


HEADERS = {
"User-Agent":"Mozilla/5.0",
"Accept-Language":"en-US,en;q=0.9"
}


# ---------------- CSV ---------------- #

def load_master():
    try:
        with open(MASTER_CSV,newline="",encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def save_master(rows):
    with open(MASTER_CSV,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=MASTER_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in MASTER_FIELDS})


# ---------------- Utilities ---------------- #

def normalize(name):
    return re.sub(r"[^a-z0-9]","",name.lower())


def make_key(home,away,dt):
    teams=sorted([normalize(home),normalize(away)])
    date=dt.split(" ")[0]
    return f"{teams[0]}|{teams[1]}|{date}"


def parse_dt(dt):
    try:
        return datetime.strptime(dt,"%m/%d/%Y %H:%M:%S")
    except:
        return datetime.max


def sort_key(r):
    return(parse_dt(r.get("game_datetime","")),
           r.get("header",""),
           r.get("home_team",""),
           r.get("game_id",""))


# ---------------- Parser ---------------- #

def parse_datetime(date_str,time_str):

    md=re.search(r"(\d+)/(\d+)",date_str)
    tm=re.match(r"(\d+):(\d+)\s*([AP]M)",time_str.strip(),re.I)

    if not md or not tm:
        return ""

    m,d=int(md.group(1)),int(md.group(2))
    h,min=int(tm.group(1)),int(tm.group(2))
    ap=tm.group(3).upper()

    if ap=="PM" and h!=12:
        h+=12
    if ap=="AM" and h==12:
        h=0

    today=datetime.now()

    year=today.year-1 if(m>=11 and today.month<7) else today.year

    return datetime(year,m,d,h,min).strftime("%m/%d/%Y %H:%M:%S")


def parse_row(row):

    cells=row.find_all("td")
    if not cells:
        return None

    date_span=cells[0].find("span",class_="date")
    time_span=cells[0].find("span",class_="time")

    if not date_span or not time_span:
        return None

    dt=parse_datetime(date_span.text.strip(),time_span.text.strip())
    if not dt:
        return None

    a=row.find("a",href=re.compile("dashboardgame",re.I))
    if not a:
        return None

    teams=a.find_all("div",class_="team")

    clean=[]
    for t in teams:
        if re.search(r"-\s*[IVX]+",t.text):
            clean.append(t)

    if len(clean)<2:
        return None

    def get(div):

        score_div=div.find("div",class_="scoreright")
        score=score_div.text.strip() if score_div else ""

        for tag in div.find_all(["div","i"]):
            tag.decompose()

        name=re.sub(r"\s*-\s*[IVX]+\s*$","",div.text.strip())

        return name,score

    home_idx=0
    for i,d in enumerate(clean):
        if d.find("i",class_=re.compile("fa-house")):
            home_idx=i
            break

    away_idx=1 if home_idx==0 else 0

    home,hs=get(clean[home_idx])
    away,as_=get(clean[away_idx])

    if not home or not away:
        return None

    return{
        "home_team":home,
        "away_team":away,
        "home_score":hs,
        "away_score":as_,
        "game_datetime":dt,
        "status":"final" if(hs and as_) else "scheduled"
    }


# ---------------- Fetch ---------------- #

def fetch_page(sport,qf,school=None):

    params={"L":"1","SportID":sport,"QuickFilter":qf}

    if school:
        params["SchoolID"]=school

    try:
        r=requests.get(BASE_URL,params=params,headers=HEADERS,timeout=20)
        r.raise_for_status()
    except:
        return []

    soup=BeautifulSoup(r.text,"html.parser")

    table=soup.find("table")
    if not table:
        return []

    games=[]
    for tr in table.find_all("tr"):
        g=parse_row(tr)
        if g:
            games.append(g)

    return games


def fetch_today(sport):

    today=datetime.now().strftime("%m/%d/%Y")
    seen={}

    for qf in("2","1"):

        for g in fetch_page(sport,qf):

            if not g["game_datetime"].startswith(today):
                continue

            key=make_key(g["home_team"],g["away_team"],g["game_datetime"])

            if key not in seen or g["status"]=="final":
                seen[key]=g

    return list(seen.values())


def fetch_all(sport):

    all_games=[]
    seen=set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:

        futures=[ex.submit(fetch_page,sport,"3",sid) for _,sid in SCHOOLS]

        for f in as_completed(futures):

            try:
                for g in f.result():

                    key=make_key(g["home_team"],g["away_team"],g["game_datetime"])

                    if key not in seen:
                        seen.add(key)
                        all_games.append(g)

            except Exception as e:
                print("WARN",e,file=sys.stderr)

    return all_games


# ---------------- Records ---------------- #

def calculate_records(rows):

    finals=[r for r in rows if r["status"]=="final" and r["home_score"] and r["away_score"]]

    finals.sort(key=sort_key)

    wins=defaultdict(int)
    losses=defaultdict(int)
    rec={}

    for r in finals:

        h=r["home_team"]
        a=r["away_team"]
        hdr=r["header"]

        try:
            hs=int(r["home_score"])
            as_=int(r["away_score"])
        except:
            continue

        if hs>as_:
            wins[(hdr,h)]+=1
            losses[(hdr,a)]+=1
        else:
            wins[(hdr,a)]+=1
            losses[(hdr,h)]+=1

        rec[r["game_id"]]=(f"{wins[(hdr,h)]}-{losses[(hdr,h)]}",
                           f"{wins[(hdr,a)]}-{losses[(hdr,a)]}")

    for r in rows:
        if r["game_id"] in rec:
            r["home_record"],r["away_record"]=rec[r["game_id"]]

    return rows


# ---------------- Main ---------------- #

def main():

    parser=argparse.ArgumentParser()
    g=parser.add_mutually_exclusive_group()

    g.add_argument("--all",action="store_true")
    g.add_argument("--week",action="store_true")

    args=parser.parse_args()

    existing=load_master()

    by_key={make_key(r["home_team"],r["away_team"],r["game_datetime"]):r for r in existing}

    new=0
    updated=0

    for header,sport in SPORTS:

        if args.all:
            games=fetch_all(sport)
        elif args.week:
            games=fetch_page(sport,"2")
        else:
            games=fetch_today(sport)

        print(header,len(games),"games")

        for g in games:

            key=make_key(g["home_team"],g["away_team"],g["game_datetime"])

            if key in by_key:

                row=by_key[key]

                if g["status"]=="final" and row["status"]!="final":

                    row["home_score"]=g["home_score"]
                    row["away_score"]=g["away_score"]
                    row["status"]="final"

                    updated+=1

            else:

                by_key[key]={

                "game_id":"ciac_"+str(uuid.uuid4())[:8],
                "header":header,
                "home_team":g["home_team"],
                "away_team":g["away_team"],
                "home_rank":"NR",
                "away_rank":"NR",
                "home_score":g["home_score"],
                "away_score":g["away_score"],
                "home_record":"",
                "away_record":"",
                "bg_image":"",
                "photo_cred":"",
                "game_datetime":g["game_datetime"],
                "status":g["status"],
                "posted_to_instagram":"FALSE",
                "caption":""

                }

                new+=1

    rows=list(by_key.values())

    ciac=[r for r in rows if r["game_id"].startswith("ciac_")]
    manual=[r for r in rows if not r["game_id"].startswith("ciac_")]

    ciac=calculate_records(ciac)

    final=sorted(ciac+manual,key=sort_key)

    save_master(final)

    print("Done:",new,"new",updated,"updated")


if __name__=="__main__":
    main()