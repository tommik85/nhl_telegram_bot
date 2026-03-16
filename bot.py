# -*- coding: utf-8 -*-

import os
import time
import json
import logging
import sqlite3
import feedparser
import requests
import urllib.parse
from datetime import datetime, timedelta
from dateutil import parser as dateparser
import pytz

###############################################################################
# CONFIG
###############################################################################

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki")
DB_PATH = os.getenv("DB_PATH", "nhlbot.db")

# 48h tuoreussuodatin
MAX_HOURS = 48

TWITTER_USERS = [
    "FriedgeHNIC",
    "reporterchris",
    "DarrenDreger",
    "PierreVLeBrun",
    "frank_seravalli",
    "RussoHockey"
]

RSS_FEEDS = [
    "https://www.iltalehti.fi/rss/nhl.xml",
    "https://www.is.fi/rss/nhl.xml",
    "https://www.nhl.com/rss/news.xml",
    "https://www.tsn.ca/nhl/rss.xml",
    "https://www.sportsnet.ca/feed/nhl/",
    "https://www.espn.com/espn/rss/nhl/news",
    "https://www.thehockeynews.com/rss",
    "https://www.cbssports.com/nhl/feeds/rss/",
    "https://www.sbnation.com/rss/nhl/index.xml",
    "https://www.yardbarker.com/rss/nhl"
]

HTTP_TIMEOUT = 20

# Suomalaisraportin ajo
NIGHTLY_STATS_HOUR = 8
NIGHTLY_STATS_MINUTE = 0

###############################################################################
# LOGGING
###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

###############################################################################
# HTTP SESSION (Railway Free-tier stabilizer)
###############################################################################

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

###############################################################################
# DATABASE
###############################################################################

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with db_conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS seen_items (
            url TEXT PRIMARY KEY,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT)""")
        conn.commit()

def has_seen(url: str) -> bool:
    with db_conn() as conn:
        r = conn.execute("SELECT 1 FROM seen_items WHERE url=?", (url,))
        return r.fetchone() is not None

def mark_seen(url: str):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
        conn.commit()

def get_setting(key: str):
    with db_conn() as conn:
        r = conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = r.fetchone()
        return row[0] if row else None

def set_setting(key: str, value: str):
    with db_conn() as conn:
        conn.execute("""INSERT INTO settings(key,value)
                        VALUES(?,?)
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                     (key,value))
        conn.commit()

###############################################################################
# HELPERS
###############################################################################

def now_local():
    return datetime.now(pytz.timezone(TIMEZONE))

def normalize_url(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(p.query)
        q = [(k,v) for k,v in q if not k.lower().startswith(("utm_","fbclid","gclid"))]
        new_q = urllib.parse.urlencode(q)
        return urllib.parse.urlunparse((p.scheme,p.netloc,p.path,p.params,new_q,""))
    except:
        return url

def send_telegram(msg: str):
    if not TOKEN or not CHAT_ID:
        print("TOKEN/CHAT_ID missing")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram send error: {e}")

def is_recent(date_str: str, hours=48):
    if not date_str:
        return False
    try:
        dt = dateparser.parse(date_str)
        if not dt.tzinfo:
            dt = pytz.timezone(TIMEZONE).localize(dt)
        age = now_local() - dt
        return age.total_seconds() <= hours*3600
    except:
        return False

###############################################################################
# RSS MODULE
###############################################################################

def poll_rss():
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                pub = getattr(entry,"published",None) or getattr(entry,"updated",None)
                if not is_recent(pub, MAX_HOURS):
                    continue

                link = normalize_url(getattr(entry,"link",""))
                title = getattr(entry,"title","").strip()
                if not link or not title: 
                    continue

                if has_seen(link):
                    continue

                send_telegram(f"🚨 NHL-UUTINEN\n\n{title}\n{link}")
                mark_seen(link)

        except Exception as e:
            logging.warning(f"RSS error {feed_url}: {e}")
            time.sleep(2)

###############################################################################
# A-VERSION FREE TWITTER/X MODULE (NO API, NO SNSCRAPE)
###############################################################################

def twitter_get_user_id(username: str):
    """Hakee julkisen user-id:n X:n syndication endpointista."""
    try:
        url = f"https://cdn.syndication.twimg.com/user/by-screen-name/{username}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except:
        return None

def twitter_get_latest_tweets(user_id: str, limit=5):
    """Hakee profiilin twiittejä ilman kirjautumista."""
    try:
        url = f"https://cdn.syndication.twimg.com/timeline/profile/{user_id}.json"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        items = data.get("instructions",[])
        tweets = []
        for instr in items:
            if "addEntries" in instr:
                for entry in instr["addEntries"]["entries"]:
                    tw = entry.get("content",{}).get("item",{}).get("content",{}).get("tweet")
                    if tw:
                        tweets.append(tw)
        return tweets[:limit]
    except:
        return []

def poll_twitter():
    for handle in TWITTER_USERS:
        try:
            cache_key = f"twitter_userid_{handle}"
            user_id = get_setting(cache_key)
            if not user_id:
                user_id = twitter_get_user_id(handle)
                if not user_id:
                    continue
                set_setting(cache_key, user_id)

            tweets = twitter_get_latest_tweets(user_id)
            for tw in tweets:
                tid = tw.get("id_str") or tw.get("id")
                text = tw.get("full_text") or tw.get("text") or ""
                created = tw.get("created_at")
                url = f"https://x.com/{handle}/status/{tid}"

                if not is_recent(created, MAX_HOURS):
                    continue
                if has_seen(url):
                    continue

                msg = f"🐦 X — {handle}\n\n{text}\n{url}"
                send_telegram(msg)
                mark_seen(url)

        except Exception as e:
            logging.warning(f"Twitter error for {handle}: {e}")

###############################################################################
# NHL FINNISH PLAYERS STATS MODULE
###############################################################################

def nhl_schedule(date_str):
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={date_str}"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def nhl_boxscore(game_pk):
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_pk}/boxscore"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def last_completed_nhl_date():
    today = now_local().date()
    for d in [today - timedelta(days=1), today - timedelta(days=2)]:
        ds = d.strftime("%Y-%m-%d")
        try:
            sch = nhl_schedule(ds)
            if sch.get("dates"):
                games = sch["dates"][0].get("games",[])
                finals = [g for g in games if g.get("status",{}).get("detailedState","").startswith("Final")]
                if finals:
                    return ds
        except:
            pass
    return (today - timedelta(days=1)).strftime("%Y-%m-%d")

def fetch_finnish_points_for_date(date_str):
    try:
        sch = nhl_schedule(date_str)
    except:
        return []

    dates = sch.get("dates",[])
    if not dates:
        return []

    games = dates[0].get("games",[])
    results = []

    for g in games:
        pk = g.get("gamePk")
        if not pk: continue
        try:
            box = nhl_boxscore(pk)
        except:
            continue

        for side in ("home","away"):
            team = box.get("teams",{}).get(side,{})
            team_name = team.get("team",{}).get("name","")
            players = team.get("players",{}) or {}

            for p in players.values():
                person = p.get("person",{})
                if (person.get("nationality","").upper() != "FIN"):
                    continue

                name = person.get("fullName","")
                stats = p.get("stats",{})

                # Skater
                if "skaterStats" in stats:
                    s = stats["skaterStats"]
                    g_ = int(s.get("goals",0))
                    a_ = int(s.get("assists",0))
                    p_ = g_ + a_
                    plus = s.get("plusMinus",0)
                    toi = s.get("timeOnIce","00:00")
                    shots = s.get("shots")
                    hits = s.get("hits")
                    blocks = s.get("blocked")
                    pim = s.get("penaltyMinutes")
                    fow = s.get("faceOffWins")
                    fot = s.get("faceoffTaken")
                    pp_toi = s.get("powerPlayTimeOnIce","0:00")
                    sh_toi = s.get("shortHandedTimeOnIce","0:00")

                    header = f"{name} ({team_name}) {g_}+{a_}={p_}, ±{plus}, TOI {toi}"
                    notes = []

                    if shots not in [None,""]:
                        notes.append(f"Laukaukset {shots}")
                    if fow not in [None,""] and fot:
                        notes.append(f"Aloitukset {fow}/{fot}")
                    if pim not in [None,"",0]:
                        notes.append(f"Jäähyt {pim} min")
                    if hits not in [None,""]:
                        notes.append(f"Taklaukset {hits}")
                    if blocks not in [None,""]:
                        notes.append(f"Blokit {blocks}")
                    if pp_toi not in ["0:00","",None]:
                        notes.append(f"YV {pp_toi}")
                    if sh_toi not in ["0:00","",None]:
                        notes.append(f"AV {sh_toi}")

                    line = f"• {header}"
                    if notes:
                        line += "\n  " + " | ".join(notes)

                    results.append((p_, name, line))

                # Goalie
                elif "goalieStats" in stats:
                    gk = stats["goalieStats"]
                    sv = int(gk.get("saves",0))
                    sa = int(gk.get("shotsAgainst",0))
                    toi = gk.get("timeOnIce","00:00")
                    svpct = round(sv/sa,3) if sa>0 else "—"

                    header = f"{name} ({team_name})\nMV: {sv}/{sa} torjuntaa, SV% {svpct}, TOI {toi}"
                    notes = []
                    # Extra splits if available
                    for key, label in [
                        ("evenSaves","EV"),
                        ("powerPlaySaves","YV"),
                        ("shortHandedSaves","AV")
                    ]:
                        if gk.get(key) is not None:
                            # we cannot get SA split reliably
                            notes.append(f"{label} {gk.get(key)}")

                    line = f"• {header}"
                    if notes:
                        line += "\n  " + " | ".join(notes)

                    results.append((0, name, line))

    results.sort(key=lambda x: (-x[0], x[1]))    
    return [r[2] for r in results]

def send_nightly_finns_once():
    last_sent = get_setting("last_stats_date")
    target_date = last_completed_nhl_date()
    if last_sent == target_date:
        return

    now = now_local()
    if now.hour > NIGHTLY_STATS_HOUR or (now.hour == NIGHTLY_STATS_HOUR and now.minute >= NIGHTLY_STATS_MINUTE):
        for attempt in range(5):
            try:
                rows = fetch_finnish_points_for_date(target_date)
                if rows:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target_date}\n\n" + "\n\n".join(rows)
                else:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target_date}\n\nEi suomalaispisteitä."
                send_telegram(msg)
                set_setting("last_stats_date", target_date)
                return
            except Exception as e:
                if attempt == 4:
                    send_telegram(f"⚠️ Suomalaisraportti epäonnistui: {e}")
                time.sleep(4*(attempt+1))

###############################################################################
# OPTIONAL TEST (poista tämä kutsu kun testattu)
###############################################################################

def send_test_finns_safe():
    time.sleep(5)
    for attempt in range(5):
        try:
            dt = last_completed_nhl_date()
            rows = fetch_finnish_points_for_date(dt)
            if rows:
                send_telegram("🔧 TESTI – Suomalaisraportti\n\n" + "\n\n".join(rows))
            else:
                send_telegram("🔧 TESTI – Ei suomalaispisteitä.")
            return
        except Exception as e:
            if attempt == 4:
                send_telegram(f"🔧 TESTI VIRHE (5 yritystä): {e}")
            time.sleep(3)

###############################################################################
# MAIN LOOP
###############################################################################

def main():
    init_db()

    # 🔥 KOMMENTOI TÄMÄ POIS, kun testi on valmis
    send_test_finns_safe()

    last_rss = 0
    last_twitter = 0

    while True:
        now_ts = time.time()
        try:
            if now_ts - last_rss > 200:
                poll_rss()
                last_rss = now_ts

            if now_ts - last_twitter > 260:
                poll_twitter()
                last_twitter = now_ts

            send_nightly_finns_once()
            time.sleep(2)

        except Exception as e:
            logging.warning(f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
