# -*- coding: utf-8 -*-
"""
NHL Telegram Bot – Final Full Version
"""

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

# =============================================================================
# CONFIGURATION
# =============================================================================

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki")
DB_PATH = os.getenv("DB_PATH", "nhlbot.db")
ENABLE_COMMANDS = os.getenv("ENABLE_COMMANDS", "true").lower() in ("1", "true", "yes")

UPDATES_POLL_SECONDS = 2
MAX_HOURS = 48
HTTP_TIMEOUT = 20

NIGHTLY_STATS_HOUR = 8
NIGHTLY_STATS_MINUTE = 0

# 00–18 → eilinen, 18–24 → tänään
FINNISH_DAY_BOUNDARY_HOUR = 18

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# =============================================================================
# HTTP SESSION WITH RETRY
# =============================================================================
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
    ad = HTTPAdapter(max_retries=retries)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

SESSION = make_session()

# =============================================================================
# DATABASE
# =============================================================================

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with db_conn() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS seen_items (
            url TEXT PRIMARY KEY,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        c.commit()

def has_seen(url):
    with db_conn() as c:
        return c.execute("SELECT 1 FROM seen_items WHERE url=?", (url,)).fetchone() is not None

def mark_seen(url):
    with db_conn() as c:
        c.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
        c.commit()

def get_setting(key):
    with db_conn() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

def set_setting(key, value):
    with db_conn() as c:
        c.execute("""
            INSERT INTO settings(key,value)
            VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key,value))
        c.commit()

# =============================================================================
# HELPERS
# =============================================================================

def now_local():
    return datetime.now(pytz.timezone(TIMEZONE))

def send_telegram(msg, chat_id=None):
    if not chat_id:
        chat_id = CHAT_ID
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": chat_id, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram error: {e}")

def normalize_url(url):
    try:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(p.query)
        q = [(k,v) for (k,v) in q if not k.lower().startswith(("utm_","fbclid","gclid"))]
        return urllib.parse.urlunparse((p.scheme,p.netloc,p.path,p.params,urllib.parse.urlencode(q), ""))
    except:
        return url

def is_recent(date_str, hours=48):
    if not date_str:
        return False
    try:
        dt = dateparser.parse(date_str)
        if not dt.tzinfo:
            dt = pytz.timezone(TIMEZONE).localize(dt)
        return (now_local() - dt).total_seconds() <= hours * 3600
    except:
        return False

# =============================================================================
# EFFECTIVE DATE LOGIC
# =============================================================================

def nhl_effective_games():
    local = now_local()
    if local.hour < FINNISH_DAY_BOUNDARY_HOUR:
        return (local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return local.strftime("%Y-%m-%d")

# =============================================================================
# RSS
# =============================================================================

def poll_rss():
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                pub = getattr(entry, "published", None) or getattr(entry, "updated", None)
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

# =============================================================================
# TWITTER/X A-VERSION
# =============================================================================

def twitter_get_user_id(username):
    try:
        url = f"https://cdn.syndication.twimg.com/user/by-screen-name/{username}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except:
        return None

def twitter_get_latest_tweets(uid, limit=5):
    try:
        url = f"https://cdn.syndication.twimg.com/timeline/profile/{uid}.json"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        items = data.get("instructions", [])
        tweets = []
        for instr in items:
            if "addEntries" in instr:
                for entry in instr["addEntries"]["entries"]:
                    tw = entry.get("content",{}).get("item",{}).get("content",{}).get("tweet")
                    if tw: tweets.append(tw)
        return tweets[:limit]
    except:
        return []

def poll_twitter():
    for h in TWITTER_USERS:
        try:
            key = f"twitter_userid_{h}"
            uid = get_setting(key)
            if not uid:
                uid = twitter_get_user_id(h)
                if not uid: continue
                set_setting(key, uid)

            tweets = twitter_get_latest_tweets(uid)
            for tw in tweets:
                tid = tw.get("id_str") or tw.get("id")
                text = tw.get("full_text") or tw.get("text") or ""
                created = tw.get("created_at")
                url = f"https://x.com/{h}/status/{tid}"

                if not is_recent(created, MAX_HOURS):
                    continue
                if has_seen(url):
                    continue

                send_telegram(f"🐦 X — {h}\n\n{text}\n{url}")
                mark_seen(url)

        except Exception as e:
            logging.warning(f"Twitter error {h}: {e}")

# =============================================================================
# NHL SCHEDULE + BOX + FINNS
# =============================================================================

def nhl_schedule(date_str):
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={date_str}"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def nhl_boxscore(pk):
    url = f"https://statsapi.web.nhl.com/api/v1/game/{pk}/boxscore"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def fetch_finnish_points_for_date(date_str):
    try:
        sch = nhl_schedule(date_str)
    except:
        return []

    dates = sch.get("dates", [])
    if not dates:
        return []

    games = dates[0].get("games", [])
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
            tname = team.get("team",{}).get("name","")
            players = team.get("players",{}) or {}

            for p in players.values():
                pr = p.get("person",{})
                if pr.get("nationality","").upper() != "FIN":
                    continue

                name = pr.get("fullName","")
                stats = p.get("stats",{})

                if "skaterStats" in stats:
                    s = stats["skaterStats"]
                    g_,a_ = int(s.get("goals",0)), int(s.get("assists",0))
                    p_ = g_+a_
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

                    header = f"{name} ({tname}) {g_}+{a_}={p_}, ±{plus}, TOI {toi}"
                    notes = []
                    if shots not in [None,""]: notes.append(f"Laukaukset {shots}")
                    if fow not in [None,"",0] and fot: notes.append(f"Aloitukset {fow}/{fot}")
                    if pim not in [None,"",0]: notes.append(f"Jäähyt {pim} min")
                    if hits not in [None,"",0]: notes.append(f"Taklaukset {hits}")
                    if blocks not in [None,"",0]: notes.append(f"Blokit {blocks}")
                    if pp_toi not in ["0:00",None,""]: notes.append(f"YV {pp_toi}")
                    if sh_toi not in ["0:00",None,""]: notes.append(f"AV {sh_toi}")

                    line = f"• {header}"
                    if notes: line += "\n  " + " | ".join(notes)
                    results.append((p_, name, line))

                elif "goalieStats" in stats:
                    s = stats["goalieStats"]
                    sv, sa = int(s.get("saves",0)), int(s.get("shotsAgainst",0))
                    toi = s.get("timeOnIce","00:00")
                    svpct = round(sv/sa,3) if sa>0 else "—"
                    header = f"{name} ({tname})\nMV: {sv}/{sa}, SV% {svpct}, TOI {toi}"
                    notes=[]
                    if s.get("evenSaves") is not None:
                        notes.append(f"EV {s.get('evenSaves')}")
                    if s.get("powerPlaySaves") is not None:
                        notes.append(f"YV {s.get('powerPlaySaves')}")
                    if s.get("shortHandedSaves") is not None:
                        notes.append(f"AV {s.get('shortHandedSaves')}")

                    line = f"• {header}"
                    if notes: line += "\n  " + " | ".join(notes)
                    results.append((0,name,line))

    results.sort(key=lambda x:(-x[0], x[1]))
    return [r[2] for r in results]

def list_finns_in_game(box):
    out=[]
    for side in ("home","away"):
        team = box.get("teams",{}).get(side,{})
        for p in team.get("players",{}).values():
            pr = p.get("person",{})
            if pr.get("nationality","").upper()=="FIN":
                out.append(pr.get("fullName"))
    return out

def get_games_with_finns(date_str):
    try:
        sch = nhl_schedule(date_str)
    except:
        return []

    dates = sch.get("dates",[])
    if not dates: return []

    games = dates[0].get("games",[])
    out=[]

    for g in games:
        home = g["teams"]["home"]["team"]["name"]
        away = g["teams"]["away"]["team"]["name"]
        status = g["status"]["detailedState"]
        gd = g["gameDate"]

        try:
            dt = dateparser.parse(gd).astimezone(pytz.timezone(TIMEZONE))
            t = dt.strftime("%H:%M")
        except:
            t="?"

        hsc = g["teams"]["home"]["score"]
        asc = g["teams"]["away"]["score"]

        if status.startswith("Final"):
            line=f"🏁 {home} {hsc} – {asc} {away}"
        elif status in ("Live","In Progress"):
            line=f"🔴 LIVE {home} {hsc} – {asc} {away}"
        else:
            line=f"⏰ {away} @ {home} klo {t}"

        try:
            box = nhl_boxscore(g["gamePk"])
            finns = list_finns_in_game(box)
            if finns:
                line += "\nSuomalaiset: " + ", ".join(finns)
        except:
            pass

        out.append(line)
    return out

# =============================================================================
# PLAYER SEARCH
# =============================================================================

def search_player_by_name(q):
    try:
        url=f"https://statsapi.web.nhl.com/api/v1/people/?names={q}"
        r=SESSION.get(url,timeout=HTTP_TIMEOUT)
        return r.json().get("people",[])
    except:
        return []

def get_player_stats(pid):
    try:
        url=f"https://statsapi.web.nhl.com/api/v1/people/{pid}/stats?stats=statsSingleSeason&season=20242025"
        r=SESSION.get(url,timeout=HTTP_TIMEOUT)
        splits=r.json().get("stats",[{}])[0].get("splits",[])
        return splits[0].get("stat",{}) if splits else {}
    except:
        return {}

# =============================================================================
# TELEGRAM COMMANDS
# =============================================================================

def tg_get_updates(offset):
    try:
        url=f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        params={"timeout":20}
        if offset: params["offset"]=offset
        r=SESSION.get(url,params=params,timeout=HTTP_TIMEOUT)
        if r.status_code!=200: return []
        return r.json().get("result",[])
    except:
        return []

def handle_command(cmd,chat_id):
    c=cmd.strip().split()[0].lower()

    if c.startswith("/ping"):
        send_telegram("pong",chat_id)
        return

    if c.startswith("/suomalaiset"):
        d=nhl_effective_games()
        rows=fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\n\n"+"\n\n".join(rows),chat_id)
        else:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\nEi suomalaispisteitä.",chat_id)
        return

    if c.startswith("/stats") or c.startswith("/test"):
        d=nhl_effective_games()
        rows=fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"📊 Suomalaisraportti — {d}\n\n"+"\n\n".join(rows),chat_id)
        else:
            send_telegram(f"📊 Suomalaisraportti — {d}\nEi suomalaispisteitä.",chat_id)
        return

    if c.startswith("/players"):
        parts=cmd.split(maxsplit=1)
        if len(parts)<2:
            send_telegram("Käyttö: /players <nimi>",chat_id)
            return

        q=parts[1].strip()
        players=search_player_by_name(q)
        if not players:
            send_telegram(f"Ei pelaajia haulla: {q}",chat_id)
            return

        p=players[0]
        pid=p.get("id")
        name=p.get("fullName","")
        team=p.get("currentTeam",{}).get("name","")
        pos=p.get("primaryPosition",{}).get("name","")
        stats=get_player_stats(pid)
        g,a=stats.get("goals",0),stats.get("assists",0)
        gp=stats.get("games",0)

        msg=(f"📌 Pelaaja: {name}\n"
             f"Joukkue: {team}\n"
             f"Pelipaikka: {pos}\n\n"
             f"Pelit: {gp}\n"
             f"Pisteet: {g}+{a}={g+a}")
        send_telegram(msg,chat_id)
        return

    if c.startswith("/games"):
        d=nhl_effective_games()
        lines=get_games_with_finns(d)
        if not lines:
            send_telegram("Ei otteluita.",chat_id)
        else:
            send_telegram("📅 NHL-ottelut:\n\n"+"\n\n".join(lines),chat_id)
        return

    send_telegram("Tuntematon komento. Kokeile: /ping /players /games /suomalaiset /stats /test",chat_id)

def poll_commands(state):
    if not ENABLE_COMMANDS:
        return
    offset=state.get("tg_offset")
    updates=tg_get_updates(offset)
    if not updates:
        return

    maxid=offset or 0
    for upd in updates:
        uid=upd.get("update_id",0)
        if uid>maxid: maxid=uid

        msg=upd.get("message") or upd.get("edited_message")
        if not msg: continue

        chat_id=msg.get("chat",{}).get("id")
        text=msg.get("text","")

        if text.startswith("/"):
            handle_command(text,chat_id)

    state["tg_offset"]=maxid+1

# =============================================================================
# AUTOMATIC FINNISH REPORT 08:00
# =============================================================================

def send_nightly_finns_once():
    last=get_setting("last_stats_date")
    target=nhl_effective_games()
    if last==target: return

    now=now_local()
    if now.hour>NIGHTLY_STATS_HOUR or (now.hour==NIGHTLY_STATS_HOUR and now.minute>=NIGHTLY_STATS_MINUTE):
        for attempt in range(5):
            try:
                rows=fetch_finnish_points_for_date(target)
                if rows:
                    msg=f"🇫🇮 Viime yön suomalaiset — {target}\n\n"+"\n\n".join(rows)
                else:
                    msg=f"🇫🇮 Viime yön suomalaiset — {target}\nEi suomalaispisteitä."
                send_telegram(msg)
                set_setting("last_stats_date",target)
                return
            except Exception as e:
                if attempt==4:
                    send_telegram(f"⚠️ Suomalaisraportti epäonnistui: {e}")
                time.sleep(4*(attempt+1))

# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    init_db()
    logging.info("NHL Bot is running...")

    last_rss=0
    last_tw=0
    last_cmd=0

    state={"tg_offset":None}

    while True:
        now_ts=time.time()
        try:
            if now_ts-last_cmd>=UPDATES_POLL_SECONDS:
                poll_commands(state)
                last_cmd=now_ts

            if now_ts-last_rss>=200:
                poll_rss()
                last_rss=now_ts

            if now_ts-last_tw>=260:
                poll_twitter()
                last_tw=now_ts

            send_nightly_finns_once()

            time.sleep(1)

        except Exception as e:
            logging.warning(f"Main loop error: {e}")
            time.sleep(5)

# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    main()
