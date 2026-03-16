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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

###############################################################################
# CONFIG
###############################################################################
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

###############################################################################
# LOGGING
###############################################################################
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

###############################################################################
# HTTP SESSION WITH RETRY (STABLE FOR RAILWAY)
###############################################################################
def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
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
        conn.execute("CREATE TABLE IF NOT EXISTS seen_items (url TEXT PRIMARY KEY, first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        conn.commit()

def has_seen(url: str) -> bool:
    with db_conn() as conn:
        return conn.execute("SELECT 1 FROM seen_items WHERE url=?", (url,)).fetchone() is not None

def mark_seen(url: str):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
        conn.commit()

def get_setting(key: str):
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

def set_setting(key: str, value: str):
    with db_conn() as conn:
        conn.execute("INSERT INTO settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
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
        q = [(k, v) for k, v in q if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
        new_q = urllib.parse.urlencode(q)
        return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, ""))
    except:
        return url

def send_telegram(msg: str, chat_id=None):
    if not chat_id:
        chat_id = CHAT_ID
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": chat_id, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram error: {e}")

def is_recent(date_str: str, hours=48):
    if not date_str:
        return False
    try:
        dt = dateparser.parse(date_str)
        if not dt.tzinfo:
            dt = pytz.timezone(TIMEZONE).localize(dt)
        return (now_local() - dt).total_seconds() <= hours * 3600
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
                pub = getattr(entry, "published", None) or getattr(entry, "updated", None)
                if not is_recent(pub, MAX_HOURS):
                    continue
                link = normalize_url(getattr(entry, "link", ""))
                title = getattr(entry, "title", "").strip()
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
# TWITTER A-VERSION (NO API, NO SNSCRAPE)
###############################################################################
def twitter_get_user_id(username: str):
    try:
        url = f"https://cdn.syndcation.twimg.com/user/by-screen-name/{username}"  # note: typo fixed below
        url = f"https://cdn.syndication.twimg.com/user/by-screen-name/{username}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except:
        return None

def twitter_get_latest_tweets(user_id: str, limit=5):
    try:
        url = f"https://cdn.syndication.twimg.com/timeline/profile/{user_id}.json"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        items = data.get("instructions", [])
        tweets = []
        for instr in items:
            if "addEntries" in instr:
                for entry in instr["addEntries"]["entries"]:
                    tw = entry.get("content", {}).get("item", {}).get("content", {}).get("tweet")
                    if tw:
                        tweets.append(tw)
        return tweets[:limit]
    except:
        return []

def poll_twitter():
    for handle in TWITTER_USERS:
        try:
            key = f"twitter_userid_{handle}"
            user_id = get_setting(key)
            if not user_id:
                user_id = twitter_get_user_id(handle)
                if not user_id:
                    continue
                set_setting(key, user_id)
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
                send_telegram(f"🐦 X — {handle}\n\n{text}\n{url}")
                mark_seen(url)
        except Exception as e:
            logging.warning(f"Twitter error: {e}")

###############################################################################
# NHL FINNISH STATS, GAMES, PLAYER SEARCH
###############################################################################
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

def last_completed_nhl_date():
    today = now_local().date()
    for d in [today - timedelta(days=1), today - timedelta(days=2)]:
        ds = d.strftime("%Y-%m-%d")
        try:
            sch = nhl_schedule(ds)
            if sch.get("dates"):
                games = sch["dates"][0].get("games", [])
                finals = [g for g in games if g.get("status", {}).get("detailedState", "").startswith("Final")]
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
    dates = sch.get("dates", [])
    if not dates:
        return []
    results = []
    games = dates[0].get("games", [])
    for g in games:
        pk = g.get("gamePk")
        if not pk:
            continue
        try:
            box = nhl_boxscore(pk)
        except:
            continue
        for side in ("home", "away"):
            team = box.get("teams", {}).get(side, {})
            tname = team.get("team", {}).get("name", "")
            players = team.get("players", {}) or {}
            for pdata in players.values():
                person = pdata.get("person", {})
                if person.get("nationality", "").upper() != "FIN":
                    continue
                name = person.get("fullName", "")
                stats = pdata.get("stats", {})
                # Skater
                if "skaterStats" in stats:
                    s = stats["skaterStats"]
                    g_, a_ = int(s.get("goals",0)), int(s.get("assists",0))
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
                # Goalie
                elif "goalieStats" in stats:
                    gk = stats["goalieStats"]
                    sv, sa = int(gk.get("saves",0)), int(gk.get("shotsAgainst",0))
                    toi = gk.get("timeOnIce","00:00")
                    svpct = round(sv/sa,3) if sa>0 else "—"
                    header = f"{name} ({tname})\nMV: {sv}/{sa} torjuntaa, SV% {svpct}, TOI {toi}"
                    notes = []
                    for key,label in [("evenSaves","EV"),("powerPlaySaves","YV"),("shortHandedSaves","AV")]:
                        if gk.get(key) is not None:
                            notes.append(f"{label} {gk.get(key)}")
                    line = f"• {header}"
                    if notes: line += "\n  " + " | ".join(notes)
                    results.append((0, name, line))
    results.sort(key=lambda x:(-x[0],x[1]))
    return [r[2] for r in results]

def search_player_by_name(query: str):
    try:
        url = f"https://statsapi.web.nhl.com/api/v1/people/?names={query}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        return r.json().get("people", []) if r.status_code==200 else []
    except:
        return []

def get_player_stats(pid: int):
    try:
        url = f"https://statsapi.web.nhl.com/api/v1/people/{pid}/stats?stats=statsSingleSeason&season=20242025"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        splits = r.json().get("stats",[{}])[0].get("splits",[])
        return splits[0].get("stat",{}) if splits else {}
    except:
        return {}

def get_today_games():
    today = now_local().strftime("%Y-%m-%d")
    try:
        url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={today}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        dates = r.json().get("dates",[])
        return dates[0].get("games",[]) if dates else []
    except:
        return []

###############################################################################
# TELEGRAM COMMANDS (Long Poll)
###############################################################################
def tg_get_updates(offset):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        params = {"timeout": 20}
        if offset is not None:
            params["offset"] = offset
        r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        return r.json().get("result", []) if r.status_code==200 else []
    except Exception as e:
        logging.warning(f"getUpdates error: {e}")
        return []

def handle_command(cmd: str, chat_id):
    c = cmd.strip().split()[0].lower()
    # ping
    if c.startswith("/ping"):
        send_telegram("pong", chat_id)
        return
    # /suomalaiset
    if c.startswith("/suomalaiset"):
        try:
            d = last_completed_nhl_date()
            rows = fetch_finnish_points_for_date(d)
            if rows:
                send_telegram(f"🇫🇮 Suomalaiset — {d}\n\n" + "\n\n".join(rows), chat_id)
            else:
                send_telegram(f"🇫🇮 Suomalaiset — {d}\nEi suomalaispisteitä.", chat_id)
        except Exception as e:
            send_telegram(f"Virhe: {e}", chat_id)
        return
    # /test and /stats
    if c.startswith("/test") or c.startswith("/stats"):
        try:
            d = last_completed_nhl_date()
            rows = fetch_finnish_points_for_date(d)
            if rows:
                send_telegram(f"🔧 Testi — {d}\n\n" + "\n\n".join(rows), chat_id)
            else:
                send_telegram(f"🔧 Testi — {d}\nEi suomalaispisteitä.", chat_id)
        except Exception as e:
            send_telegram(f"Virhe: {e}", chat_id)
        return
    # /players
    if c.startswith("/players"):
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram("Käyttö: /players <nimi>", chat_id)
            return
        q = parts[1]
        players = search_player_by_name(q)
        if not players:
            send_telegram(f"Ei pelaajia: {q}", chat_id)
            return
        p = players[0]
        pid = p.get("id")
        name = p.get("fullName","")
        team = p.get("currentTeam",{}).get("name","")
        pos = p.get("primaryPosition",{}).get("name","")
        stats = get_player_stats(pid)
        g = stats.get("goals",0)
        a = stats.get("assists",0)
        pts = g + a
        gp = stats.get("games",0)
        msg = f"📌 Pelaaja: {name}\nJoukkue: {team}\nPelipaikka: {pos}\n\nPelit: {gp}\nPisteet: {g}+{a}={pts}"
        send_telegram(msg, chat_id)
        return
    # /games
    if c.startswith("/games"):
        games = get_today_games()
        if not games:
            send_telegram("Ei tämän päivän NHL-otteluita.", chat_id)
            return
        lines = []
        for g in games:
            home = g["teams"]["home"]["team"]["name"]
            away = g["teams"]["away"]["team"]["name"]
            status = g["status"]["detailedState"]
            gd = g.get("gameDate")
            try:
                dt = dateparser.parse(gd).astimezone(pytz.timezone(TIMEZONE))
                t = dt.strftime("%H:%M")
            except:
                t = "?"
            if status.startswith("Final"):
                score = f"{home} {g['teams']['home']['score']} – {g['teams']['away']['score']} {away}"
                lines.append(f"🏁 {score}")
            elif status in ("Live","In Progress"):
                score = f"{home} {g['teams']['home']['score']} – {g['teams']['away']['score']} {away}"
                lines.append(f"🔴 LIVE {score}")
            else:
                lines.append(f"⏰ {away} @ {home} klo {t}")
        send_telegram("📅 Tämän päivän ottelut:\n\n" + "\n".join(lines), chat_id)
        return
    send_telegram("Tuntematon komento. Käytä /ping /test /stats /players /games /suomalaiset", chat_id)

def poll_commands(state):
    if not ENABLE_COMMANDS:
        return
    offset = state.get("tg_offset")
    updates = tg_get_updates(offset)
    if not updates:
        return
    max_id = offset or 0
    for upd in updates:
        try:
            uid = upd.get("update_id",0)
            if uid > max_id:
                max_id = uid
            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            chat_id = msg.get("chat",{}).get("id",CHAT_ID)
            text = msg.get("text","")
            if text.startswith("/"):
                handle_command(text, chat_id)
        except Exception as e:
            logging.warning(f"poll_commands error: {e}")
    state["tg_offset"] = max_id + 1

###############################################################################
# TEST FUNCTION (OPTIONAL)
###############################################################################
def send_test_finns_safe():
    time.sleep(5)
    for attempt in range(5):
        try:
            d = last_completed_nhl_date()
            rows = fetch_finnish_points_for_date(d)
            if rows:
                send_telegram("🔧 TESTI — Suomalaisraportti\n\n" + "\n\n".join(rows))
            else:
                send_telegram("🔧 TESTI — Ei suomalaispisteitä.")
            return
        except Exception as e:
            if attempt == 4:
                send_telegram(f"🔧 TESTI VIRHE: {e}")
            time.sleep(3)

###############################################################################
# MAIN LOOP
###############################################################################
def main():
    init_db()
    # Optionally enable this for testing:
    # send_test_finns_safe()
    last_rss = 0
    last_tw = 0
    last_cmd = 0
    state = {"tg_offset": None}
    while True:
        now_ts = time.time()
        try:
            if now_ts - last_cmd >= UPDATES_POLL_SECONDS:
                poll_commands(state)
                last_cmd = now_ts
            if now_ts - last_rss > 200:
                poll_rss()
                last_rss = now_ts
            if now_ts - last_tw > 260:
                poll_twitter()
                last_tw = now_ts
            send_nightly_finns_once()
            time.sleep(1)
        except Exception as e:
            logging.warning(f"Main loop error: {e}")
            time.sleep(5)

###############################################################################
# NIGHTLY FINNS
###############################################################################
def send_nightly_finns_once():
    last_sent = get_setting("last_stats_date")
    target_date = last_completed_nhl_date()
    if last_sent == target_date:
        return
    n = now_local()
    if n.hour > NIGHTLY_STATS_HOUR or (n.hour == NIGHTLY_STATS_HOUR and n.minute >= NIGHTLY_STATS_MINUTE):
        for attempt in range(5):
            try:
                rows = fetch_finnish_points_for_date(target_date)
                if rows:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target_date}\n\n" + "\n\n".join(rows)
                else:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target_date}\nEi suomalaispisteitä."
                send_telegram(msg)
                set_setting("last_stats_date", target_date)
                return
            except Exception as e:
                if attempt == 4:
                    send_telegram(f"⚠️ Suomalaisraportti epäonnistui: {e}")
                time.sleep(4*(attempt+1))

if __name__ == "__main__":
    main()
