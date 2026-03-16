# -*- coding: utf-8 -*-
"""
NHL Telegram Bot – Modern API version (2026)
LOHKO 1 / 3 — Config, Retry Session, DB, Helpers
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

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki")
DB_PATH = os.getenv("DB_PATH", "nhlbot.db")
ENABLE_COMMANDS = os.getenv("ENABLE_COMMANDS", "true").lower() in ("1", "true", "yes")

UPDATES_POLL_SECONDS = 2
HTTP_TIMEOUT = 20

# Twitter A‑versio (ei API-avainta)
TWITTER_USERS = [
    "FriedgeHNIC",
    "reporterchris",
    "DarrenDreger",
    "PierreVLeBrun",
    "frank_seravalli",
    "RussoHockey",
]

# RSS feeds
RSS_FEEDS = [
    "https://www.iltalehti.fi/rss/nhl.xml",
    "https://www.is.fi/rss/nhl.xml",
    "https://www.nhl.com/rss/news.xml",
    "https://feeds.yle.fi/uutiset/v1/majorHeadlines/urheilu.rss",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# HTTP SESSION WITH RETRY
# ---------------------------------------------------------------------------
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

# ---------------------------------------------------------------------------
# SQLITE
# ---------------------------------------------------------------------------
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with db_conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS seen_items (
                url TEXT PRIMARY KEY,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        c.commit()

def has_seen(url: str) -> bool:
    with db_conn() as c:
        return c.execute("SELECT 1 FROM seen_items WHERE url=?", (url,)).fetchone() is not None

def mark_seen(url: str):
    with db_conn() as c:
        c.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
        c.commit()

def get_setting(key: str):
    with db_conn() as c:
        row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

def set_setting(key: str, value: str):
    with db_conn() as c:
        c.execute("""
            INSERT INTO settings(key,value)
            VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        c.commit()

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def now_local():
    return datetime.now(pytz.timezone(TIMEZONE))

def normalize_url(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(p.query)
        q = [(k, v) for (k, v) in q if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
        return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, p.params, urllib.parse.urlencode(q), ""))
    except:
        return url
        
def nhl_player_stats():

    now = now_local()
    yr = now.year

    # NHL season logic
    if now.month < 7:
        season = f"{yr-1}{yr}"
    else:
        season = f"{yr}{yr+1}"

    url = "https://api.nhle.com/stats/rest/en/skater/summary"

    params = {
        "isAggregate": "false",
        "isGame": "false",
        "sort": "[{\"property\":\"points\",\"direction\":\"DESC\"}]",
        "start": 0,
        "limit": 30,
        "cayenneExp": f"seasonId={season} and gameTypeId=2"
    }

    r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    data = r.json().get("data", [])

    players = []

    for p in data:

        name = f"{p.get('skaterFullName','')}".strip()

        players.append({
            "name": name,
            "team": p.get("teamAbbrevs", ""),
            "gp": p.get("gamesPlayed", 0),
            "g": p.get("goals", 0),
            "a": p.get("assists", 0),
            "p": p.get("points", 0),
            "pm": p.get("plusMinus", 0),
            "pim": p.get("penaltyMinutes", 0),
            "toi": p.get("timeOnIcePerGame", "")
        })

    return players
    
def send_telegram(msg: str, chat_id=None):
    if not chat_id:
        chat_id = CHAT_ID
    if not TOKEN or not chat_id:
        logging.warning("Missing TOKEN/CHAT_ID")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": chat_id, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram error: {e}")

def is_recent(pub_time):
    try:
        dt = dateparser.parse(pub_time)

        if not dt.tzinfo:
            dt = pytz.utc.localize(dt)

        limit = datetime.now(pytz.utc) - timedelta(hours=48)
        return dt > limit
    except:
        return False

def nhl_effective_date():
    """
    NHL effective date (Suomensääntö):
    Aamuun 17:59 asti katsotaan eilinen, klo 18 jälkeen nykyinen päivä.
    """
    local = now_local()
    if local.hour < 18:
        return (local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return local.strftime("%Y-%m-%d")

FINNISH_PLAYERS = {
    8478402: "Sebastian Aho",
    8477503: "Mikko Rantanen",
    8476883: "Aleksander Barkov",
    8476459: "Patrik Laine",
    8477934: "Roope Hintz",
    8478439: "Esa Lindell",
    8477462: "Miro Heiskanen",
    8478010: "Kaapo Kakko",
    8478013: "Joel Armia",
    8477493: "Teuvo Teräväinen",
    8477500: "Artturi Lehkonen",
    8476476: "Erik Haula",
    8479376: "Anton Lundell",
    8479314: "Juuso Pärssinen",
    8479420: "Ukko-Pekka Luukkonen",
}

# ---------------------------------------------------------------------------
# RSS (48h filter, duplicate filtered)
# ---------------------------------------------------------------------------

def poll_rss():
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries:

                pub = getattr(entry, "published", None) or getattr(entry, "updated", None)
                title = getattr(entry, "title", "").strip()
                link = normalize_url(getattr(entry, "link", ""))

                if not pub or not title or not link:
                    continue

                # 48h filter
                if not is_recent(pub):
                    continue

                # YLE NHL filter
                if "yle.fi" in link:
                    if "nhl" not in title.lower() and "nhl" not in getattr(entry, "summary", "").lower():
                        continue

                if has_seen(link):
                    continue

                send_telegram(f"🏒 NHL-UUTINEN\n\n{title}\n{link}")
                mark_seen(link)

        except Exception as e:
            logging.warning(f"RSS error: {e}")
            time.sleep(2)
# ---------------------------------------------------------------------------
# TWITTER / X — A-version (no API key)
# ---------------------------------------------------------------------------
def twitter_get_user_id(username):
    try:
        url = "https://cdn.syndication.twimg.com/user/by-screen-name/{0}".format(username)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except:
        return None

def twitter_get_latest_tweets(user_id, limit=5):
    try:
        url = "https://cdn.syndication.twimg.com/timeline/profile/{0}.json".format(user_id)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        tweets = []
        for instr in data.get("instructions", []):
            if "addEntries" in instr:
                for ent in instr["addEntries"]["entries"]:
                    tw = ent.get("content", {}).get("item", {}).get("content", {}).get("tweet")
                    if tw:
                        tweets.append(tw)
        return tweets[:limit]
    except:
        return []

def poll_twitter():
    for handle in TWITTER_USERS:
        try:
            key = "twitter_uid_{0}".format(handle)
            uid = get_setting(key)
            if not uid:
                uid = twitter_get_user_id(handle)
                if uid:
                    set_setting(key, uid)
            if not uid:
                continue

            tweets = twitter_get_latest_tweets(uid)
            for tw in tweets:
                tid = tw.get("id_str") or tw.get("id")
                text = tw.get("full_text") or tw.get("text") or ""
                url = "https://x.com/{0}/status/{1}".format(handle, tid)
                if has_seen(url):
                    continue
                send_telegram("X ({0})\n\n{1}\n{2}".format(handle, text, url))
                mark_seen(url)
        except Exception as e:
            logging.warning("Twitter error: {0}".format(e))

# ---------------------------------------------------------------------------
# NHL MODERN ENDPOINTS
# ---------------------------------------------------------------------------

def nhl_schedule(date_str):
    url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    games = []

    if "gameWeek" in data:
        for day in data["gameWeek"]:
            if day.get("date") == date_str:
                games.extend(day.get("games", []))

    elif "games" in data:
        games = data["games"]

    return games

def nhl_play_by_play(game_pk):
    url = "https://api-web.nhle.com/v1/gamecenter/{0}/play-by-play".format(game_pk)
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_finnish_points(date_str):

    games = nhl_schedule(date_str)
    stats = {}

    for g in games:

        game_pk = g.get("id") or g.get("gamePk") or g.get("gameId")
        if not game_pk:
            continue

        try:
            pbp = nhl_play_by_play(int(game_pk))
            goals = extract_goals(pbp)
        except:
            continue

        for ev in goals:

            scorer = int(ev["scorer"]) if ev["scorer"] else None
            a1 = int(ev["a1"]) if ev["a1"] else None
            a2 = int(ev["a2"]) if ev["a2"] else None

            if scorer in FINNISH_PLAYERS:
                stats.setdefault(scorer, {"g":0,"a":0})
                stats[scorer]["g"] += 1

            if a1 in FINNISH_PLAYERS:
                stats.setdefault(a1, {"g":0,"a":0})
                stats[a1]["a"] += 1

            if a2 in FINNISH_PLAYERS:
                stats.setdefault(a2, {"g":0,"a":0})
                stats[a2]["a"] += 1

    return stats

def extract_goals(pbp_json):
    plays = pbp_json.get("plays", [])
    goals = []
    for p in plays:
        if p.get("typeDescKey") == "goal":
            d = p.get("details", {})
            goals.append({
                "time": p.get("timeInPeriod", "?"),
                "period": p.get("periodDescriptor", {}).get("number", 0),
                "scorer": d.get("scoringPlayerId"),
                "a1": d.get("assist1PlayerId"),
                "a2": d.get("assist2PlayerId")
            })
    return goals

def get_player_name(player_id):
    try:
        url = "https://api-web.nhle.com/v1/player/{0}/landing".format(player_id)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return "Player {0}".format(player_id)
        data = r.json()
        fn = data.get("firstName", {}).get("default", "")
        ln = data.get("lastName", {}).get("default", "")
        full = (fn + " " + ln).strip()
        return full if full else "Player {0}".format(player_id)
    except:
        return "Player {0}".format(player_id)

def calculate_points(goal_events):
    pts = {}
    for g in goal_events:
        s = g["scorer"]
        if s:
            pts.setdefault(s, {"g": 0, "a": 0})
            pts[s]["g"] += 1
        if g["a1"]:
            pts.setdefault(g["a1"], {"g": 0, "a": 0})
            pts[g["a1"]]["a"] += 1
        if g["a2"]:
            pts.setdefault(g["a2"], {"g": 0, "a": 0})
            pts[g["a2"]]["a"] += 1
    return pts

def get_team_full_name(team_block):
    name = team_block.get("commonName", {}).get("default")
    if name:
        return name
    return team_block.get("abbrev", "Team")

def nhl_standings():

    url = "https://api-web.nhle.com/v1/standings/now"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    rows = data.get("standings", [])

    divisions = {}

    for row in rows:

        div = row.get("divisionName")
        if not div:
            continue

        team = row.get("teamName", {}).get("default", "")
        pts = row.get("points", 0)
        w = row.get("wins", 0)
        l = row.get("losses", 0)
        ot = row.get("otLosses", 0)

        divisions.setdefault(div, []).append((team, pts, w, l, ot))

    for div in divisions:
        divisions[div].sort(key=lambda x: -x[1])

    return divisions

def search_player(query):
    try:
        url = "https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=20&q={0}".format(query)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json().get("items", [])
    except:
        return []

def get_player_stats(player_id):
    now = now_local()
    yr = now.year
    season = "{0}{1}".format(yr-1, yr) if now.month < 7 else "{0}{1}".format(yr, yr+1)

    url = "https://api-web.nhle.com/v1/skaters/{0}?site=en_nhl&gameType=2".format(season)
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        return None

    for p in r.json():
        if int(p.get("playerId", -1)) == int(player_id):
            return {
                "games": p.get("gamesPlayed", 0),
                "goals": p.get("goals", 0),
                "assists": p.get("assists", 0),
                "points": p.get("points", 0)
            }
    return None

def format_game_output(game, goal_events):
    home = get_team_full_name(game.get("homeTeam", {}))
    away = get_team_full_name(game.get("awayTeam", {}))
    hsc = game.get("homeTeam", {}).get("score")
    asc = game.get("awayTeam", {}).get("score")

    if hsc is not None and asc is not None:
        header = "{0} {1} - {2} {3}".format(home, hsc, asc, away)
    else:
        start_utc = game.get("startTimeUTC") or game.get("gameDate")
        try:
            dt = dateparser.parse(start_utc).astimezone(pytz.timezone(TIMEZONE))
            t = dt.strftime("%H:%M")
        except:
            t = "?"
        header = "{0} @ {1} klo {2}".format(away, home, t)

    if goal_events:
        lines = ["Maalit:"]
        for ev in goal_events:
            scorer = get_player_name(ev["scorer"])
            a1 = get_player_name(ev["a1"]) if ev["a1"] else None
            a2 = get_player_name(ev["a2"]) if ev["a2"] else None

            if a1 and a2:
                assist = "{0}, {1}".format(a1, a2)
            elif a1:
                assist = a1
            else:
                assist = ""

            if assist:
                lines.append(" • {0} {1} ({2})".format(ev["time"], scorer, assist))
            else:
                lines.append(" • {0} {1}".format(ev["time"], scorer))

        header += "\n" + "\n".join(lines)

    pts = calculate_points(goal_events)
    if pts:
        lines = ["Pistemiehet:"]
        arr = []
        for pid, res in pts.items():
            g = res["g"]
            a = res["a"]
            p = g + a
            name = get_player_name(pid)
            arr.append((p, name, g, a))
        arr.sort(key=lambda x: (-x[0], x[1]))
        for p, name, g, a in arr:
            lines.append(" • {0} {1}+{2}={3}".format(name, g, a, p))
        header += "\n\n" + "\n".join(lines)

    return header

# ---------------------------------------------------------------------------
# TELEGRAM POLLING
# ---------------------------------------------------------------------------
def tg_get_updates(offset):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        params = {"timeout": 20}
        if offset is not None:
            params["offset"] = offset
        r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        return r.json().get("result", [])
    except Exception as e:
        logging.warning(f"getUpdates error: {e}")
        return []


# ---------------------------------------------------------------------------
# COMMAND HANDLER
# ---------------------------------------------------------------------------

def handle_command(text, chat_id):

    c = text.lower().strip()

    # /ping
    if c == "/ping":
        send_telegram("Botti toimii 👍", chat_id)
        return


    # /games
    if c == "/games":

        date_str = nhl_effective_date()
        games = nhl_schedule(date_str)

        if not games:
            send_telegram("Ei pelejä tänään.", chat_id)
            return

        lines = ["🏒 NHL pelit:\n"]

        for g in games:

            away = g["awayTeam"]["abbrev"]
            home = g["homeTeam"]["abbrev"]

            if g.get("gameState") == "OFF":
                score = f"{g['awayTeam']['score']} - {g['homeTeam']['score']}"
            else:
                score = "vs"

            lines.append(f"{away} {score} {home}")

        send_telegram("\n".join(lines), chat_id)
        return


    # /players
    if c.startswith("/players"):

        parts = text.split(" ", 1)

        if len(parts) < 2:
            send_telegram("Käyttö: /players nimi", chat_id)
            return

        name = parts[1]

        players = search_players(name)

        if not players:
            send_telegram("Pelaajaa ei löytynyt.", chat_id)
            return

        lines = ["🔎 Pelaajahaku:\n"]

        for p in players[:10]:
            lines.append(f"{p['name']} ({p['team']})")

        send_telegram("\n".join(lines), chat_id)
        return


    # /standings
    if c == "/standings":

        divs = nhl_standings()

        if not divs:
            send_telegram("Standings-tietoja ei saatu.", chat_id)
            return

        lines = ["🏆 NHL divisioonat:\n"]

        for div, teams in divs.items():

            lines.append(div)

            for team, pts, w, l, ot in teams:

                lines.append(f"• {team} {pts}p ({w}-{l}-{ot})")

            lines.append("")

        send_telegram("\n".join(lines), chat_id)
        return


    # /suomalaiset
    if c == "/suomalaiset":

        date_str = nhl_effective_date()
        stats = get_finnish_points(date_str)

        if not stats:
            send_telegram("Ei suomalaispisteitä viime yön peleissä.", chat_id)
            return

        lines = ["🇫🇮 Suomalaisten pisteet viime yön NHL-peleissä:\n"]

        for pid, s in stats.items():

            name = FINNISH_PLAYERS.get(pid, str(pid))
            g = s["g"]
            a = s["a"]
            p = g + a

            lines.append(f"• {name} {g}+{a}={p}")

        send_telegram("\n".join(lines), chat_id)
        return

    # /top30
    if c == "/top30":

    players = nhl_player_stats()

    if not players:
        send_telegram("Tilastoja ei saatu.", chat_id)
        return

    lines = ["🏒 NHL TOP30 pistemiehet\n"]
    lines.append("Nimi | GP G A P +/- PIM TOI\n")

    for i, p in enumerate(players, 1):

        lines.append(
            f"{i}. {p['name']} ({p['team']}) "
            f"{p['gp']} {p['g']} {p['a']} {p['p']} "
            f"{p['pm']} {p['pim']} {p['toi']}"
        )

    send_telegram("\n".join(lines), chat_id)
    return


    # /suomipisteet
    if c == "/suomipisteet":

        players = nhl_player_stats()

        fins = [p for p in players if p["name"] in FINNISH_NAMES]

        if not fins:
            send_telegram("Suomalaistilastoja ei löytynyt.", chat_id)
            return

        lines = ["🇫🇮 Suomalaisten NHL-tilastot\n"]
        lines.append("Nimi | GP G A P +/- PIM TOI")

        fins = sorted(fins, key=lambda x: -x["p"])

        for p in fins:

            lines.append(
                f"{p['name']} ({p['team']}) "
                f"{p['gp']} {p['g']} {p['a']} {p['p']} "
                f"{p['pm']} {p['pim']} {p['toi']}"
            )

        send_telegram("\n".join(lines), chat_id)
        return


    # unknown command
    send_telegram(
        "Tuntematon komento.\n"
        "Komennot:\n"
        "/games\n"
        "/players nimi\n"
        "/standings\n"
        "/suomalaiset\n"
        "/top30\n"
        "/suomipisteet\n"
        "/ping",
        chat_id
    ) 

# ---------------------------------------------------------------------------
# POLL COMMANDS
# ---------------------------------------------------------------------------
def poll_commands(state):
    if not ENABLE_COMMANDS:
        return

    offset = state.get("tg_offset")
    updates = tg_get_updates(offset)
    if not updates:
        return

    maxid = offset or 0

    for upd in updates:
        try:
            uid = upd.get("update_id", 0)
            if uid > maxid:
                maxid = uid

            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue

            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "")

            if text.startswith("/"):
                handle_command(text, chat_id)

        except Exception as e:
            logging.warning(f"poll_commands error: {e}")

    state["tg_offset"] = maxid + 1


# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------
def main():
    init_db()
    logging.info("NHL Modern Bot is running...")

    last_rss = 0
    last_tw = 0
    last_cmd = 0

    state = {"tg_offset": None}

    while True:
        now_ts = time.time()

        try:
            # Commands
            if now_ts - last_cmd >= UPDATES_POLL_SECONDS:
                poll_commands(state)
                last_cmd = now_ts

            # RSS
            if now_ts - last_rss >= 200:
                poll_rss()
                last_rss = now_ts

            # Twitter
            if now_ts - last_tw >= 260:
                poll_twitter()
                last_tw = now_ts

            time.sleep(1)

        except Exception as e:
            logging.warning(f"Main loop error: {e}")
            time.sleep(5)


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
