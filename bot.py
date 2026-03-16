# -*- coding: utf-8 -*-
"""
NHL Telegram Bot – Modern API (2026)
Block 1/3 — Config, Retry Session, DB, Helpers
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
MAX_HOURS = 48
HTTP_TIMEOUT = 20

# 08:00 morning report
NIGHTLY_STATS_HOUR = 8
NIGHTLY_STATS_MINUTE = 0

# Finland split: 00–17:59 → previous day; 18–23:59 → today
FINNISH_DAY_BOUNDARY_HOUR = 18

# Twitter handles (A-version; public JSON)
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
    "https://www.tsn.ca/nhl/rss.xml",
    "https://www.sportsnet.ca/feed/nhl/",
    "https://www.espn.com/espn/rss/nhl/news",
    "https://www.thehockeynews.com/rss",
    "https://www.cbssports.com/nhl/feeds/rss/",
    "https://www.sbnation.com/rss/nhl/index.xml",
    "https://www.yardbarker.com/rss/nhl",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# HTTP SESSION with retries (Railway Free-friendly)
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
    ad = HTTPAdapter(max_retries=retries)
    s.mount("http://", ad)
    s.mount("https://", ad)
    return s

SESSION = make_session()

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
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

def has_seen(url: str) -> bool:
    with db_conn() as c:
        return c.execute("SELECT 1 FROM seen_items WHERE url=?", (url,)).fetchone() is not None

def mark_seen(url: str):
    with db_conn() as c:
        c.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
        c.commit()

def get_setting(key: str):
    with db_conn() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

def set_setting(key: str, value: str):
    with db_conn() as c:
        c.execute("""
            INSERT INTO settings(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        c.commit()

# ---------------------------------------------------------------------------
# Helpers
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

def send_telegram(msg: str, chat_id=None):
    if not chat_id:
        chat_id = CHAT_ID
    if not TOKEN or not chat_id:
        logging.warning("TOKEN/CHAT_ID missing")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": chat_id, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram error: {e}")

def nhl_effective_date():
    """00–17:59 → previous day; 18–23:59 → today (Finnish time)."""
    local = now_local()
    if local.hour < FINNISH_DAY_BOUNDARY_HOUR:
        return (local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return local.strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# RSS (48h filter, duplicate filtered)
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# TWITTER / X — A-version (no API key, no snscrape)
# ---------------------------------------------------------------------------
def twitter_get_user_id(username: str):
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
            logging.warning(f"Twitter error {handle}: {e}")

# ---------------------------------------------------------------------------
# MODERN NHL ENDPOINTS (2026)
# ---------------------------------------------------------------------------
def nhl_schedule(date_str: str):
    """
    Schedule by date from modern API (api-web.nhle.com).
    We normalize to {"dates":[{"games":[...]}]} for backwards compatibility.
    """
    url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    games = []
    # The schedule may return {"games":[...]} directly;
    # If structure differs (e.g., nested gameWeek), flatten here:
    if isinstance(data, dict):
        if "games" in data:
            games = data["games"]
        elif "gameWeek" in data:
            for day in data["gameWeek"]:
                games.extend(day.get("games", []))
    return {"dates": [{"games": games}]}

def nhl_boxscore(game_pk: int):
    """
    Boxscore from modern API (api-web.nhle.com).
    Convert to legacy-like {"teams":{"home":{...},"away":{...}}} for parser reuse.
    """
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_pk}/boxscore"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    raw = r.json()

    wrapped = {"teams": {}}
    mapping = (("homeTeam", "home"), ("awayTeam", "away"))
    for src, dst in mapping:
        t = raw.get(src, {})
        players = t.get("players", [])
        pmap = {}
        for p in players:
            pid = f"ID{p.get('playerId')}"
            pmap[pid] = {
                "person": {
                    "fullName": p.get("name"),
                    "nationality": (p.get("nationality") or "").upper(),
                },
                "stats": {
                    "skaterStats": p.get("skaterStats"),
                    "goalieStats": p.get("goalieStats"),
                },
            }
        wrapped["teams"][dst] = {
            "team": {"name": t.get("name", "")},
            "players": pmap,
        }
    return wrapped

def search_player_by_name(query: str):
    """Player search via search.d3 — returns list of dicts resembling old format."""
    try:
        url = f"https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=20&q={query}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        items = r.json().get("items", [])
        out = []
        for it in items:
            out.append({
                "id": it.get("playerId"),
                "fullName": it.get("name"),
                "currentTeam": {"name": it.get("teamAbbrev") or it.get("teamName", "")},
                "primaryPosition": {"name": it.get("positionCode", "")},
            })
        return out
    except:
        return []

def get_player_stats(player_id: int):
    """
    Season stats via stats REST. This endpoint remains widely used for season aggregates.
    If NHL changes it, we can later swap to api-web player landing.
    """
    try:
        url = f"https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=false&start=0&limit=1&cayenneExp=playerId={player_id}%20and%20gameTypeId=2"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        data = r.json()
        rows = data.get("data", [])
        if rows:
            row = rows[0]
            return {
                "games": row.get("gamesPlayed", 0),
                "goals": row.get("goals", 0),
                "assists": row.get("assists", 0),
            }
    except:
        pass
    return {}

# ---------------------------------------------------------------------------
# Games list with Finns (Option A: list all Finns in lineup)
# ---------------------------------------------------------------------------
def list_finns_in_box(box: dict):
    names = []
    for side in ("home", "away"):
        team = box.get("teams", {}).get(side, {})
        players = team.get("players", {}) or {}
        for pdata in players.values():
            person = pdata.get("person", {})
            if person.get("nationality", "").upper() == "FIN":
                nm = person.get("fullName")
                if nm:
                    names.append(nm)
    return names

def get_games_with_finns(date_str: str):
    try:
        sch = nhl_schedule(date_str)
    except:
        return []
    dates = sch.get("dates", [])
    if not dates:
        return []

    out = []
    games = dates[0].get("games", [])
    for g in games:
        # Modern API field names
        home = g.get("homeTeam", {}).get("name") or g.get("homeTeam", {}).get("abbrev") or "Home"
        away = g.get("awayTeam", {}).get("name") or g.get("awayTeam", {}).get("abbrev") or "Away"
        status = g.get("gameState") or g.get("gameScheduleState") or ""
        start_utc = g.get("startTimeUTC") or g.get("gameTimeUTC") or g.get("gameDate")

        # Scores (if present)
        hsc = g.get("homeTeam", {}).get("score")
        asc = g.get("awayTeam", {}).get("score")

        # Time to Finnish
        try:
            dt = dateparser.parse(start_utc).astimezone(pytz.timezone(TIMEZONE))
            t = dt.strftime("%H:%M")
        except:
            t = "?"

        # line header
        if status.upper() in ("FINAL", "OFF"):  # OFF = finished in some snapshots
            header = f"🏁 {home} {hsc} – {asc} {away}"
        elif status.upper() in ("LIVE", "INPROGRESS", "IN_PROGRESS"):
            header = f"🔴 LIVE {home} {hsc} – {asc} {away}"
        else:
            header = f"⏰ {away} @ {home} klo {t}"

        # Fetch boxscore for Finn list
        game_pk = g.get("id") or g.get("gameId") or g.get("gamePk")
        if game_pk:
            try:
                box = nhl_boxscore(int(game_pk))
                finns = list_finns_in_box(box)
                if finns:
                    header += "\nSuomalaiset: " + ", ".join(sorted(set(finns)))
            except:
                pass

        out.append(header)

    return out

# ---------------------------------------------------------------------------
# Finnish players' points for a given date (nightly report & /suomalaiset)
# ---------------------------------------------------------------------------
def fetch_finnish_points_for_date(date_str: str):
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
        game_pk = g.get("id") or g.get("gameId") or g.get("gamePk")
        if not game_pk:
            continue
        try:
            box = nhl_boxscore(int(game_pk))
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

                if "skaterStats" in stats and stats["skaterStats"]:
                    s = stats["skaterStats"]
                    g_ = int(s.get("goals", 0))
                    a_ = int(s.get("assists", 0))
                    p_ = g_ + a_
                    plus = s.get("plusMinus", 0)
                    toi = s.get("toi") or s.get("timeOnIce") or "00:00"
                    shots = s.get("shots")
                    hits = s.get("hits")
                    pim = s.get("pim") or s.get("penaltyMinutes")
                    header = f"{name} ({tname}) {g_}+{a_}={p_}, ±{plus}, TOI {toi}"
                    notes = []
                    if shots not in [None, ""]: notes.append(f"Laukaukset {shots}")
                    if hits not in [None, ""]: notes.append(f"Taklaukset {hits}")
                    if pim not in [None, "", 0]: notes.append(f"Jäähyt {pim} min")
                    line = f"• {header}"
                    if notes:
                        line += "\n  " + " | ".join(notes)
                    results.append((p_, name, line))

                elif "goalieStats" in stats and stats["goalieStats"]:
                    gk = stats["goalieStats"]
                    sv = int(gk.get("saves", 0))
                    sa = int(gk.get("shots", gk.get("shotsAgainst", 0)))
                    toi = gk.get("toi") or gk.get("timeOnIce") or "00:00"
                    svpct = round(sv / sa, 3) if sa > 0 else "—"
                    header = f"{name} ({tname})\nMV: {sv}/{sa} torjuntaa, SV% {svpct}, TOI {toi}"
                    results.append((0, name, f"• {header}"))

    results.sort(key=lambda x: (-x[0], x[1]))
    return [r[2] for r in results]

# ---------------------------------------------------------------------------
# Telegram commands (long polling)
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

def handle_command(cmd: str, chat_id):
    c = cmd.strip().split()[0].lower()

    if c.startswith("/ping"):
        send_telegram("pong", chat_id)
        return

    if c.startswith("/suomalaiset"):
        d = nhl_effective_date()
        rows = fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\n\n" + "\n\n".join(rows), chat_id)
        else:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\nEi suomalaispisteitä.", chat_id)
        return

    if c.startswith("/stats") or c.startswith("/test"):
        d = nhl_effective_date()
        rows = fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"📊 Suomalaisraportti — {d}\n\n" + "\n\n".join(rows), chat_id)
        else:
            send_telegram(f"📊 Suomalaisraportti — {d}\nEi suomalaispisteitä.", chat_id)
        return

    if c.startswith("/players"):
        parts = cmd.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram("Käyttö: /players <nimi>", chat_id)
            return
        q = parts[1].strip()
        players = search_player_by_name(q)
        if not players:
            send_telegram(f"Ei pelaajia haulla: {q}", chat_id)
            return
        p = players[0]
        pid = p.get("id")
        name = p.get("fullName", "")
        team = p.get("currentTeam", {}).get("name", "")
        pos = p.get("primaryPosition", {}).get("name", "")
        stats = get_player_stats(pid)
        g = stats.get("goals", 0)
        a = stats.get("assists", 0)
        gp = stats.get("games", 0)
        msg = (
            f"📌 Pelaaja: {name}\n"
            f"Joukkue: {team}\n"
            f"Pelipaikka: {pos}\n\n"
            f"Pelit: {gp}\n"
            f"Pisteet: {g}+{a}={g+a}"
        )
        send_telegram(msg, chat_id)
        return

    if c.startswith("/games"):
        d = nhl_effective_date()
        lines = get_games_with_finns(d)
        if not lines:
            send_telegram("Ei otteluita.", chat_id)
            return
        send_telegram("📅 NHL-ottelut:\n\n" + "\n\n".join(lines), chat_id)
        return

    send_telegram("Tuntematon komento. Kokeile: /ping /players /games /suomalaiset /stats /test", chat_id)

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
# 08:00 Finnish nightly report (once per effective NHL day)
# ---------------------------------------------------------------------------
def send_nightly_finns_once():
    last = get_setting("last_stats_date")
    target = nhl_effective_date()
    if last == target:
        return
    now = now_local()
    if now.hour > NIGHTLY_STATS_HOUR or (now.hour == NIGHTLY_STATS_HOUR and now.minute >= NIGHTLY_STATS_MINUTE):
        for attempt in range(5):
            try:
                rows = fetch_finnish_points_for_date(target)
                if rows:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target}\n\n" + "\n\n".join(rows)
                else:
                    msg = f"🇫🇮 Viime yön suomalaiset — {target}\nEi suomalaispisteitä."
                send_telegram(msg)
                set_setting("last_stats_date", target)
                return
            except Exception as e:
                if attempt == 4:
                    send_telegram(f"⚠️ Suomalaisraportti epäonnistui: {e}")
                time.sleep(4 * (attempt + 1))

# ---------------------------------------------------------------------------
# MAIN
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
            if now_ts - last_cmd >= UPDATES_POLL_SECONDS:
                poll_commands(state)
                last_cmd = now_ts

            if now_ts - last_rss >= 200:
                poll_rss()
                last_rss = now_ts

            if now_ts - last_tw >= 260:
                poll_twitter()
                last_tw = now_ts

            send_nightly_finns_once()
            time.sleep(1)

        except Exception as e:
            logging.warning(f"Main loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
