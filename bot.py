# -*- coding: utf-8 -*-
"""
NHL Telegram Bot – Final Full Version
OSA 1/6 – Imports, Config, Retry HTTP Session, SQLite Setup
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

# How often to poll Telegram getUpdates
UPDATES_POLL_SECONDS = 2

# For filtering old news
MAX_HOURS = 48

# HTTP timeout
HTTP_TIMEOUT = 20

# Automated Finnish report time
NIGHTLY_STATS_HOUR = 8
NIGHTLY_STATS_MINUTE = 0

# NHL time logic: before 18 Finnish time = yesterday’s games
FINNISH_DAY_BOUNDARY_HOUR = 18

# Twitter handles for A-version X-module
TWITTER_USERS = [
    "FriedgeHNIC",
    "reporterchris",
    "DarrenDreger",
    "PierreVLeBrun",
    "frank_seravalli",
    "RussoHockey"
]

# RSS Feeds
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

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# =============================================================================
# HTTP SESSION WITH RETRIES (Railway Free-tier friendly)
# =============================================================================

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session():
    """Return requests.Session() with retry & backoff."""
    s = requests.Session()
    
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    return s

SESSION = make_session()

# =============================================================================
# SQLITE DATABASE
# =============================================================================

def db_conn():
    """Open SQLite connection with WAL journaling."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    """Create tables if needed."""
    with db_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_items (
                url TEXT PRIMARY KEY,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()


def has_seen(url: str) -> bool:
    with db_conn() as conn:
        cur = conn.execute("SELECT 1 FROM seen_items WHERE url=?", (url,))
        return cur.fetchone() is not None


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
        conn.execute("""
            INSERT INTO settings(key, value)
            VALUES (?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        conn.commit()

# =============================================================================
# HELPER FUNCTIONS (Telegram, Dates, Filters)
# =============================================================================

def now_local():
    """Return current local time in Finnish timezone."""
    return datetime.now(pytz.timezone(TIMEZONE))


def send_telegram(msg: str, chat_id=None):
    """Send a Telegram message using the bot."""
    if not chat_id:
        chat_id = CHAT_ID
    if not TOKEN or not chat_id:
        logging.warning("TOKEN or CHAT_ID missing")
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        SESSION.post(url, data={"chat_id": chat_id, "text": msg}, timeout=HTTP_TIMEOUT)
    except Exception as e:
        logging.warning(f"Telegram send error: {e}")


def normalize_url(url: str) -> str:
    """Remove tracking params from URLs to avoid duplicate detection issues."""
    try:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(p.query)
        q_clean = [(k, v) for (k, v) in q if not k.lower().startswith(("utm_", "fbclid", "gclid"))]
        new_q = urllib.parse.urlencode(q_clean)
        return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, ""))
    except:
        return url


def is_recent(date_str: str, hours=48):
    """Return True if a timestamp is under X hours old."""
    if not date_str:
        return False
    try:
        dt = dateparser.parse(date_str)
        if not dt.tzinfo:
            dt = pytz.timezone(TIMEZONE).localize(dt)
        age = now_local() - dt
        return age.total_seconds() <= hours * 3600
    except:
        return False

# =============================================================================
# FINNISH NHL DATE LOGIC
# =============================================================================
def nhl_effective_date():
    """
    Return the correct NHL date for Finnish time.
    - 00:00–17:59 → previous day (previous night's NHL games)
    - 18:00–23:59 → current date
    """
    local = now_local()
    if local.hour < FINNISH_DAY_BOUNDARY_HOUR:
        return (local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return local.strftime("%Y-%m-%d")

# =============================================================================
# RSS MODULE (48h fresh news only, duplicate filtered)
# =============================================================================

def poll_rss():
    """Poll all RSS feeds and send new NHL news."""
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries:
                # Timestamp for filtering
                pub = getattr(entry, "published", NoneINEN\n\n{title}\n{link}")                pub = getattr(entry, "published", None) or getattr(entry, "updated", None)
                mark_seen(link)

        except Exception as e:
            logging.warning(f"RSS error {feed_url}: {e}")
            time.sleep(2)


# =============================================================================
# TWITTER / X – A-VERSION (NO API, NO SNSCRAPE)
# Uses public JSON endpoints: compatible with Railway Free-tier.
# =============================================================================

def twitter_get_user_id(username: str):
    """Fetch X/Twitter user's ID via public JSON endpoint."""
    try:
        url = f"https://cdn.syndication.twimg.com/user/by-screen-name/{username}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except Exception:
        return None


def twitter_get_latest_tweets(user_id: str, limit=5):
    """Fetch timeline tweets without authentication."""
    try:
        url = f"https://cdn.syndication.twimg.com/timeline/profile/{user_id}.json"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)

        if r.status_code != 200:
            return []

        data = r.json()
        instructions = data.get("instructions", [])
        tweets = []

        for instr in instructions:
            if "addEntries" in instr:
                for entry in instr["addEntries"]["entries"]:
                    tweet = (
                        entry.get("content", {})
                             .get("item", {})
                             .get("content", {})
                             .get("tweet")
                    )
                    if tweet:
                        tweets.append(tweet)

        return tweets[:limit]

    except Exception:
        return []


def poll_twitter():
    """Poll configured Twitter handles using public JSON API."""
    for handle in TWITTER_USERS:
        try:
            # Cached user_id for speed
            key = f"twitter_userid_{handle}"
            user_id = get_setting(key)

            if not user_id:
                user_id = twitter_get_user_id(handle)
                if not user_id:
                    continue
                set_setting(key, user_id)

            tweets = twitter_get_latest_tweets(user_id)
            if not tweets:
                continue

            for tw in tweets:
                tid = tw.get("id_str") or tw.get("id")
                text = tw.get("full_text") or tw.get("text") or ""
                created = tw.get("created_at")
                url = f"https://x.com/{handle}/status/{tid}"

                # 48h freshness check
                if not is_recent(created, MAX_HOURS):
                    continue
                # Duplicate check
                if has_seen(url):
                    continue

                send_telegram(f"🐦 X — {handle}\n\n{text}\n{url}")
                mark_seen(url)

        except Exception as e:
            logging.warning(f"Twitter error ({handle}): {e}")
            continue
                if not is_recent(pub, MAX_HOURS):
                    continue

                link = normalize_url(getattr(entry, "link", ""))
                title = getattr(entry, "title", "").strip()

                if not link or not title:
                    continue
                if has_seen(link):
                    continue

# =============================================================================
# NHL API HELPERS: schedule, boxscore, effective date logic
# =============================================================================

def nhl_schedule(date_str):
    """Fetch NHL schedule for a specific date."""
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={date_str}"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def nhl_boxscore(game_pk):
    """Fetch boxscore data for an NHL game."""
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_pk}/boxscore"
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def nhl_effective_games():
    """
    Return 'effective date' for NHL: before 18:00 Finnish time → yesterday,
    after 18:00 → today.
    Used by /games and /suomalaiset.
    """
    local = now_local()
    if local.hour < FINNISH_DAY_BOUNDARY_HOUR:
        return (local.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return local.strftime("%Y-%m-%d")


# =============================================================================
# FINNISH PLAYERS – NIGHTLY STATS (used by /suomalaiset and morning report)
# =============================================================================

def fetch_finnish_points_for_date(date_str):
    """
    Return formatted list of Finnish player results from games played on given date.
    Each element formatted ready for Telegram.
    """
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
        game_pk = g.get("gamePk")
        if not game_pk:
            continue

        try:
            box = nhl_boxscore(game_pk)
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

                # SKATER
                if "skaterStats" in stats:
                    s = stats["skaterStats"]
                    g_ = int(s.get("goals", 0))
                    a_ = int(s.get("assists", 0))
                    p_ = g_ + a_
                    plus = s.get("plusMinus", 0)
                    toi = s.get("timeOnIce", "00:00")
                    shots = s.get("shots")
                    hits = s.get("hits")
                    blocks = s.get("blocked")
                    pim = s.get("penaltyMinutes")
                    fow = s.get("faceOffWins")
                    fot = s.get("faceoffTaken")
                    pp_toi = s.get("powerPlayTimeOnIce", "0:00")
                    sh_toi = s.get("shortHandedTimeOnIce", "0:00")

                    header = f"{name} ({tname}) {g_}+{a_}={p_}, ±{plus}, TOI {toi}"
                    notes = []

                    if shots not in [None, ""]:
                        notes.append(f"Laukaukset {shots}")
                    if fow not in [None, "", 0] and fot:
                        notes.append(f"Aloitukset {fow}/{fot}")
                    if pim not in [None, "", 0]:
                        notes.append(f"Jäähyt {pim} min")
                    if hits not in [None, "", 0]:
                        notes.append(f"Taklaukset {hits}")
                    if blocks not in [None, "", 0]:
                        notes.append(f"Blokit {blocks}")
                    if pp_toi not in ["0:00", None, ""]:
                        notes.append(f"YV {pp_toi}")
                    if sh_toi not in ["0:00", None, ""]:
                        notes.append(f"AV {sh_toi}")

                    line = f"• {header}"
                    if notes:
                        line += "\n  " + " | ".join(notes)

                    results.append((p_, name, line))

                # GOALIE
                elif "goalieStats" in stats:
                    gk = stats["goalieStats"]
                    sv = int(gk.get("saves", 0))
                    sa = int(gk.get("shotsAgainst", 0))
                    toi = gk.get("timeOnIce", "00:00")
                    svpct = round(sv / sa, 3) if sa > 0 else "—"

                    header = f"{name} ({tname})\nMV: {sv}/{sa} torjuntaa, SV% {svpct}, TOI {toi}"
                    notes = []

                    for key, label in [
                        ("evenSaves", "EV"),
                        ("powerPlaySaves", "YV"),
                        ("shortHandedSaves", "AV")
                    ]:
                        if gk.get(key) is not None:
                            notes.append(f"{label} {gk.get(key)}")

                    line = f"• {header}"
                    if notes:
                        line += "\n  " + " | ".join(notes)

                    results.append((0, name, line))

    # Sort by points desc, name asc
    results.sort(key=lambda x: (-x[0], x[1]))

    return [r[2] for r in results]


# =============================================================================
# GAMES OF THE DAY – INCLUDING FINNISH PLAYERS (OPTION A)
# =============================================================================

def list_finns_in_game(box):
    """Return a list of all Finnish players in a boxscore."""
    names = []
    for side in ("home", "away"):
        team = box.get("teams", {}).get(side, {})
        players = team.get("players", {}) or {}
        for pdata in players.values():
            person = pdata.get("person", {})
            if person.get("nationality", "").upper() == "FIN":
                names.append(person.get("fullName"))
    return names


def get_games_with_finns(date_str):
    """
    Return formatted lines for /games command:
    - Finnish time kickoff
    - All Finnish players in each game (Option A)
    """
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
        home = g["teams"]["home"]["team"]["name"]
        away = g["teams"]["away"]["team"]["name"]
        status = g["status"]["detailedState"]

        # Convert gameDate to Finnish time
        gd = g.get("gameDate")
        try:
            dt = dateparser.parse(gd).astimezone(pytz.timezone(TIMEZONE))
            t = dt.strftime("%H:%M")
        except:
            t = "?"

        # Scores (if live or final)
        hsc = g["teams"]["home"]["score"]
        asc = g["teams"]["away"]["score"]

        line = ""

        if status.startswith("Final"):
            line += f"🏁 {home} {hsc} – {asc} {away}"
        elif status in ("Live", "In Progress"):
            line += f"🔴 LIVE {home} {hsc} – {asc} {away}"
        else:
            line += f"⏰ {away} @ {home} klo {t}"

        # Fetch Finnish players
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
# PLAYER SEARCH (/players)
# =============================================================================

def search_player_by_name(query: str):
    """Search NHL players by name using Stats API."""
    try:
        url = f"https://statsapi.web.nhl.com/api/v1/people/?names={query}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        data = r.json()
        return data.get("people", [])
    except:
        return []


def get_player_stats(pid: int):
    """Return player's season stats."""
    try:
        url = f"https://statsapi.web.nhl.com/api/v1/people/{pid}/stats?stats=statsSingleSeason&season=20242025"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        splits = r.json().get("stats", [])[0].get("splits", [])
        return splits[0].get("stat", {}) if splits else {}
    except:
        return {}


# =============================================================================
# TELEGRAM COMMAND HANDLING (LONG POLLING)
# =============================================================================

def tg_get_updates(offset):
    """Fetch new Telegram messages using getUpdates (long polling)."""
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
        params = {"timeout": 20}
        if offset is not None:
            params["offset"] = offset

        r = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []

        data = r.json()
        return data.get("result", [])

    except Exception as e:
        logging.warning(f"getUpdates error: {e}")
        return []


def handle_command(cmd: str, chat_id):
    """Interpret a command string and run the correct bot feature."""
    c = cmd.strip().split()[0].lower()

    # /ping
    if c.startswith("/ping"):
        send_telegram("pong", chat_id)
        return

    # /suomalaiset
    if c.startswith("/suomalaiset"):
        try:
            d = nhl_effective_games()
            rows = fetch_finnish_points_for_date(d)
            if rows:
                send_telegram(f"🇫🇮 Suomalaiset — {d}\n\n" + "\n\n".join(rows), chat_id)
            else:
                send_telegram(f"🇫🇮 Suomalaiset — {d}\nEi suomalaispisteitä.", chat_id)
        except Exception as e:
            send_telegram(f"Virhe suomalaishausta: {e}", chat_id)
        return

    # /stats OR /test (same logic)
    if c.startswith("/stats") or c.startswith("/test"):
        try:
            d = nhl_effective_games()
            rows = fetch_finnish_points_for_date(d)
            if rows:
                send_telegram(f"📊 Suomalaisraportti — {d}\n\n" + "\n\n".join(rows), chat_id)
            else:
                send_telegram(f"📊 Suomalaisraportti — {d}\nEi suomalaispisteitä.", chat_id)
        except Exception as e:
            send_telegram(f"Virhe raporteissa: {e}", chat_id)
        return

    # /players <nimi>
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

        # Pick the best match
        p = players[0]
        pid = p.get("id")
        name = p.get("fullName", "")
        team = p.get("currentTeam", {}).get("name", "")
        pos = p.get("primaryPosition", {}).get("name", "")

        stats = get_player_stats(pid)
        g = stats.get("goals", 0)
        a = stats.get("assists", 0)
        pts = g + a
        gp = stats.get("games", 0)

        msg = (
            f"📌 Pelaaja: {name}\n"
            f"Joukkue: {team}\n"
            f"Pelipaikka: {pos}\n\n"
            f"Pelit: {gp}\n"
            f"Pisteet: {g}+{a}={pts}"
        )
        send_telegram(msg, chat_id)
        return

    # /games
    if c.startswith("/games"):
        d = nhl_effective_games()
        lines = get_games_with_finns(d)

        if not lines:
            send_telegram("Ei otteluita tälle päivälle/yölle.", chat_id)
            return

        send_telegram("📅 NHL-ottelut:\n\n" + "\n\n".join(lines), chat_id)
        return

    # Unknown command
    send_telegram("Tuntematon komento.\nKokeile: /ping /players /games /suomalaiset /stats /test", chat_id)


def poll_commands(state):
    """Poll Telegram updates repeatedly; feed commands to handler."""
    if not ENABLE_COMMANDS:
        return

    offset = state.get("tg_offset")
    updates = tg_get_updates(offset)
    if not updates:
        return

    max_id = offset or 0

    for upd in updates:
        try:
            uid = upd.get("update_id", 0)
            if uid > max_id:
                max_id = uid

            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue

            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "")

            if text.startswith("/"):
                handle_command(text, chat_id)

        except Exception as e:
            logging.warning(f"poll_commands error: {e}")

    # Save offset
    state["tg_offset"] = max_id + 1


# =============================================================================
# AUTOMATIC 08:00 FINNISH PLAYERS REPORT
# =============================================================================

def send_nightly_finns_once():
    """
    Sends the Finnish players' nightly report once per effective NHL day.
    Uses the 00–18 = yesterday rule.
    """
    last_sent = get_setting("last_stats_date")
    target_date = nhl_effective_games()   # correct date based on Finland time

    # Already sent?
    if last_sent == target_date:
        return

    now = now_local()
    # Send only when correct local time has passed
    if now.hour > NIGHTLY_STATS_HOUR or (now.hour == NIGHTLY_STATS_HOUR and now.minute >= NIGHTLY_STATS_MINUTE):
        # Retry handling (DNS hiccups on Railway Free)
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
                time.sleep(4 * (attempt + 1))


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    init_db()
    logging.info("NHL Bot is running...")

    last_rss = 0
    last_tw = 0
    last_cmd = 0

    state = {"tg_offset": None}

    while True:
        now_ts = time.time()

        try:
            # Poll Telegram commands
            if now_ts - last_cmd >= UPDATES_POLL_SECONDS:
                poll_commands(state)
                last_cmd = now_ts

            # RSS news
            if now_ts - last_rss >= 200:
                poll_rss()
                last_rss = now_ts

            # Twitter/X A-Version
            if now_ts - last_tw >= 260:
                poll_twitter()
                last_tw = now_ts

            # Automatic Finnish report at 08:00
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
