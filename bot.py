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
MAX_HOURS = 48
HTTP_TIMEOUT = 20

# 08:00 Finnish morning report
NIGHTLY_STATS_HOUR = 8
NIGHTLY_STATS_MINUTE = 0

# 00–17:59 = previous day, 18–23:59 = current day
FINNISH_DAY_BOUNDARY_HOUR = 18

# Twitter handles (A-version)
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
# HTTP SESSION (RETRY) — Railway Free -friendly
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
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

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
    """00–17:59 = yesterday; 18–23:59 = today."""
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
                send_telegram("NHL-UUTINEN\n\n{0}\n{1}".format(title, link))
                mark_seen(link)
        except Exception as e:
            logging.warning("RSS error {0}: {1}".format(feed_url, e))
            time.sleep(2)

# ---------------------------------------------------------------------------
# TWITTER / X — A-version (no API key, no snscrape)
# ---------------------------------------------------------------------------
def twitter_get_user_id(username: str):
    try:
        url = "https://cdn.syndication.twimg.com/user/by-screen-name/{0}".format(username)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        data = r.json()
        return data.get("id_str") or data.get("id")
    except:
        return None

def twitter_get_latest_tweets(user_id: str, limit=5):
    try:
        url = "https://cdn.syndication.twimg.com/timeline/profile/{0}.json".format(user_id)
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
            key = "twitter_userid_{0}".format(handle)
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
                url = "https://x.com/{0}/status/{1}".format(handle, tid)
                if not is_recent(created, MAX_HOURS):
                    continue
                if has_seen(url):
                    continue
                send_telegram("X — {0}\n\n{1}\n{2}".format(handle, text, url))
                mark_seen(url)
        except Exception as e:
            logging.warning("Twitter error {0}: {1}".format(handle, e))

# ---------------------------------------------------------------------------
# MODERN NHL ENDPOINTS (SCHEDULE, BOXSCORE, PLAYER SEARCH)
# ---------------------------------------------------------------------------
def nhl_schedule(date_str: str):
    """
    Moderni paivakohtainen schedule (api-web.nhle.com).
    Normalisoidaan muotoon {"dates":[{"games":[...]}]}.
    """
    url = "https://api-web.nhle.com/v1/schedule/{0}".format(date_str)
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    games = []
    if isinstance(data, dict):
        if "games" in data:
            games = data["games"]
        elif "gameWeek" in data:
            for day in data["gameWeek"]:
                games.extend(day.get("games", []))
    return {"dates": [{"games": games}]}

# ---- nationality caching (for Finnish player detection) ----
def cache_get_nat(player_id: int):
    return get_setting("nat_{0}".format(player_id))

def cache_set_nat(player_id: int, nat: str):
    if nat:
        set_setting("nat_{0}".format(player_id), nat)

def resolve_nationality(player_id: int, fallback_name: str = "") -> str:
    """
    Taydentaa pelaajan kansallisuuden search.d3 API:sta,
    koska modernissa boxscoren datassa nationality voi puuttua.
    """
    cached = cache_get_nat(player_id)
    if cached:
        return cached
    try:
        q = fallback_name if fallback_name else str(player_id)
        url = "https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=20&q={0}".format(q)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            for it in r.json().get("items", []):
                if int(it.get("playerId", 0)) == int(player_id):
                    nat = (it.get("birthCountry") or "").upper()
                    if nat:
                        cache_set_nat(player_id, nat)
                        return nat
    except:
        pass
    return ""

def nhl_boxscore(game_pk: int):
    """
    Moderni boxscore (api-web.nhle.com). Tukee players[] ja playerByGameStats[].
    Normalisoidaan muotoon {"teams":{"home":{...},"away":{...}}}.
    """
    url = "https://api-web.nhle.com/v1/gamecenter/{0}/boxscore".format(game_pk)
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    raw = r.json()

    wrapped = {"teams": {}}

    for src, dst in (("homeTeam", "home"), ("awayTeam", "away")):
        t = raw.get(src, {})
        name = t.get("name", "") or t.get("abbrev", "")

        players = t.get("players")
        dict_players = {}

        if isinstance(players, list) and players:
            # players[]-rakenne
            for p in players:
                pid = int(p.get("playerId"))
                fullName = p.get("name")
                nat = (p.get("nationality") or "").upper()
                if not nat:
                    nat = resolve_nationality(pid, fullName)
                sk = p.get("skaterStats") or {}
                gk = p.get("goalieStats") or {}
                dict_players["ID{0}".format(pid)] = {
                    "person": {"fullName": fullName, "nationality": nat},
                    "stats": {"skaterStats": sk if sk else None, "goalieStats": gk if gk else None},
                }
        else:
            # playerByGameStats-fallback
            pstats = t.get("playerByGameStats", {})
            merged = []
            for key in ("forwards", "defense", "goalies", "scratches"):
                arr = pstats.get(key, [])
                if isinstance(arr, list):
                    merged.extend(arr)

            for p in merged:
                pid = int(p.get("playerId"))
                fullName = p.get("name", {}).get("default") or p.get("name") or ""
                nat = resolve_nationality(pid, fullName)

                sk = None
                gk = None
                if (p.get("position") or "").upper() != "G":
                    sk = {
                        "goals": p.get("goals", 0),
                        "assists": p.get("assists", 0),
                        "points": p.get("points", 0),
                        "plusMinus": p.get("plusMinus", 0),
                        "hits": p.get("hits"),
                        "shots": p.get("shots"),
                        "timeOnIce": p.get("toi") or p.get("timeOnIce"),
                        "penaltyMinutes": p.get("pim"),
                    }
                else:
                    gk = {
                        "saves": p.get("saves", 0),
                        "shotsAgainst": p.get("shots") or p.get("shotsAgainst", 0),
                        "timeOnIce": p.get("toi") or p.get("timeOnIce"),
                    }

                dict_players["ID{0}".format(pid)] = {
                    "person": {"fullName": fullName, "nationality": nat},
                    "stats": {"skaterStats": sk, "goalieStats": gk},
                }

        wrapped["teams"][dst] = {"team": {"name": name}, "players": dict_players}

    return wrapped

def search_player_by_name(query: str):
    """Player search via search.d3."""
    try:
        url = "https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=20&q={0}".format(query)
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
    Kausitilastot stats REST -rajapinnasta (kausiaggregaatit).
    """
    try:
        url = (
            "https://api.nhle.com/stats/rest/en/skater/summary"
            "?isAggregate=false&isGame=false&start=0&limit=1"
            "&cayenneExp=playerId={0}%20and%20gameTypeId=2"
        ).format(player_id)
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
# Games list with scorers (A-version: listaa pistemiehet; ei erillista suomalaisrivia)
# ---------------------------------------------------------------------------
FINISHED_STATES = {"FINAL", "OFF"}      # OFF = finished joissain feedeissa
LIVE_STATES     = {"LIVE", "INPROGRESS", "IN_PROGRESS"}
FUTURE_STATES   = {"FUT", "SCHEDULED", "PRE"}

def filter_games_by_state(games, include_future=False):
    """Palauta vain finished+live ellei include_future=True."""
    out = []
    for g in games:
        state = (g.get("gameState") or g.get("gameScheduleState") or "").upper()
        if state in FINISHED_STATES or state in LIVE_STATES:
            out.append(g)
        elif include_future:
            out.append(g)
    return out

def get_game_scorers(game_pk: int):
    """
    Hakee ottelun pistemiehet landing-endpointin kautta.
    Palauttaa rivit kuten: 'Barkov 1+2=3'
    """
    try:
        url = "https://api-web.nhle.com/v1/gamecenter/{0}/landing".format(game_pk)
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        scorers = []
        pg = data.get("playerByGameStats", {})

        for side in ("homeTeam", "awayTeam"):
            side_block = pg.get(side, {})
            players = []
            for key in ("forwards", "defense", "goalies"):
                arr = side_block.get(key, [])
                if isinstance(arr, list):
                    players.extend(arr)

            for p in players:
                name = p.get("name", {}).get("default") or p.get("name") or ""
                g = int(p.get("goals", 0))
                a = int(p.get("assists", 0))
                pts = g + a
                if pts > 0:
                    scorers.append("{0} {1}+{2}={3}".format(name, g, a, pts))

        def pts_key(line):
            # line format: 'Name G+A=P'
            pts = int(line.split("=")[1])
            return (-pts, line.lower())

        scorers.sort(key=pts_key)
        return scorers

    except Exception as e:
        logging.warning("landing scorers error: {0}".format(e))
        return []

def get_games_with_finns(date_str: str, include_future: bool = False):
    try:
        sch = nhl_schedule(date_str)
    except:
        return []
    dates = sch.get("dates", [])
    if not dates:
        return []

    games = dates[0].get("games", [])
    games = filter_games_by_state(games, include_future=include_future)

    out = []
    for g in games:
        home = g.get("homeTeam", {}).get("name") or g.get("homeTeam", {}).get("abbrev") or "Home"
        away = g.get("awayTeam", {}).get("name") or g.get("awayTeam", {}).get("abbrev") or "Away"
        state = (g.get("gameState") or g.get("gameScheduleState") or "").upper()
        start_utc = g.get("startTimeUTC") or g.get("gameTimeUTC") or g.get("gameDate")
        hsc = g.get("homeTeam", {}).get("score")
        asc = g.get("awayTeam", {}).get("score")

        try:
            dt = dateparser.parse(start_utc).astimezone(pytz.timezone(TIMEZONE))
            t = dt.strftime("%H:%M")
        except:
            t = "?"

        if state in FINISHED_STATES:
            header = "{0} {1} – {2} {3}".format(home, hsc, asc, away)
            header = "FINAL " + header
        elif state in LIVE_STATES:
            header = "LIVE {0} {1} – {2} {3}".format(home, hsc, asc, away)
        else:
            header = "{0} @ {1} klo {2}".format(away, home, t)

        game_pk = g.get("id") or g.get("gameId") or g.get("gamePk")
        if game_pk:
            try:
                scorers = get_game_scorers(int(game_pk))
                if scorers:
                    header += "\nPistemiehet:\n" + "\n".join("  • " + s for s in scorers)
            except:
                pass

        out.append(header)

    return out

# ---------------------------------------------------------------------------
# Finnish players' nightly points for a given date
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

                if stats.get("skaterStats"):
                    s = stats["skaterStats"]
                    g_ = int(s.get("goals", 0))
                    a_ = int(s.get("assists", 0))
                    p_ = g_ + a_
                    plus = s.get("plusMinus", 0)
                    toi = s.get("toi") or s.get("timeOnIce") or "00:00"
                    shots = s.get("shots")
                    hits = s.get("hits")
                    pim = s.get("pim") or s.get("penaltyMinutes")

                    header = "{0} ({1}) {2}+{3}={4}, ±{5}, TOI {6}".format(name, tname, g_, a_, p_, plus, toi)
                    notes = []
                    if shots not in [None, ""]: notes.append("Laukaukset {0}".format(shots))
                    if hits  not in [None, ""]: notes.append("Taklaukset {0}".format(hits))
                    if pim   not in [None, "", 0]: notes.append("Jäähyt {0} min".format(pim))

                    line = "• " + header
                    if notes:
                        line += "\n  " + " | ".join(notes)
                    results.append((p_, name, line))

                elif stats.get("goalieStats"):
                    gk = stats["goalieStats"]
                    sv = int(gk.get("saves", 0))
                    sa = int(gk.get("shots") or gk.get("shotsAgainst", 0))
                    toi = gk.get("toi") or gk.get("timeOnIce") or "00:00"
                    svpct = round(sv / sa, 3) if sa > 0 else "—"
                    header = "{0} ({1})\nMV: {2}/{3} torjuntaa, SV% {4}, TOI {5}".format(name, tname, sv, sa, svpct, toi)
                    results.append((0, name, "• " + header))

    results.sort(key=lambda x: (-x[0], x[1]))
    return [r[2] for r in results]
    
# ---------------------------------------------------------------------------
# Telegram getUpdates (long polling)
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
# Standings (divisions)
# ---------------------------------------------------------------------------
def get_standings():
    """
    Hakee kuluvan kauden standingsit kaikista divisioonista.
    Palauttaa dict:
        { "Atlantic": [ (teamName, PTS, W, L, OT), ... ], ... }
    """
    try:
        now = now_local()
        year = now.year
        # Kausi alkaa syksyllä => jos kevät, kausi on esim 20252026
        if now.month < 7:
            season = f"{year-1}{year}"
        else:
            season = f"{year}{year+1}"

        url = f"https://api-web.nhle.com/v1/standings/{season}"
        r = SESSION.get(url, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        divisions = {}
        for team in data:
            div = team.get("divisionName", "Unknown")
            name = team.get("teamName", "Team")
            pts = team.get("points", 0)
            w = team.get("wins", 0)
            l = team.get("losses", 0)
            ot = team.get("otLosses", 0)
            divisions.setdefault(div, []).append((name, pts, w, l, ot))

        for div in divisions:
            divisions[div].sort(key=lambda x: -x[1])

        return divisions

    except Exception as e:
        logging.warning(f"standings error: {e}")
        return {}

# ---------------------------------------------------------------------------
# Telegram command handler
# ---------------------------------------------------------------------------
def handle_command(cmd: str, chat_id):
    c = cmd.strip().split()[0].lower()

    # PING
    if c.startswith("/ping"):
        send_telegram("pong", chat_id)
        return

    # SUOMALAISET (pisteet, viime yön kierros)
    if c.startswith("/suomalaiset"):
        d = nhl_effective_date()
        rows = fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\n\n" + "\n\n".join(rows), chat_id)
        else:
            send_telegram(f"🇫🇮 Suomalaiset — {d}\nEi suomalaispisteitä.", chat_id)
        return

    # STATS/TEST (sama kuin suomalaiset, mutta eri otsikko)
    if c.startswith("/stats") or c.startswith("/test"):
        d = nhl_effective_date()
        rows = fetch_finnish_points_for_date(d)
        if rows:
            send_telegram(f"📊 Suomalaisraportti — {d}\n\n" + "\n\n".join(rows), chat_id)
        else:
            send_telegram(f"📊 Suomalaisraportti — {d}\nEi suomalaispisteitä.", chat_id)
        return

    # PLAYERS <nimi>
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

    # GAMES / GAMES ALL
    if c.startswith("/games"):
        include_future = "all" in cmd.lower()
        d = nhl_effective_date()
        lines = get_games_with_finns(d, include_future=include_future)
        if not lines:
            send_telegram("Ei otteluita.", chat_id)
        else:
            send_telegram("📅 NHL-ottelut:\n\n" + "\n\n".join(lines), chat_id)
        return

    # STANDINGS
    if c.startswith("/standings"):
        divs = get_standings()
        if not divs:
            send_telegram("Ei saatu standings-tietoja.", chat_id)
            return

        out = "🏆 NHL Standings (Divisions)\n"
        for div, teams in divs.items():
            out += f"\n📌 {div}\n"
            for name, pts, w, l, ot in teams:
                out += f"  • {name}: {pts}p ({w}-{l}-{ot})\n"

        send_telegram(out, chat_id)
        return

    # UNKNOWN COMMAND
    send_telegram("Tuntematon komento.\nKokeile: /ping /players /games /suomalaiset /stats /test /standings", chat_id)

# ---------------------------------------------------------------------------
# Poll telegram commands
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
# 08:00 Finnish nightly report
# ---------------------------------------------------------------------------
def send_nightly_finns_once():
    last = get_setting("last_stats_date")
    target = nhl_effective_date()

    if last == target:
        return

    now = now_local()
    if now.hour > NIGHTLY_STATS_HOUR or (now.hour == NIGHTLY_STATS_HOUR and now.minute >= NIGHTLY_STATS_MINUTE):
        # Retry logic (Railway Free can drop DNS occasionally)
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

