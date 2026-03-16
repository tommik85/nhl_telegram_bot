# -*- coding: utf-8 -*-
import os
import time
import logging
import sqlite3
import feedparser
import requests
import urllib.parse
from datetime import datetime, timedelta
from dateutil import tz
import pytz

# ---------------------------
# Peruskonfiguraatio
# ---------------------------
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki")
DB_PATH = os.getenv("DB_PATH", "nhlbot.db")

# X/Twitter-asetukset
TWITTER_MODE = os.getenv("TWITTER_MODE", "snscrape").lower()  # 'api' tai 'snscrape'
TWITTER_BEARER = os.getenv("TWITTER_BEARER")  # tarvitaan jos TWITTER_MODE='api'

# (Voit muokata listaa omiin suosikkeihin)
TWITTER_USERS = [
    "FriedgeHNIC",      # Elliotte Friedman
    "reporterchris",    # Chris Johnston
    "DarrenDreger",     # Darren Dreger
    "PierreVLeBrun",    # Pierre LeBrun
    "frank_seravalli",  # Frank Seravalli
    "RussoHockey",      # Michael Russo
]

# RSS-lähteet: mukana kotimaiset + ulkomaiset
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

# Telegram
TG_API_BASE = f"https://api.telegram.org/bot{TOKEN}"

# Pollausvälit (sekunteina)
RSS_POLL_SECONDS = 180           # 3 min
TWITTER_POLL_SECONDS = 300       # 5 min
ERROR_BACKOFF_SECONDS = 15
HTTP_TIMEOUT = 15

# Aika, jolloin lähetetään "viime yön suomalaiset" (24h)
NIGHTLY_STATS_HOUR = 13
NIGHTLY_STATS_MINUTE = 47

# ---------------------------
# Lokitus
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------
# DB-apurit
# ---------------------------
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    with db_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_items (
                url TEXT PRIMARY KEY,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS twitter_since (
                handle TEXT PRIMARY KEY,
                since_id TEXT
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
        try:
            conn.execute("INSERT OR IGNORE INTO seen_items(url) VALUES (?)", (url,))
            conn.commit()
        except Exception as e:
            logging.warning(f"mark_seen error: {e}")

def get_setting(key: str) -> str | None:
    with db_conn() as conn:
        cur = conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def set_setting(key: str, value: str):
    with db_conn() as conn:
        conn.execute("INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        conn.commit()

def get_since_id(handle: str) -> str | None:
    with db_conn() as conn:
        cur = conn.execute("SELECT since_id FROM twitter_since WHERE handle=?", (handle,))
        row = cur.fetchone()
        return row[0] if row else None

def set_since_id(handle: str, since_id: str):
    with db_conn() as conn:
        conn.execute("INSERT INTO twitter_since(handle, since_id) VALUES(?, ?) ON CONFLICT(handle) DO UPDATE SET since_id=excluded.since_id", (handle, since_id))
        conn.commit()

# ---------------------------
# Hyötyfunktiot
# ---------------------------
def normalize_url(url: str) -> str:
    """Poista yleiset seurantaparametrit duplikaattien välttämiseksi."""
    try:
        parsed = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in q if not k.lower().startswith(("utm_", "fbclid", "gclid", "mc_eid"))]
        new_query = urllib.parse.urlencode(filtered)
        normalized = urllib.parse.urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            new_query,
            ""  # fragment pois
        ))
        return normalized
    except Exception:
        return url

def send_telegram(text: str):
    if not TOKEN or not CHAT_ID:
        logging.error("TOKEN/CHAT_ID puuttuu.")
        return
    try:
        url = f"{TG_API_BASE}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": text}, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            logging.warning(f"Telegram sendMessage status {resp.status_code}: {resp.text}")
    except Exception as e:
        logging.error(f"Telegram-virhe: {e}")

def now_local():
    return datetime.now(pytz.timezone(TIMEZONE))

# ---------------------------
# RSS-haku
# ---------------------------
def poll_rss():
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if getattr(feed, 'bozo', False):
                logging.warning(f"RSS bozo {feed_url}: {getattr(feed, 'bozo_exception', None)}")

            for entry in feed.entries:
                
                pub = getattr(entry, "published", None) or getattr(entry, "updated", None)
                if not is_recent(pub, hours=48):
                    continue
                    
                link = normalize_url(getattr(entry, "link", None) or "")
                title = getattr(entry, "title", "").strip()
                if not link or not title:
                    continue
                if has_seen(link):
                    continue

                message = f"🚨 NHL-UUTISET\n\n{title}\n{link}"
                send_telegram(message)
                mark_seen(link)

        except Exception as e:
            logging.error(f"RSS-virhe {feed_url}: {e}")
            time.sleep(ERROR_BACKOFF_SECONDS)

# ---------------------------
# X/Twitter
# ---------------------------
def poll_twitter():
    if TWITTER_MODE == "api":
        poll_twitter_api()
    else:
        # oletus: snscrape, koska se ei vaadi virallista tokenia
        poll_twitter_snscrape()

def poll_twitter_api():
    """Virallinen X API v2: edellyttää TWITTER_BEARER-tokenia."""
    if not TWITTER_BEARER:
        logging.warning("TWITTER_BEARER puuttuu, ohitetaan virallinen API.")
        return

    headers = {"Authorization": f"Bearer {TWITTER_BEARER}"}
    base_v2 = "https://api.twitter.com/2"

    for handle in TWITTER_USERS:
        try:
            # 1) hae user-id
            u = requests.get(f"{base_v2}/users/by/username/{handle}", headers=headers, timeout=HTTP_TIMEOUT)
            if u.status_code != 200:
                logging.warning(f"Twitter user {handle} status {u.status_code}: {u.text}")
                continue
            user = u.json().get("data", {})
            user_id = user.get("id")
            if not user_id:
                continue

            params = {
                "max_results": 20,
                "exclude": "retweets,replies",
                "tweet.fields": "created_at"
            }
            since_id = get_since_id(handle)
            if since_id:
                params["since_id"] = since_id

            t = requests.get(f"{base_v2}/users/{user_id}/tweets", headers=headers, params=params, timeout=HTTP_TIMEOUT)
            if t.status_code != 200:
                logging.warning(f"Twitter timeline {handle} status {t.status_code}: {t.text}")
                continue

            data = t.json().get("data", [])
            if not data:
                continue

            # Lajittele vanhimmasta uusimpaan, jotta viestit tulevat oikeassa järjestyksessä
            data.sort(key=lambda x: x.get("id"))

            for tweet in data:
                tid = tweet.get("id")
                text = tweet.get("text", "").strip()
                url = f"https://x.com/{handle}/status/{tid}"

                if not has_seen(url):
                    msg = f"🐦 X (Twitter) — {handle}\n\n{text}\n{url}"
                    send_telegram(msg)
                    mark_seen(url)

                set_since_id(handle, tid)

        except Exception as e:
            logging.error(f"Twitter API virhe {handle}: {e}")
            time.sleep(ERROR_BACKOFF_SECONDS)

def poll_twitter_snscrape():
    """Käyttää snscrapea (python-kirjasto). Tarvitsee: pip install snscrape"""
    try:
        import snscrape.modules.twitter as sntwitter
    except Exception:
        logging.warning("snscrape ei asennettu (pip install snscrape). Ohitetaan X/Twitter.")
        return

    for handle in TWITTER_USERS:
        try:
            since_id = get_since_id(handle)
            # Hae enintään 30 uusinta twiittiä
            tweets = []
            for i, tweet in enumerate(sntwitter.TwitterUserScraper(handle).get_items()):
                if i >= 30:
                    break
                # suodata retweetit ja replyt
                if getattr(tweet, "retweetedTweet", None):
                    continue
                if getattr(tweet, "inReplyToTweetId", None):
                    continue
                tweets.append(tweet)

            # Lajittele vanhimmasta uusimpaan
            tweets.sort(key=lambda t: int(t.id))

            for tw in tweets:
                tid = str(tw.id)
                # since_id-kontrolli (jos asetettu)
                if since_id and int(tid) <= int(since_id):
                    continue

                text = (tw.rawContent or "").strip()
                url = f"https://x.com/{handle}/status/{tid}"

                if not has_seen(url):
                    msg = f"🐦 X (Twitter) — {handle}\n\n{text}\n{url}"
                    send_telegram(msg)
                    mark_seen(url)

                set_since_id(handle, tid)

        except Exception as e:
            logging.error(f"snscrape virhe {handle}: {e}")
            time.sleep(ERROR_BACKOFF_SECONDS)

# ---------------------------
# Suomalaisten pisteet (NHL Stats API)
# ---------------------------
def nhl_schedule(date_str: str):
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?date={date_str}"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def nhl_boxscore(game_pk: int):
    url = f"https://statsapi.web.nhl.com/api/v1/game/{game_pk}/boxscore"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def last_completed_nhl_date() -> str:
    """
    Yksinkertainen heuristiikka: etsi tuorein päivä (tänään-1, sitten tänään-2), jolla on finalisoituja pelejä.
    Tämä vastaa tyypillisesti 'viime yön' kierrosta Suomessa.
    """
    today_local = now_local().date()
    candidates = [today_local - timedelta(days=1), today_local - timedelta(days=2)]
    for d in candidates:
        ds = d.strftime("%Y-%m-%d")
        try:
            sched = nhl_schedule(ds)
            dates = sched.get("dates", [])
            if not dates:
                continue
            games = dates[0].get("games", [])
            finals = [g for g in games if g.get("status", {}).get("detailedState") in ("Final", "Final/OT", "Final/SO")]
            if finals:
                return ds
        except Exception as e:
            logging.warning(f"schedule virhe {ds}: {e}")
            continue
    # varalla: eilinen string
    return (today_local - timedelta(days=1)).strftime("%Y-%m-%d")

def _fmt_toi(v: str | None) -> str:
    return v or "00:00"

def _fmt_pp_sh(v: str | None) -> str:
    # powerPlayTimeOnIce / shortHandedTimeOnIce voivat olla tyhjiä -> normalisoidaan
    return v or "0:00"

def _append_stat(parts, label, value, suffix=""):
    if value is None:
        return
    if isinstance(value, str) and not value:
        return
    parts.append(f"{label} {value}{suffix}")

def fetch_finnish_points_for_date(date_str: str):
    sched = nhl_schedule(date_str)
    dates = sched.get("dates", [])
    if not dates:
        return []

    games = dates[0].get("games", [])
    fins = []
    for g in games:
        game_pk = g.get("gamePk")
        if not game_pk:
            continue
        try:
            box = nhl_boxscore(game_pk)
            for side in ("home", "away"):
                team = box.get("teams", {}).get(side, {})
                players = team.get("players", {}) or {}
                team_name = team.get("team", {}).get("name", "")
                for pkey, pdata in players.items():
                    person = pdata.get("person", {}) or {}
                    nationality = (person.get("nationality") or "").upper()
                    if nationality != "FIN":
                        continue

                    name = person.get("fullName") or "N/A"
                    stats = pdata.get("stats", {}) or {}
                    skater = stats.get("skaterStats")
                    goalie = stats.get("goalieStats")

                    if skater:
                        g_ = int(skater.get("goals") or 0)
                        a_ = int(skater.get("assists") or 0)
                        p_ = g_ + a_
                        toi = _fmt_toi(skater.get("timeOnIce"))
                        plusminus = skater.get("plusMinus")
                        shots = skater.get("shots")
                        pim = skater.get("penaltyMinutes")
                        hits = skater.get("hits")
                        blocks = skater.get("blocked")
                        fow = skater.get("faceOffWins")
                        fot = skater.get("faceoffTaken")
                        pp_toi = _fmt_pp_sh(skater.get("powerPlayTimeOnIce"))
                        sh_toi = _fmt_pp_sh(skater.get("shortHandedTimeOnIce"))

                        header = f"{name} ({team_name}) {g_}+{a_}={p_}, ±{plusminus if plusminus is not None else 0}, TOI {toi}"
                        notes = []
                        _append_stat(notes, "Laukaukset", shots)
                        if fow is not None and fot:
                            try:
                                notes.append(f"Aloitukset {int(fow)}/{int(fot)}")
                            except Exception:
                                pass
                        _append_stat(notes, "Jäähyt", pim, " min")
                        _append_stat(notes, "Taklaukset", hits)
                        _append_stat(notes, "Blokit", blocks)
                        if pp_toi != "0:00":
                            notes.append(f"YV {pp_toi}")
                        if sh_toi != "0:00":
                            notes.append(f"AV {sh_toi}")

                        fins.append((p_, name, header, notes))

                    elif goalie:
                        svs = int(goalie.get("saves") or 0)
                        sa = int(goalie.get("shotsAgainst") or 0)
                        ga = int(goalie.get("goalsAgainst") or 0)
                        toi = _fmt_toi(goalie.get("timeOnIce"))
                        pp_saves = goalie.get("powerPlaySaves")
                        sh_saves = goalie.get("shortHandedSaves")
                        ev_saves = goalie.get("evenSaves")
                        sv_pct = None
                        if sa and sa > 0:
                            sv_pct = round(svs / sa, 3)

                        header = f"{name} ({team_name})\nMV: {svs}/{sa} torjuntaa, SV% {sv_pct if sv_pct is not None else '—'}, TOI {toi}"
                        notes = []
                        if ev_saves is not None and isinstance(ev_saves, (int, float)):
                            notes.append(f"EV {int(ev_saves)}/{sa - int(pp_saves or 0) - int(sh_saves or 0) if sa is not None else ''}")
                        if pp_saves is not None:
                            notes.append(f"YV {int(pp_saves)}/{int(pp_saves) + int(goalie.get('powerPlayShotsAgainst') or 0) if goalie.get('powerPlayShotsAgainst') is not None else ''}")
                        if sh_saves is not None:
                            notes.append(f"AV {int(sh_saves)}/{int(sh_saves) + int(goalie.get('shortHandedShotsAgainst') or 0) if goalie.get('shortHandedShotsAgainst') is not None else ''}")

                        fins.append((0, name, header, notes))
        except Exception as e:
            logging.warning(f"Boxscore {game_pk} virhe: {e}")
            continue

    # Järjestä pisteiden mukaan, sitten nimen mukaan
    fins.sort(key=lambda x: (-x[0], x[1]))

    # Muotoillaan viestirivit
    lines = []
    for _, __, header, notes in fins:
        if notes:
            lines.append(f"• {header}\n  " + " | ".join(notes))
        else:
            lines.append(f"• {header}")
    return lines

def send_nightly_finns_once():
    """Lähetä kerran päivässä klo HH:MM paikallista aikaa."""
    last_sent = get_setting("last_stats_date")
    target_date = last_completed_nhl_date()
    if last_sent == target_date:
        return  # jo lähetetty tälle yölle

    # Lähetetään vain ajastettuna aikana
    now = now_local()
    if now.hour > NIGHTLY_STATS_HOUR or (
        now.hour == NIGHTLY_STATS_HOUR and now.minute >= NIGHTLY_STATS_MINUTE
    ):
        fins = fetch_finnish_points_for_date(target_date)
        if fins:
            header = f"🇫🇮 Viime yön suomalaiset (NHL) — {target_date}"
            send_telegram(header + "\n\n" + "\n\n".join(fins))
        else:
            send_telegram(
                f"🇫🇮 Viime yön suomalaiset (NHL) — {target_date}\n\nEi suomalaispisteitä tai datassa häiriö."
            )
        set_setting("last_stats_date", target_date)

# ---------------------------
# Main loop
# ---------------------------
def main():
    init_db()
    send_test_finns_safe()   # ← TURVALLINEN versio
    logging.info("NHL-uutisvahti käynnissä.")

    last_rss = 0
    last_twitter = 0

    while True:
        now_ts = time.time()
        try:
            # RSS
            if now_ts - last_rss >= RSS_POLL_SECONDS:
                poll_rss()
                last_rss = now_ts

            # Twitter
            if TWITTER_USERS and (now_ts - last_twitter >= TWITTER_POLL_SECONDS):
                poll_twitter()
                last_twitter = now_ts

            # Suomalaisten pisteet — kerran päivässä määritettyyn aikaan
            send_nightly_finns_once()

            time.sleep(2)
        except KeyboardInterrupt:
            logging.info("Keskeytetty käyttäjän toimesta.")
            break
        except Exception as e:
            logging.error(f"Pääsilmukan virhe: {e}")
            time.sleep(ERROR_BACKOFF_SECONDS)

def send_test_finns_safe():
    try:
        date = last_completed_nhl_date()
        fins = fetch_finnish_points_for_date(date)
        if fins:
            send_telegram("🔧 TESTI – Suomalaisraportti\n\n" + "\n\n".join(fins))
        else:
            send_telegram("🔧 TESTI – Ei suomalaispisteitä tai API-ongelma.")
    except Exception as e:
        send_telegram(f"🔧 TESTI VIRHE: {e}")

if __name__ == "__main__":
    main()
