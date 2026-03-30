# --- IPv4 FORCE (Termux/mobile gateway reliability) ---
import socket
_real_getaddrinfo = socket.getaddrinfo
def _v4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _real_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _v4_only_getaddrinfo

import os
import re
import time
import json
import sqlite3
import sys
import signal
from dataclasses import dataclass
from typing import Iterable, Optional, Dict, Tuple, Any, List
from datetime import datetime, timezone, timedelta

import aiohttp
import discord
from discord.ext import tasks


# ==========================================================
# ENV
# ==========================================================
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DEFAULT_CHANNEL_ID = int(os.environ["DISCORD_CHANNEL_ID"])

DB_PATH = os.environ.get("DB_PATH", "state.sqlite3")

FOREXFACTORY_JSON_URL = os.environ.get(
    "FOREXFACTORY_JSON_URL",
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
)

# Optional: alternate/mirror JSON endpoint (if you have one)
FOREXFACTORY_FALLBACK_JSON_URL = os.environ.get("FOREXFACTORY_FALLBACK_JSON_URL", "").strip()

GLINT_FEED_URL = os.environ.get(
    "GLINT_FEED_URL",
    "https://api-v2.glint.trade/api/feed/v2?limit=30",
)

# FF_POLL_SECONDS now only influences how often we *check* for alerts, not how often we fetch upstream.
FF_POLL_SECONDS = int(os.environ.get("FF_POLL_SECONDS", "60"))
GLINT_POLL_SECONDS = int(os.environ.get("GLINT_POLL_SECONDS", "60"))
FF_PREALERT_MINUTES = int(os.environ.get("FF_PREALERT_MINUTES", "15"))

# You said ForexFactory times are IST (Asia/Kolkata).
FF_TIMEZONE = os.environ.get("FF_TIMEZONE", "IST").strip().upper()

KEYWORDS = [k.strip() for k in os.environ.get("KEYWORDS", "").split(",") if k.strip()]
ALIASES = [k.strip() for k in os.environ.get("ALIASES", "").split(",") if k.strip()]

GLINT_HEADERS = {
    "Accept": os.getenv("GLINT_ACCEPT", "application/json"),
    "Authorization": os.getenv("GLINT_AUTHORIZATION", ""),
    "Origin": os.getenv("GLINT_ORIGIN", "https://glint.trade"),
    "Referer": os.getenv("GLINT_REFERER", "https://glint.trade/"),
    "User-Agent": "Mozilla/5.0",
}

OG_FETCH_ENABLED = os.environ.get("OG_FETCH_ENABLED", "1").strip().lower() not in {"0", "false"}
OG_FETCH_TIMEOUT_SECONDS = float(os.environ.get("OG_FETCH_TIMEOUT_SECONDS", "2.5"))
OG_CACHE_TTL_SECONDS = int(os.environ.get("OG_CACHE_TTL_SECONDS", "3600"))
OG_SKIP_DOMAINS = {"twitter.com", "x.com", "t.co"}

MAX_SEND_PER_TICK = int(os.environ.get("MAX_SEND_PER_TICK", "4"))
SEND_DELAY_SECONDS = float(os.environ.get("SEND_DELAY_SECONDS", "1.1"))

FF_BACKOFF_MIN_SECONDS = int(os.environ.get("FF_BACKOFF_MIN_SECONDS", "120"))
FF_BACKOFF_MAX_SECONDS = int(os.environ.get("FF_BACKOFF_MAX_SECONDS", "1800"))
FF_BACKOFF_JITTER_SECONDS = int(os.environ.get("FF_BACKOFF_JITTER_SECONDS", "10"))

# Stronger cache + minimum fetch interval (reduces 429s)
FF_CACHE_TTL_SECONDS = int(os.environ.get("FF_CACHE_TTL_SECONDS", "900"))  # 15 min default
FF_MIN_FETCH_INTERVAL_SECONDS = int(os.environ.get("FF_MIN_FETCH_INTERVAL_SECONDS", "300"))  # 5 min default

# Startup grace to avoid instant refetch storms on restarts/reconnects
FF_STARTUP_GRACE_SECONDS = int(os.environ.get("FF_STARTUP_GRACE_SECONDS", "120"))

BOT_BRAND = os.environ.get("BOT_BRAND", "atlas-news")
BOT_FOOTER = os.environ.get("BOT_FOOTER", "Strict USD/FX • Viewer-local timestamps • Low-noise, high-signal")
SHOW_REASON_FIELD = os.environ.get("SHOW_REASON_FIELD", "1").strip().lower() not in {"0", "false"}

FF_USE_THREADS = os.environ.get("FF_USE_THREADS", "1").strip().lower() not in {"0", "false"}

# optional: at startup also sync to this guild (instant) for testing
SYNC_GUILD_ID = int(os.environ.get("SYNC_GUILD_ID", "0") or "0")

# Restart behavior
# - exit: cleanly exit; your wrapper script should relaunch
# - exec: os.execv() the python process (works if argv is stable)
RESTART_MODE = os.environ.get("RESTART_MODE", "exit").strip().lower()
RESTART_DELAY_SECONDS = float(os.environ.get("RESTART_DELAY_SECONDS", "0.6"))


# ==========================================================
# UTIL
# ==========================================================
def discord_ts(unix_ts: int, style: str = "f") -> str:
    return f"<t:{int(unix_ts)}:{style}>"

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_unix() -> int:
    return int(time.time())

def url_domain(url: str) -> str:
    try:
        m = re.search(r"https?://([^/]+)/", (url or "") + "/")
        return (m.group(1) or "").lower()
    except Exception:
        return ""

def norm_text(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def has_any(text: str, terms: List[str]) -> bool:
    tl = (text or "").lower()
    return any(w in tl for w in terms)

def pick_url(*vals) -> str:
    for v in vals:
        if isinstance(v, str) and v.startswith(("http://", "https://")):
            return v
    return ""

def utc_day_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")

def utc_date_from_unix(ts: int) -> datetime.date:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).date()


# ==========================================================
# SIGNAL TERMS
# ==========================================================
CRITICAL_WORDS = ["critical", "breaking", "urgent", "alert", "emergency"]

MACRO_HIGH_WORDS = [
    "fomc", "federal reserve", "powell", "rate decision", "rate hike", "rate cut",
    "interest rate", "dot plot", "cpi", "core cpi", "ppi", "pce", "core pce",
    "nfp", "nonfarm", "jobs report", "unemployment", "jobless claims", "gdp",
    "retail sales", "ism", "payrolls", "treasury auction", "bond auction",
    "2y", "10y", "yields", "yield", "dxy", "dollar index",
]

FX_CONTEXT_TERMS = [
    "usd", "u.s. dollar", "dollar", "dxy", "forex", "fx",
    "treasury", "treasuries", "yield", "yields", "rates", "interest rate",
]

GEO_CORE_TERMS = ["israel", "iran", "gaza", "red sea", "strait of hormuz", "yemen"]
GEO_LINK_TERMS = ["oil", "brent", "wti", "energy", "shipping", "sanction", "sanctions",
                  "attack", "strike", "missile", "war", "conflict", "escalation", "ceasefire"]

BLOCKLIST_WORDS_DEFAULT = [
    "dyson", "amazon", "prime", "sale", "deal", "deals", "coupon", "promo",
    "promotion", "discount", "% off", "save ", "shopping", "buy now", "gift",
    "review", "appliance", "vacuum", "celebrity", "influencer", "viral",
]
PERSONALITY_BLOCK_DEFAULT = ["trump"]


# ==========================================================
# OPTIONAL MATCHER (KEYWORDS/ALIASES)
# ==========================================================
def compile_matcher():
    tokens = KEYWORDS + ALIASES
    if not tokens:
        return re.compile(r"$^")
    pattern = "|".join(re.escape(t) for t in sorted(tokens, key=len, reverse=True))
    return re.compile(pattern, re.IGNORECASE)

MATCH_RE = compile_matcher()

def matches(text: str) -> bool:
    # If no keywords configured, allow everything
    if MATCH_RE.pattern == r"$^":
        return True
    return bool(MATCH_RE.search(text or ""))


# ==========================================================
# MODEL
# ==========================================================
@dataclass(frozen=True)
class AlertItem:
    source: str          # "glint" | "ff"
    uid: str
    title: str
    url: str
    description: str = ""
    event_unix: int = 0
    severity: str = "high"     # "critical" | "high"
    kind: str = "alert"        # "schedule" | "prealert" | "alert"
    image_url: str = ""
    reason: str = ""
    dedupe_key: str = ""
    ff_currency: str = ""
    ff_impact: str = ""
    ff_forecast: str = ""
    ff_previous: str = ""
    ff_date_label: str = ""


# ==========================================================
# DB
# ==========================================================
def db_connect():
    return sqlite3.connect(DB_PATH, timeout=10)

def migrate_db():
    con = db_connect()
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    cur.execute("CREATE TABLE IF NOT EXISTS sent (source TEXT, uid TEXT, kind TEXT, sent_at INTEGER, PRIMARY KEY(source, uid, kind))")
    cur.execute("CREATE TABLE IF NOT EXISTS sent_norm (source TEXT, norm_key TEXT, sent_at INTEGER, PRIMARY KEY(source, norm_key))")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_settings(
          guild_id INTEGER NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY(guild_id, key)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_filters(
          guild_id INTEGER NOT NULL,
          kind TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY(guild_id, kind, value)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ff_threads(
          guild_id INTEGER NOT NULL,
          event_uid TEXT NOT NULL,
          thread_id INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(guild_id, event_uid)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ff_daily(
          guild_id INTEGER NOT NULL,
          day_utc TEXT NOT NULL,
          posted_at INTEGER NOT NULL,
          PRIMARY KEY(guild_id, day_utc)
        )
        """
    )

    con.commit()
    con.close()

def sent(source: str, uid: str, kind: str) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent WHERE source=? AND uid=? AND kind=? LIMIT 1", (source, uid, kind))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def mark_sent(source: str, uid: str, kind: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO sent(source, uid, kind, sent_at) VALUES(?,?,?,?)",
                (source, uid, kind, now_utc_unix()))
    con.commit()
    con.close()

def sent_norm(source: str, norm_key: str) -> bool:
    if not norm_key:
        return False
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent_norm WHERE source=? AND norm_key=? LIMIT 1", (source, norm_key))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def mark_sent_norm(source: str, norm_key: str):
    if not norm_key:
        return
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO sent_norm(source, norm_key, sent_at) VALUES(?,?,?)",
                (source, norm_key, now_utc_unix()))
    con.commit()
    con.close()

def gs_set(guild_id: int, key: str, value: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO guild_settings(guild_id, key, value) VALUES(?,?,?)",
                (guild_id, key, value))
    con.commit()
    con.close()

def gs_get(guild_id: int, key: str, default: str = "") -> str:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT value FROM guild_settings WHERE guild_id=? AND key=? LIMIT 1", (guild_id, key))
    row = cur.fetchone()
    con.close()
    return row[0] if row else default

def gs_del(guild_id: int, key: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("DELETE FROM guild_settings WHERE guild_id=? AND key=?", (guild_id, key))
    con.commit()
    con.close()

def gf_add(guild_id: int, kind: str, value: str):
    v = value.strip().lower()
    if not v:
        return
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO guild_filters(guild_id, kind, value) VALUES(?,?,?)", (guild_id, kind, v))
    con.commit()
    con.close()

def gf_del(guild_id: int, kind: str, value: str):
    v = value.strip().lower()
    con = db_connect()
    cur = con.cursor()
    cur.execute("DELETE FROM guild_filters WHERE guild_id=? AND kind=? AND value=?", (guild_id, kind, v))
    con.commit()
    con.close()

def gf_list(guild_id: int, kind: str) -> List[str]:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT value FROM guild_filters WHERE guild_id=? AND kind=? ORDER BY value", (guild_id, kind))
    rows = [r[0] for r in cur.fetchall()]
    con.close()
    return rows

def ff_thread_get(guild_id: int, event_uid: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT thread_id FROM ff_threads WHERE guild_id=? AND event_uid=? LIMIT 1", (guild_id, event_uid))
    row = cur.fetchone()
    con.close()
    return int(row[0]) if row else 0

def ff_thread_set(guild_id: int, event_uid: str, thread_id: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO ff_threads(guild_id, event_uid, thread_id, updated_at) VALUES(?,?,?,?)",
        (guild_id, guild_id and event_uid, int(thread_id), now_utc_unix()),
    )
    con.commit()
    con.close()

def ff_daily_was_posted(guild_id: int, day_utc: str) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM ff_daily WHERE guild_id=? AND day_utc=? LIMIT 1", (guild_id, day_utc))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def ff_daily_mark_posted(guild_id: int, day_utc: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO ff_daily(guild_id, day_utc, posted_at) VALUES(?,?,?)",
                (guild_id, day_utc, now_utc_unix()))
    con.commit()
    con.close()

def K_CHANNEL(source: str, severity: str) -> str: return f"channel:{source}:{severity}"
def K_ROLE(source: str, severity: str) -> str: return f"role:{source}:{severity}"
def K_MODE() -> str: return "mode"
def K_SILENT() -> str: return "silent"
def K_ENABLED() -> str: return "enabled"


# ==========================================================
# HTTP + OG cache
# ==========================================================
class Http429(Exception):
    def __init__(self, retry_after: Optional[int] = None):
        super().__init__("Too Many Requests")
        self.retry_after = retry_after

class OgCache:
    def __init__(self):
        self._cache: Dict[str, Tuple[float, str]] = {}

    def get(self, url: str) -> str:
        ent = self._cache.get(url)
        if not ent:
            return ""
        exp, val = ent
        if exp < time.time():
            self._cache.pop(url, None)
            return ""
        return val

    def set(self, url: str, value: str):
        self._cache[url] = (time.time() + OG_CACHE_TTL_SECONDS, value)

OG_CACHE = OgCache()

async def fetch_json(session: aiohttp.ClientSession, url: str, headers=None, timeout_s: float = 25.0):
    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
        if r.status == 429:
            ra = r.headers.get("Retry-After")
            retry_after = None
            if ra:
                try:
                    retry_after = int(float(ra))
                except Exception:
                    retry_after = None
            raise Http429(retry_after=retry_after)
        r.raise_for_status()
        return await r.json()

async def fetch_text(session: aiohttp.ClientSession, url: str, timeout_s: float) -> str:
    async with session.get(
        url,
        timeout=aiohttp.ClientTimeout(total=timeout_s),
        headers={"User-Agent": "Mozilla/5.0"},
    ) as r:
        if r.status == 429:
            raise Http429()
        r.raise_for_status()
        return await r.text()

def extract_meta_image(html: str) -> str:
    if not html:
        return ""
    pats = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image:src["\'][^>]+content=["\']([^"\']+)["\']',
    ]
    for p in pats:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""

async def best_image_url(session: aiohttp.ClientSession, direct: str, page_url: str) -> str:
    if direct:
        return direct
    if not OG_FETCH_ENABLED:
        return ""
    if not page_url or not page_url.startswith(("http://", "https://")):
        return ""
    dom = url_domain(page_url)
    if dom in OG_SKIP_DOMAINS:
        return ""
    cached = OG_CACHE.get(page_url)
    if cached != "":
        return cached
    try:
        html = await fetch_text(session, page_url, OG_FETCH_TIMEOUT_SECONDS)
        img = extract_meta_image(html) or ""
        OG_CACHE.set(page_url, img)
        return img
    except Exception:
        OG_CACHE.set(page_url, "")
        return ""


# ==========================================================
# BACKOFF
# ==========================================================
class Backoff:
    def __init__(self, min_s: int, max_s: int, jitter_s: int):
        self.min_s = max(1, min_s)
        self.max_s = max(self.min_s, max_s)
        self.jitter_s = max(0, jitter_s)
        self._current = 0
        self._until_ts = 0.0

    def active(self) -> bool:
        return time.time() < self._until_ts

    def remaining(self) -> int:
        return max(0, int(self._until_ts - time.time()))

    def reset(self):
        self._current = 0
        self._until_ts = 0.0

    def hit_429(self, retry_after: Optional[int] = None) -> int:
        if retry_after and retry_after > 0:
            wait = min(self.max_s, max(self.min_s, retry_after))
        else:
            self._current = self.min_s if self._current <= 0 else min(self.max_s, self._current * 2)
            wait = self._current
        if self.jitter_s:
            wait = min(self.max_s, wait + (int(time.time()) % (self.jitter_s + 1)))
        self._until_ts = time.time() + wait
        return wait


# ==========================================================
# FF parsing
# ==========================================================
def _ff_tzinfo() -> timezone:
    # You want IST source times (Asia/Kolkata) converted to UTC.
    if FF_TIMEZONE == "IST":
        return timezone(timedelta(hours=5, minutes=30))
    if FF_TIMEZONE == "UTC":
        return timezone.utc
    # fallback: UTC
    return timezone.utc

def parse_ff_dt_to_utc(dt_str: str, tm_str: str) -> Optional[datetime]:
    if not dt_str or not tm_str:
        return None
    low = tm_str.lower().strip()
    if low in {"all day", "tentative"}:
        return None

    # Use current UTC year, but also try +/-1 year to avoid New Year edge cases.
    base_year = datetime.now(timezone.utc).year
    tz = _ff_tzinfo()

    # Example dt_str: "Mar 30", tm_str: "1:30pm" or "1pm"
    for year in (base_year - 1, base_year, base_year + 1):
        raw = f"{dt_str} {year} {tm_str}".strip()
        for fmt in ("%b %d %Y %I:%M%p", "%b %d %Y %I%p"):
            try:
                local_dt = datetime.strptime(raw, fmt).replace(tzinfo=tz)
                # pick the year that produces a datetime closest to "now +/- 400 days"
                return local_dt.astimezone(timezone.utc)
            except ValueError:
                pass

    return None

def _ff_num_to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if float(v).is_integer():
            return str(int(v))
        return str(v)
    if isinstance(v, str):
        return v.strip()
    return str(v)

def _ff_extract_vals(ev: Dict[str, Any]) -> Tuple[str, str]:
    forecast = ev.get("forecast")
    previous = ev.get("previous")
    if forecast is None:
        forecast = ev.get("f")
    if previous is None:
        previous = ev.get("p")
    return _ff_num_to_str(forecast), _ff_num_to_str(previous)

def _ff_event_url(ev: Dict[str, Any]) -> str:
    return ev.get("url") or ev.get("link") or "https://www.forexfactory.com/calendar"

async def ff_get_events_utc_from_url(session: aiohttp.ClientSession, url: str) -> List[AlertItem]:
    data = await fetch_json(session, url)
    out: List[AlertItem] = []
    for ev in (data or []):
        currency = (ev.get("currency") or ev.get("c") or "").upper()
        impact = (ev.get("impact") or ev.get("i") or "").lower()
        title = ev.get("title") or ev.get("event") or ev.get("e") or "Event"
        dt = (ev.get("date") or ev.get("d") or "").strip()
        tm = (ev.get("time") or ev.get("t") or "").strip()

        is_high = ("high" in impact) or (impact in {"3"})
        if currency != "USD" or not is_high:
            continue

        if not matches(f"USD {title}"):
            continue

        uid = str(ev.get("id") or ev.get("eid") or f"{dt}|{tm}|USD|{title}")
        ev_time_utc = parse_ff_dt_to_utc(dt, tm)
        if not ev_time_utc:
            continue

        forecast, previous = _ff_extract_vals(ev)
        link = _ff_event_url(ev)

        out.append(AlertItem(
            source="ff",
            uid=uid,
            title=str(title)[:256],
            url=link,
            description="",
            event_unix=int(ev_time_utc.timestamp()),
            severity="high",
            kind="alert",
            reason="FF: USD High Impact",
            dedupe_key=f"{dt}|{tm}|usd|{norm_text(str(title))}",
            ff_currency="USD",
            ff_impact="High Impact",
            ff_forecast=forecast,
            ff_previous=previous,
            ff_date_label=ev_time_utc.strftime("%a, %b %d"),
        ))
    return out

async def ff_get_events_utc(session: aiohttp.ClientSession) -> List[AlertItem]:
    last_exc: Optional[Exception] = None
    for url in [FOREXFACTORY_JSON_URL, FOREXFACTORY_FALLBACK_JSON_URL]:
        if not url:
            continue
        try:
            return await ff_get_events_utc_from_url(session, url)
        except Exception as e:
            last_exc = e
            if isinstance(e, Http429):
                raise
            continue
    if last_exc:
        raise last_exc
    return []


# ==========================================================
# GLINT parsing
# ==========================================================
async def parse_glint(session: aiohttp.ClientSession) -> List[AlertItem]:
    if not GLINT_HEADERS.get("Authorization"):
        return []
    data = await fetch_json(session, GLINT_FEED_URL, headers=GLINT_HEADERS)
    items = data.get("items") or []
    out: List[AlertItem] = []

    for it in items:
        uid = str(it.get("id") or "")
        if not uid:
            continue

        cats = {c.lower() for c in (it.get("categories") or []) if isinstance(c, str)}

        news = it.get("news") or {}
        tweet = it.get("tweet") or {}
        telegram = it.get("telegram") or {}
        reddit = it.get("reddit") or {}
        osint = it.get("osint") or {}

        title = (news.get("title") or news.get("headline") or reddit.get("title") or "").strip()
        body = (
            news.get("summary")
            or news.get("text")
            or tweet.get("body")
            or tweet.get("text")
            or telegram.get("text")
            or reddit.get("text")
            or osint.get("text")
            or ""
        ).strip()

        url = (tweet.get("link") or "") or (news.get("link") or "") or (news.get("url") or "") or ""
        direct_image_url = pick_url(
            news.get("image"), news.get("image_url"), news.get("thumbnail"),
            tweet.get("image"), tweet.get("image_url"), tweet.get("thumbnail"),
            reddit.get("thumbnail"),
            osint.get("image"), osint.get("image_url"),
        )

        text = (title + "\n" + body).strip()
        tl = text.lower()

        if not matches(text):
            continue

        is_critical = has_any(tl, CRITICAL_WORDS)
        is_fx = has_any(tl, FX_CONTEXT_TERMS) or bool({"fx", "forex", "usd"} & cats)
        is_macro = has_any(tl, MACRO_HIGH_WORDS) or bool({"macroeconomics", "central bank"} & cats)
        is_geo = has_any(tl, GEO_CORE_TERMS) or bool({"geopolitics"} & cats)
        is_geo_link = has_any(tl, GEO_LINK_TERMS)
        is_geo_allowed = is_geo and is_geo_link and is_fx

        if has_any(tl, BLOCKLIST_WORDS_DEFAULT) and not is_critical:
            continue
        if has_any(tl, PERSONALITY_BLOCK_DEFAULT) and not is_critical:
            continue

        passed = False
        severity = "high"
        reason_parts: List[str] = []

        if is_critical:
            passed = True
            severity = "critical"
            reason_parts.append("CRITICAL keyword")
        else:
            if is_macro and is_fx:
                passed = True
                severity = "high"
                reason_parts.extend(["MACRO", "FX context"])
            elif is_geo_allowed:
                passed = True
                severity = "high"
                reason_parts.extend(["GEO->USD", "FX context"])

        if not passed:
            continue

        desc = body.strip()
        if len(desc) > 900:
            desc = desc[:897] + "..."

        prefix = "CRITICAL" if severity == "critical" else "HIGH IMPACT"
        if not title:
            title = (body[:180] if body else "Glint item").strip()

        show_title = f"{prefix} - {title}".strip()[:256]
        norm_key = f"{url_domain(url)}|{norm_text(title)}"
        image_url = await best_image_url(session, direct_image_url, url)

        out.append(AlertItem(
            source="glint",
            uid=uid,
            title=show_title,
            url=url,
            description=desc,
            severity=severity,
            kind="alert",
            image_url=image_url,
            reason=" + ".join(reason_parts),
            dedupe_key=norm_key,
        ))

    return out


# ==========================================================
# SILENT WINDOW (UTC)
# ==========================================================
def in_silent_window(silent_json: str) -> bool:
    if not silent_json:
        return False
    try:
        cfg = json.loads(silent_json)
        start = cfg.get("start", "")
        end = cfg.get("end", "")
        if not start or not end:
            return False
        sh, sm = [int(x) for x in start.split(":")]
        eh, em = [int(x) for x in end.split(":")]
        nowt = datetime.now(timezone.utc).time()
        st = nowt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        et = nowt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if st <= et:
            return st <= nowt <= et
        return nowt >= st or nowt <= et
    except Exception:
        return False


# ==========================================================
# BOT
# ==========================================================
class Bot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = discord.app_commands.CommandTree(self)
        self.session: Optional[aiohttp.ClientSession] = None

        self.ff_backoff = Backoff(FF_BACKOFF_MIN_SECONDS, FF_BACKOFF_MAX_SECONDS, FF_BACKOFF_JITTER_SECONDS)

        self._ff_cache_at: int = 0
        self._ff_cache_events: Optional[List[AlertItem]] = None
        self._ff_cache_error: str = ""

        # single-flight + fetch spacing
        self._ff_inflight_task = None
        self._ff_next_allowed_fetch_ts: int = 0

        # startup grace to avoid immediate fetch storms on restart
        self._started_at_ts: int = now_utc_unix()

        self.last_ff_ok = 0
        self.last_glint_ok = 0
        self.sent_count = 0

        # /fftoday anti-spam (in-memory)
        self._fftoday_last_post: Dict[int, int] = {}

        # /restart guard
        self._restart_lock = False

    async def setup_hook(self):
        migrate_db()
        self.session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"})

        self._register_commands()

        if SYNC_GUILD_ID:
            g = discord.Object(id=SYNC_GUILD_ID)
            try:
                await self.tree.sync(guild=g)
                print(f"Synced commands to guild {SYNC_GUILD_ID}")
            except Exception as e:
                print("Guild sync at startup failed:", repr(e))

        try:
            await self.tree.sync()
            print("Synced global commands")
        except Exception as e:
            print("Global sync at startup failed:", repr(e))

        # Loops
        self.ff_scheduler_loop.start()
        self.glint_loop.start()
        self.ff_daily_loop.start()

    async def on_ready(self):
        print(f"READY as {self.user} (guilds={len(self.guilds)})")

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    def _ff_fetch_status_text(self) -> str:
        if self.ff_backoff.active():
            return f"rate-limited by source (retry in ~{self.ff_backoff.remaining()}s)"
        if self._ff_cache_error:
            return f"fetch error ({self._ff_cache_error})"
        return "unable to fetch"

    async def _ff_events_cached(self, force: bool = False, allow_stale: bool = True) -> Tuple[Optional[List[AlertItem]], str]:
        """
        FF caching + rate-limit safety.
        force=True: bypass startup grace and min-interval (for manual /fftoday).
        allow_stale=False: return None instead of stale cache on failure.
        """
        import asyncio

        if not self.session:
            return None, "no_session"

        now = now_utc_unix()

        # 0) startup grace (skip if forced)
        if (not force) and (now - self._started_at_ts < FF_STARTUP_GRACE_SECONDS):
            if allow_stale and self._ff_cache_events is not None:
                return self._ff_cache_events, "startup_grace_stale_cache"
            return None, "startup_grace_no_cache"

        # 1) fresh cache
        if self._ff_cache_events is not None and (now - self._ff_cache_at) < FF_CACHE_TTL_SECONDS:
            return self._ff_cache_events, ""

        # 2) backoff active
        if self.ff_backoff.active():
            if allow_stale and self._ff_cache_events is not None:
                return self._ff_cache_events, "backoff_stale_cache"
            return None, "backoff_no_cache"

        # 3) min interval (skip if forced)
        if (not force) and (now < self._ff_next_allowed_fetch_ts):
            if allow_stale and self._ff_cache_events is not None:
                return self._ff_cache_events, "min_interval_stale_cache"
            return None, "min_interval_no_cache"

        # 4) single-flight
        if self._ff_inflight_task is not None and not self._ff_inflight_task.done():
            try:
                return await self._ff_inflight_task
            except Exception:
                pass

        async def _do_fetch() -> Tuple[Optional[List[AlertItem]], str]:
            try:
                evs = await ff_get_events_utc(self.session)
                self._ff_cache_events = evs
                self._ff_cache_at = now_utc_unix()
                self._ff_cache_error = ""
                self.ff_backoff.reset()
                self.last_ff_ok = now_utc_unix()
                self._ff_next_allowed_fetch_ts = now_utc_unix() + FF_MIN_FETCH_INTERVAL_SECONDS
                return evs, ""
            except Http429 as e:
                wait = self.ff_backoff.hit_429(retry_after=e.retry_after)
                self._ff_cache_error = f"429 backoff {wait}s"
                self._ff_next_allowed_fetch_ts = now_utc_unix() + max(FF_MIN_FETCH_INTERVAL_SECONDS, wait)
                print(f"FF 429 -> backoff {wait}s (Retry-After={e.retry_after})")
                return (self._ff_cache_events if allow_stale else None), "429"
            except Exception as e:
                self._ff_cache_error = f"{type(e).__name__}: {e}"
                self._ff_next_allowed_fetch_ts = now_utc_unix() + FF_MIN_FETCH_INTERVAL_SECONDS
                print("FF fetch error:", self._ff_cache_error)
                return (self._ff_cache_events if allow_stale else None), "error"

        self._ff_inflight_task = asyncio.create_task(_do_fetch())
        try:
            return await self._ff_inflight_task
        finally:
            if self._ff_inflight_task is not None and self._ff_inflight_task.done():
                self._ff_inflight_task = None

    # ---- interaction helpers ----
    async def _defer(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass

    async def _reply(self, interaction: discord.Interaction, message: str):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
        except Exception:
            try:
                await interaction.followup.send(message, ephemeral=True)
            except Exception:
                pass

    # ---- per-guild settings ----
    def enabled(self, guild_id: int) -> bool:
        return (gs_get(guild_id, K_ENABLED(), "1") or "1").strip() != "0"

    def set_enabled(self, guild_id: int, on: bool):
        gs_set(guild_id, K_ENABLED(), "1" if on else "0")

    def mode(self, guild_id: int) -> str:
        m = (gs_get(guild_id, K_MODE(), "strict") or "strict").strip().lower()
        return m if m in {"strict", "geo", "broad"} else "strict"

    def silent_cfg(self, guild_id: int) -> str:
        return gs_get(guild_id, K_SILENT(), "")

    def route_channel(self, guild_id: int, source: str, severity: str) -> int:
        raw = gs_get(guild_id, K_CHANNEL(source, severity), "")
        if raw.isdigit():
            return int(raw)
        raw2 = gs_get(guild_id, K_CHANNEL(source, "all"), "")
        if raw2.isdigit():
            return int(raw2)
        return DEFAULT_CHANNEL_ID

    def route_role(self, guild_id: int, source: str, severity: str) -> int:
        raw = gs_get(guild_id, K_ROLE(source, severity), "")
        if raw.isdigit():
            return int(raw)
        raw2 = gs_get(guild_id, K_ROLE(source, "all"), "")
        if raw2.isdigit():
            return int(raw2)
        return 0

    # ---- filters ----
    def _guild_allows_domain(self, guild_id: int, domain: str) -> bool:
        return domain.lower() in set(gf_list(guild_id, "allow_domain")) if domain else False

    def _guild_blocks_domain(self, guild_id: int, domain: str) -> bool:
        return domain.lower() in set(gf_list(guild_id, "block_domain")) if domain else False

    def _guild_blocks_keyword(self, guild_id: int, text: str) -> bool:
        bkw = gf_list(guild_id, "block_kw")
        tl = (text or "").lower()
        return any(k in tl for k in bkw)

    def _guild_mode_allows(self, guild_id: int, it: AlertItem) -> bool:
        if it.severity == "critical":
            return True
        if it.source == "ff":
            return True
        m = self.mode(guild_id)
        if m == "strict":
            return ("MACRO" in it.reason and "FX context" in it.reason)
        return True

    # ---- embed/routing ----
    def _mention_for(self, guild_id: int, it: AlertItem) -> str:
        if in_silent_window(self.silent_cfg(guild_id)):
            return ""
        role_id = self.route_role(guild_id, it.source, it.severity)
        return f"<@&{role_id}>" if role_id else ""

    def _channel_for(self, guild_id: int, it: AlertItem) -> int:
        if it.source == "ff":
            return self.route_channel(guild_id, "ff", "high")
        return self.route_channel(guild_id, "glint", it.severity)

    async def _resolve_channel(self, channel_id: int) -> discord.abc.Messageable:
        ch = self.get_channel(channel_id)
        if ch:
            return ch
        return await self.fetch_channel(channel_id)

    def _ff_embed(self, it: AlertItem) -> discord.Embed:
        embed = discord.Embed(
            title=f"High Impact: USD - {it.title}"[:256],
            url=it.url or None,
            color=0xF1C40F,
        )
        embed.add_field(
            name="Date",
            value=it.ff_date_label or datetime.fromtimestamp(it.event_unix, tz=timezone.utc).strftime("%a, %b %d"),
            inline=False,
        )
        # Discord timestamps render in viewer local timezone automatically.
        embed.add_field(
            name="Time (your timezone)",
            value=f"{discord_ts(it.event_unix,'f')}\n{discord_ts(it.event_unix,'R')}",
            inline=False,
        )
        embed.add_field(name="Currency", value=it.ff_currency or "USD", inline=True)
        if it.ff_forecast:
            embed.add_field(name="Forecast", value=it.ff_forecast, inline=True)
        if it.ff_previous:
            embed.add_field(name="Previous", value=it.ff_previous, inline=True)

        embed.timestamp = datetime.now(timezone.utc)
        embed.set_footer(text="Forex Factory • High Impact")
        return embed

    def _glint_embed(self, it: AlertItem) -> discord.Embed:
        color = 0xE74C3C if it.severity == "critical" else 0x9B59B6
        author = "GLINT - CRITICAL" if it.severity == "critical" else "GLINT - HIGH IMPACT"

        desc = (it.description or "").strip()
        if desc and len(desc) > 1200:
            desc = desc[:1197] + "..."

        embed = discord.Embed(
            title=it.title[:256],
            url=(it.url or None),
            description=(desc or None),
            color=color,
        )
        embed.set_author(name=author)

        if SHOW_REASON_FIELD and it.reason:
            embed.add_field(name="Why", value=f"`{it.reason}`", inline=False)
        if it.url:
            embed.add_field(name="Link", value=it.url, inline=False)
        if it.image_url:
            embed.set_image(url=it.image_url)

        embed.timestamp = datetime.now(timezone.utc)
        embed.set_footer(text=f"{BOT_BRAND} • {BOT_FOOTER}")
        return embed

    async def _send_one(self, channel: discord.abc.Messageable, guild_id: int, it: AlertItem):
        mention = self._mention_for(guild_id, it)
        embed = self._ff_embed(it) if it.source == "ff" else self._glint_embed(it)
        await channel.send(content=(mention or None), embed=embed)

    # ---- FF threads ----
    async def _get_or_create_ff_thread(self, guild: discord.Guild, base_channel: discord.TextChannel, it: AlertItem) -> Optional[discord.Thread]:
        if not FF_USE_THREADS:
            return None
        if not isinstance(base_channel, discord.TextChannel):
            return None

        existing_id = ff_thread_get(guild.id, it.uid)
        if existing_id:
            th = guild.get_thread(existing_id)
            if th:
                return th
            try:
                th2 = await self.fetch_channel(existing_id)
                if isinstance(th2, discord.Thread):
                    return th2
            except Exception:
                pass

        starter = await base_channel.send(embed=discord.Embed(
            title=f"Event Thread - {it.title[:200]}",
            description="Updates will be posted here.",
            color=0xF1C40F,
        ))
        thread = await starter.create_thread(name=f"USD High - {it.title[:80]}", auto_archive_duration=60)
        # fix: correct args order
        con = db_connect()
        cur = con.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO ff_threads(guild_id, event_uid, thread_id, updated_at) VALUES(?,?,?,?)",
            (guild.id, it.uid, int(thread.id), now_utc_unix()),
        )
        con.commit()
        con.close()
        return thread

    async def _broadcast(self, guilds: Iterable[discord.Guild], items: List[AlertItem]):
        sent_this_tick = 0
        for guild in guilds:
            if not guild:
                continue
            if not self.enabled(guild.id):
                continue

            for it in items:
                if sent_this_tick >= MAX_SEND_PER_TICK:
                    return

                if it.kind in {"prealert", "alert"}:
                    if sent(it.source, it.uid, it.kind):
                        continue
                    if it.dedupe_key and sent_norm(it.source, it.dedupe_key):
                        continue

                dom = url_domain(it.url)
                raw_text = f"{it.title}\n{it.description}\n{it.url}"

                if not self._guild_allows_domain(guild.id, dom):
                    if self._guild_blocks_domain(guild.id, dom):
                        continue
                    if self._guild_blocks_keyword(guild.id, raw_text):
                        continue

                if not self._guild_mode_allows(guild.id, it):
                    continue

                chan_id = self._channel_for(guild.id, it)
                ch = await self._resolve_channel(chan_id)

                if it.source == "ff" and it.kind in {"prealert", "alert"} and FF_USE_THREADS and isinstance(ch, discord.TextChannel):
                    if it.kind == "prealert":
                        thread = await self._get_or_create_ff_thread(guild, ch, it)
                        if thread:
                            await self._send_one(thread, guild.id, it)
                        else:
                            await self._send_one(ch, guild.id, it)
                    else:
                        tid = ff_thread_get(guild.id, it.uid)
                        if tid:
                            th = guild.get_thread(tid)
                            if th:
                                await self._send_one(th, guild.id, it)
                            else:
                                await self._send_one(ch, guild.id, it)
                        else:
                            await self._send_one(ch, guild.id, it)
                else:
                    await self._send_one(ch, guild.id, it)

                if it.kind in {"prealert", "alert"}:
                    mark_sent(it.source, it.uid, it.kind)
                    mark_sent_norm(it.source, it.dedupe_key or "")
                self.sent_count += 1
                sent_this_tick += 1
                await _sleep(SEND_DELAY_SECONDS)

    # ==========================================================
    # helper to post a day's schedule to a guild
    # ==========================================================
    async def _post_ff_schedule_for_guild(self, guild: discord.Guild, day_utc: str, today_events: List[AlertItem]):
        chan_id = self.route_channel(guild.id, "ff", "high")
        ch = await self._resolve_channel(chan_id)

        if not today_events:
            await ch.send(content=f"USD High Impact schedule for **{day_utc} (UTC)**: **No events today**.")
            return

        header = f"USD High Impact schedule for **{day_utc} (UTC)**"
        await ch.send(content=header)
        for e in sorted(today_events, key=lambda x: x.event_unix):
            it = AlertItem(**{**e.__dict__, "kind": "schedule"})  # type: ignore
            await self._send_one(ch, guild.id, it)
            await _sleep(0.6)

    async def _do_restart(self, interaction: discord.Interaction, reason: str = ""):
        if self._restart_lock:
            await self._reply(interaction, "Restart already in progress.")
            return
        self._restart_lock = True

        await self._reply(interaction, f"Restarting bot... {('(' + reason + ')') if reason else ''}".strip())
        await _sleep(RESTART_DELAY_SECONDS)

        try:
            await self.close()
        except Exception:
            pass

        # Choose restart strategy
        if RESTART_MODE == "exec":
            # Best-effort: replace current process
            py = sys.executable
            argv = [py] + sys.argv
            os.execv(py, argv)

        # Default: exit so wrapper restarts it
        os.kill(os.getpid(), signal.SIGTERM)

    # ---- commands ----
    def _register_commands(self):
        @self.tree.error
        async def on_app_command_error(interaction: discord.Interaction, error: Exception):
            print("APP CMD ERROR:", repr(error))
            try:
                await self._reply(interaction, f"Error: `{type(error).__name__}`")
            except Exception:
                pass

        @self.tree.command(name="restart", description="Owner-only: restart the bot process (Termux).")
        async def restart(interaction: discord.Interaction, reason: str = ""):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            # Owner-only, like /sync
            if interaction.user.id != interaction.guild.owner_id:
                await self._reply(interaction, "Only the server owner can run /restart.")
                return
            await self._do_restart(interaction, reason=reason.strip())

        @self.tree.command(name="sync", description="Owner-only: resync slash commands (guild=instant, global=slow).")
        async def sync(interaction: discord.Interaction, scope: str = "guild"):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            if interaction.user.id != interaction.guild.owner_id:
                await self._reply(interaction, "Only the server owner can run /sync.")
                return

            scope = (scope or "guild").strip().lower()
            if scope not in {"guild", "global"}:
                await self._reply(interaction, "Usage: `/sync guild` or `/sync global`")
                return

            if scope == "guild":
                g = discord.Object(id=interaction.guild.id)
                self.tree.clear_commands(guild=g)
                await self.tree.sync(guild=g)
                await self._reply(interaction, "Synced commands to this server (guild). Commands should appear immediately.")
                return

            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            await self._reply(interaction, "Synced commands globally. This can take 10–60+ minutes to show everywhere.")

        @self.tree.command(name="bot", description="Turn posting on/off for this server.")
        async def bot(interaction: discord.Interaction, action: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            action = action.lower().strip()
            if action not in {"on", "off", "status"}:
                await self._reply(interaction, "Use: /bot on  /bot off  /bot status")
                return
            if action == "status":
                await self._reply(interaction, f"Posting is: `{'ON' if self.enabled(interaction.guild.id) else 'OFF'}`")
                return
            self.set_enabled(interaction.guild.id, action == "on")
            await self._reply(interaction, f"Posting set to: `{'ON' if action=='on' else 'OFF'}`")

        @self.tree.command(name="status", description="Show routing/mode for this server.")
        async def status(interaction: discord.Interaction):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            g = interaction.guild.id
            msg = (
                f"Enabled: `{'ON' if self.enabled(g) else 'OFF'}`\n"
                f"Mode: `{self.mode(g)}`\n"
                f"Silent UTC: `{self.silent_cfg(g) or 'none'}`\n\n"
                f"Routes:\n"
                f"- glint/critical -> <#{self.route_channel(g,'glint','critical')}>\n"
                f"- glint/high -> <#{self.route_channel(g,'glint','high')}>\n"
                f"- ff/high -> <#{self.route_channel(g,'ff','high')}>\n"
            )
            await self._reply(interaction, msg)

        @self.tree.command(name="route", description="Set routing for source+severity (channel + optional role ping).")
        async def route(interaction: discord.Interaction, source: str, severity: str, channel: discord.TextChannel, role: Optional[discord.Role] = None):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            source = source.lower().strip()
            severity = severity.lower().strip()
            if source not in {"glint", "ff"}:
                await self._reply(interaction, "source must be glint or ff")
                return
            if severity not in {"critical", "high", "all"}:
                await self._reply(interaction, "severity must be critical, high, or all")
                return
            gs_set(interaction.guild.id, K_CHANNEL(source, severity), str(channel.id))
            if role:
                gs_set(interaction.guild.id, K_ROLE(source, severity), str(role.id))
            msg = f"Route set: {source}/{severity} -> {channel.mention}"
            if role:
                msg += f" with ping {role.mention}"
            await self._reply(interaction, msg)

        @self.tree.command(name="routeclear", description="Clear routing for source+severity.")
        async def routeclear(interaction: discord.Interaction, source: str, severity: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            source = source.lower().strip()
            severity = severity.lower().strip()
            if source not in {"glint", "ff"} or severity not in {"critical", "high", "all"}:
                await self._reply(interaction, "Bad args. source=glint|ff severity=critical|high|all")
                return
            gs_del(interaction.guild.id, K_CHANNEL(source, severity))
            gs_del(interaction.guild.id, K_ROLE(source, severity))
            await self._reply(interaction, f"Cleared route for {source}/{severity}.")

        @self.tree.command(name="mode", description="Set filter mode: strict, geo, broad.")
        async def mode(interaction: discord.Interaction, mode: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            mode = mode.lower().strip()
            if mode not in {"strict", "geo", "broad"}:
                await self._reply(interaction, "mode must be strict, geo, or broad")
                return
            gs_set(interaction.guild.id, K_MODE(), mode)
            await self._reply(interaction, f"Mode set to `{mode}`.")

        @self.tree.command(name="silent", description="Set a UTC silent window for role pings (still posts). Example: 01:00 06:00")
        async def silent(interaction: discord.Interaction, start: str, end: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            gs_set(interaction.guild.id, K_SILENT(), json.dumps({"start": start, "end": end}))
            await self._reply(interaction, f"Silent window set (UTC): {start}-{end}")

        @self.tree.command(name="silentclear", description="Disable silent window.")
        async def silentclear(interaction: discord.Interaction):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            gs_del(interaction.guild.id, K_SILENT())
            await self._reply(interaction, "Silent window cleared.")

        @self.tree.command(name="block", description="Block a keyword or domain for this server.")
        async def block(interaction: discord.Interaction, kind: str, value: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            kind = kind.lower().strip()
            if kind not in {"keyword", "domain"}:
                await self._reply(interaction, "kind must be keyword or domain")
                return
            gf_add(interaction.guild.id, "block_kw" if kind == "keyword" else "block_domain", value)
            await self._reply(interaction, f"Blocked {kind}: `{value}`")

        @self.tree.command(name="unblock", description="Unblock a keyword or domain for this server.")
        async def unblock(interaction: discord.Interaction, kind: str, value: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            kind = kind.lower().strip()
            if kind not in {"keyword", "domain"}:
                await self._reply(interaction, "kind must be keyword or domain")
                return
            gf_del(interaction.guild.id, "block_kw" if kind == "keyword" else "block_domain", value)
            await self._reply(interaction, f"Unblocked {kind}: `{value}`")

        @self.tree.command(name="allow", description="Allow (whitelist) a domain (takes priority).")
        async def allow(interaction: discord.Interaction, domain: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            gf_add(interaction.guild.id, "allow_domain", domain)
            await self._reply(interaction, f"Allowed domain: `{domain}`")

        @self.tree.command(name="allowclear", description="Remove allowed domain.")
        async def allowclear(interaction: discord.Interaction, domain: str):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            gf_del(interaction.guild.id, "allow_domain", domain)
            await self._reply(interaction, f"Removed allowed domain: `{domain}`")

        @self.tree.command(name="filters", description="Show current blocks/allows.")
        async def filters(interaction: discord.Interaction):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            g = interaction.guild.id
            bkw = gf_list(g, "block_kw")
            bdm = gf_list(g, "block_domain")
            adm = gf_list(g, "allow_domain")
            msg = (
                f"Allow domains: {', '.join(adm) if adm else 'none'}\n"
                f"Block domains: {', '.join(bdm) if bdm else 'none'}\n"
                f"Block keywords: {', '.join(bkw) if bkw else 'none'}"
            )
            await self._reply(interaction, msg)

        @self.tree.command(name="health", description="Bot health/backoff status.")
        async def health(interaction: discord.Interaction):
            await self._defer(interaction)
            now = now_utc_unix()
            ff = discord_ts(self.last_ff_ok, "R") if self.last_ff_ok else "never"
            gl = discord_ts(self.last_glint_ok, "R") if self.last_glint_ok else "never"
            backoff = f"{self.ff_backoff.remaining()}s" if self.ff_backoff.active() else "no"
            cache = "yes" if self._ff_cache_events is not None else "no"
            msg = (
                f"FF last ok: {ff}\n"
                f"Glint last ok: {gl}\n"
                f"FF backoff: {backoff}\n"
                f"FF cache: {cache}\n"
                f"Sent (session): {self.sent_count}\n"
                f"Now (UTC): {discord_ts(now,'F')}"
            )
            await self._reply(interaction, msg)

        @self.tree.command(name="test", description="Send a test alert to verify routing/pings.")
        async def test(interaction: discord.Interaction, source: str = "glint", severity: str = "critical"):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            source = source.lower().strip()
            severity = severity.lower().strip()
            if source not in {"glint", "ff"} or severity not in {"critical", "high"}:
                await self._reply(interaction, "source=glint|ff severity=critical|high")
                return
            now = now_utc_unix()
            it = AlertItem(
                source=source,
                uid=f"test:{now}:{source}:{severity}",
                title=f"TEST - {source.upper()} - {severity.upper()}",
                url="https://example.com",
                description=f"Test generated at {discord_ts(now,'F')} ({discord_ts(now,'R')}).",
                event_unix=(now + 900 if source == "ff" else 0),
                severity=severity,
                kind="alert",
                image_url="",
                reason="test",
                dedupe_key=f"test|{now}",
                ff_currency="USD",
                ff_impact="High Impact",
            )
            await self._broadcast([interaction.guild], [it])
            await self._reply(interaction, "Test sent.")

        @self.tree.command(name="fftoday", description="Post today's USD High Impact ForexFactory schedule now.")
        async def fftoday(interaction: discord.Interaction):
            await self._defer(interaction)
            if not interaction.guild:
                await self._reply(interaction, "Run this in a server.")
                return
            g = interaction.guild.id
            if not self.enabled(g):
                await self._reply(interaction, "Bot is OFF for this server. Use `/bot on` first.")
                return

            nowts = now_utc_unix()
            last = self._fftoday_last_post.get(g, 0)
            if nowts - last < 600:
                await self._reply(interaction, "Please wait 10 minutes before running /fftoday again.")
                return
            self._fftoday_last_post[g] = nowts

            nowdt = now_utc()
            day_utc = nowdt.strftime("%Y-%m-%d")
            day_date_utc = nowdt.date()

            # Force a real fetch for manual command (ignores startup grace + min-interval)
            events, why = await self._ff_events_cached(force=True, allow_stale=False)

            chan_id = self.route_channel(g, "ff", "high")
            ch = await self._resolve_channel(chan_id)

            if not events:
                await ch.send(content=f"USD High Impact schedule for **{day_utc} (UTC)**: **{self._ff_fetch_status_text()}**. (Use `/fftoday` later to retry.)")
                await self._reply(interaction, f"Posted status to <#{chan_id}>.")
                return

            today_events = [e for e in events if utc_date_from_unix(e.event_unix) == day_date_utc]

            await self._post_ff_schedule_for_guild(interaction.guild, day_utc, today_events)
            await self._reply(interaction, f"Posted today's schedule to <#{chan_id}>.")

    # ==========================================================
    # LOOPS
    # ==========================================================

    # FF scheduler loop: checks for -15m and at-time alerts, and does daily schedule after 00:00 UTC
    @tasks.loop(seconds=30)
    async def ff_scheduler_loop(self):
        try:
            if not self.session:
                return

            # Get events (cached fetch obeys TTL/min-interval/backoff)
            events, _ = await self._ff_events_cached(force=False, allow_stale=True)
            if not events:
                return

            now_dt = now_utc()
            day_utc = now_dt.strftime("%Y-%m-%d")
            day_date_utc = now_dt.date()

            # Today's FF events (UTC day)
            today_events = [e for e in events if utc_date_from_unix(e.event_unix) == day_date_utc]

            # Prealert and at-time alerts
            items: List[AlertItem] = []
            for e in today_events:
                ev_dt = datetime.fromtimestamp(e.event_unix, tz=timezone.utc)
                minutes_to = int((ev_dt - now_dt).total_seconds() / 60)

                # -15m to 0 (prealert)
                if 0 <= minutes_to <= FF_PREALERT_MINUTES:
                    items.append(AlertItem(**{**e.__dict__, "kind": "prealert"}))  # type: ignore

                # at-time window (0 to -2 minutes)
                if -2 <= minutes_to <= 0:
                    items.append(AlertItem(**{**e.__dict__, "kind": "alert"}))  # type: ignore

            if items:
                await self._broadcast(self.guilds, items)

            # Daily schedule posting: after 00:00 UTC of the current day (DB guard ensures once/day)
            # We keep it here so it runs soon after midnight even if ff_daily_loop is slow.
            # Only do it when UTC time is >= 00:00 (always true) BUT we want to ensure it posts
            # only for today_utc and only once.
            for guild in self.guilds:
                if not guild:
                    continue
                if not self.enabled(guild.id):
                    continue
                if ff_daily_was_posted(guild.id, day_utc):
                    continue

                # If bot was offline at midnight, it will post when it comes back online.
                await self._post_ff_schedule_for_guild(guild, day_utc, today_events)
                ff_daily_mark_posted(guild.id, day_utc)

        except Exception as e:
            print("FF scheduler error:", repr(e))

    @tasks.loop(seconds=GLINT_POLL_SECONDS)
    async def glint_loop(self):
        try:
            if not self.session:
                return
            items = await parse_glint(self.session)
            if items:
                await self._broadcast(self.guilds, items)
            self.last_glint_ok = now_utc_unix()
        except Http429:
            print("Glint 429 (retry next tick)")
        except Exception as e:
            print("Glint error:", repr(e))

    # Daily catch-up loop (extra safety)
    @tasks.loop(seconds=600)
    async def ff_daily_loop(self):
        try:
            if not self.session:
                return

            nowdt = now_utc()
            day_utc = nowdt.strftime("%Y-%m-%d")
            day_date_utc = nowdt.date()

            events, _ = await self._ff_events_cached(force=False, allow_stale=True)

            today_events: List[AlertItem] = []
            if events:
                today_events = [e for e in events if utc_date_from_unix(e.event_unix) == day_date_utc]

            for guild in self.guilds:
                if not guild:
                    continue
                if not self.enabled(guild.id):
                    continue
                if ff_daily_was_posted(guild.id, day_utc):
                    continue

                chan_id = self.route_channel(guild.id, "ff", "high")
                ch = await self._resolve_channel(chan_id)

                if not events:
                    await ch.send(content=f"USD High Impact schedule for **{day_utc} (UTC)**: **{self._ff_fetch_status_text()}**. (Posted once; use `/fftoday` later to retry.)")
                    ff_daily_mark_posted(guild.id, day_utc)
                    continue

                await self._post_ff_schedule_for_guild(guild, day_utc, today_events)
                ff_daily_mark_posted(guild.id, day_utc)

        except Exception as e:
            print("FF daily error:", repr(e))


async def _sleep(seconds: float):
    import asyncio
    await asyncio.sleep(seconds)


if __name__ == "__main__":
    migrate_db()
    print("Starting bot... (daily schedule posts at/after 00:00 UTC; reminders at -15m and at time)", flush=True)
    Bot().run(DISCORD_TOKEN)