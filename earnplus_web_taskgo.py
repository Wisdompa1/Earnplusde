#!/usr/bin/env python3
"""
EarnPlus Web Platform Backend v4.0
====================================
FastAPI backend for the EarnPlus web platform.

Install:  pip install fastapi uvicorn requests tronpy bcrypt aiohttp
Run:      uvicorn earnplus_web:app --host 0.0.0.0 --port 8000

New in v4:
 - /leaderboard endpoint: returns daily top senders ranked by msgs_today
 - /admin/leaderboard: admin view with unmasked usernames + date filter
 - daily_msgs table: tracks per-user daily message counts for leaderboard
 - _increment_daily_msgs(): called on every successful message send
   (manual send, send-all, and auto REWARD worker events)
 - /withdrawal-orders: now returns order_id as "WD00000001" formatted string
   in addition to the raw numeric id
 - /dashboard: now returns msgs_today for the current user
 - DB migration: daily_msgs table created automatically on startup

Previous (v3):
 - earning_mode setting: 'manual' or 'auto'
 - In auto mode: /add-number POSTs to external worker HTTP API
 - /userbot-result webhook receives events from worker and fires notifications
 - Admin can switch modes via /admin/set-earning-mode
 - All user-facing messages use neutral language (no internal system details)
"""
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel
from typing import Optional
import sqlite3, hashlib, time, secrets, logging, os, threading, re, asyncio
import concurrent.futures
from contextlib import contextmanager
from datetime import datetime
import requests
from requests.exceptions import Timeout, ConnectionError as ReqConnError

# aiohttp not needed — userbot polls us, we don't call it

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False
    logging.warning("bcrypt not installed — falling back to SHA-256. pip install bcrypt")

try:
    from tronpy import Tron
    from tronpy.keys import PrivateKey
    TRONPY_AVAILABLE = True
except ImportError:
    TRONPY_AVAILABLE = False

# ═══════════════════ CONFIG ═══════════════════
# Manual mode: simpletasks88.com platform
SIMPLETASKS_BASE_URL = os.getenv("SIMPLETASKS_BASE_URL", "https://admin.simpletasks88.com")
PLATFORM_USER        = "Frankhustle"   # ← CHANGE TO YOUR simpletasks88 USERNAME
PLATFORM_PASS        = "f11111"        # ← CHANGE TO YOUR simpletasks88 PASSWORD
DB_FILE        = os.getenv("DB_FILE",
    "/data/earnplus_web.db" if os.path.isdir("/data") else "earnplus_web.db")
SECRET_KEY     = os.getenv("SECRET_KEY", "earnplus_web_secret_2026")
TOKEN_EXPIRY_H = 72
NGN_PER_POINT  = 0.15
MAX_RETRIES    = 6
POLL_INTERVAL  = 3
UA = ("Mozilla/5.0 (Linux; Android 13; V2116 Build/TP1A.220624.014_NONFC) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7632.120 Mobile Safari/537.36")

# Shared secret — must match SHARED_SECRET in task_userbot.py on Termux
SHARED_SECRET = os.getenv("SHARED_SECRET", "Frankpat1@")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("earnplus")

app = FastAPI(title="EarnPlus Web Platform", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

platform_session: dict = {}
platform_lock   = threading.Lock()
active_pairs: dict = {}
pairs_lock      = threading.Lock()

# ═══════════════════ DATABASE ═══════════════════
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL, password TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0, balance REAL DEFAULT 0,
            referral_code TEXT UNIQUE, referred_by INTEGER,
            is_banned INTEGER DEFAULT 0, created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS auth_tokens(
            token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS numbers(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            account TEXT NOT NULL, wsid INTEGER, status TEXT DEFAULT 'pairing',
            pair_code TEXT, msgs_sent INTEGER DEFAULT 0,
            added_at TEXT DEFAULT(datetime('now')), UNIQUE(user_id,account));
        CREATE TABLE IF NOT EXISTS auto_numbers(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            account TEXT NOT NULL, acct_type TEXT DEFAULT 'personal',
            send_limit TEXT DEFAULT 'nolimit', status TEXT DEFAULT 'pending',
            pair_code TEXT, msgs_sent INTEGER DEFAULT 0,
            added_at TEXT DEFAULT(datetime('now')), UNIQUE(user_id,account));
        CREATE TABLE IF NOT EXISTS pending_tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            account TEXT NOT NULL UNIQUE,
            acct_type TEXT DEFAULT 'personal',
            send_limit TEXT DEFAULT 'nolimit',
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT(datetime('now')));
        CREATE INDEX IF NOT EXISTS idx_ptasks_status ON pending_tasks(status);
        CREATE TABLE IF NOT EXISTS bank_details(
            user_id INTEGER PRIMARY KEY, account_num TEXT, account_name TEXT, bank_name TEXT);
        CREATE TABLE IF NOT EXISTS trx_wallets(
            user_id INTEGER PRIMARY KEY, wallet_address TEXT);
        CREATE TABLE IF NOT EXISTS withdrawals(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            amount REAL, method TEXT DEFAULT 'bank', status TEXT DEFAULT 'pending',
            reason TEXT, bank_name TEXT, account_num TEXT, account_name TEXT,
            wallet_addr TEXT, trx_amount REAL, tx_hash TEXT,
            created_at TEXT DEFAULT(datetime('now')), updated_at TEXT);
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            type TEXT, amount REAL, description TEXT,
            created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
        INSERT OR IGNORE INTO settings VALUES('naira_per_msg','30.0');
        INSERT OR IGNORE INTO settings VALUES('points_per_msg','200');
        INSERT OR IGNORE INTO settings VALUES('referral_pct','5.0');
        INSERT OR IGNORE INTO settings VALUES('min_withdrawal','15000');
        INSERT OR IGNORE INTO settings VALUES('max_withdrawal','500000');
        INSERT OR IGNORE INTO settings VALUES('ngn_usd_rate','1300.0');
        INSERT OR IGNORE INTO settings VALUES('trx_auto_payout','1');
        INSERT OR IGNORE INTO settings VALUES('allow_registration','1');
        INSERT OR IGNORE INTO settings VALUES('allow_withdrawals','1');
        INSERT OR IGNORE INTO settings VALUES('platform_url','');
        INSERT OR IGNORE INTO settings VALUES('trx_withdrawal_fee_usd','0.20');
        INSERT OR IGNORE INTO settings VALUES('min_trx_withdrawal','3.0');
        INSERT OR IGNORE INTO settings VALUES('earning_mode','manual');
        INSERT OR IGNORE INTO settings VALUES('wacash_account','');
        INSERT OR IGNORE INTO settings VALUES('wacash_password','');
        INSERT OR IGNORE INTO settings VALUES('wacash_fire_count','100');
        INSERT OR IGNORE INTO settings VALUES('wacash_threads','20');
        CREATE TABLE IF NOT EXISTS wacash_numbers(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            account TEXT NOT NULL, status TEXT DEFAULT 'pairing',
            pair_code TEXT, ws_id INTEGER, wacash_token TEXT,
            msgs_sent INTEGER DEFAULT 0,
            added_at TEXT DEFAULT(datetime('now')), UNIQUE(user_id,account));
        CREATE INDEX IF NOT EXISTS idx_wacash_uid ON wacash_numbers(user_id);
        CREATE TABLE IF NOT EXISTS claim_codes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL, points REAL NOT NULL,
            note TEXT, used_by INTEGER, used_at TEXT,
            created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS notifications(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            title TEXT NOT NULL, body TEXT NOT NULL,
            type TEXT DEFAULT 'info', is_read INTEGER DEFAULT 0,
            created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS admin_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL,
            action TEXT NOT NULL, target TEXT, detail TEXT,
            created_at TEXT DEFAULT(datetime('now')));
        CREATE TABLE IF NOT EXISTS daily_msgs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            msgs_count INTEGER DEFAULT 0,
            UNIQUE(user_id, date));
        CREATE INDEX IF NOT EXISTS idx_daily_msgs_date ON daily_msgs(date);
        CREATE INDEX IF NOT EXISTS idx_daily_msgs_uid ON daily_msgs(user_id);
        CREATE INDEX IF NOT EXISTS idx_tx_uid ON transactions(user_id);
        CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(user_id, type);
        CREATE INDEX IF NOT EXISTS idx_wd_uid ON withdrawals(user_id);
        CREATE INDEX IF NOT EXISTS idx_wd_status ON withdrawals(status);
        CREATE INDEX IF NOT EXISTS idx_num_uid ON numbers(user_id);
        CREATE INDEX IF NOT EXISTS idx_auto_uid ON auto_numbers(user_id);
        CREATE INDEX IF NOT EXISTS idx_auto_account ON auto_numbers(account);
        CREATE INDEX IF NOT EXISTS idx_notif_uid ON notifications(user_id, is_read);
        CREATE INDEX IF NOT EXISTS idx_tokens_uid ON auth_tokens(user_id);
        CREATE TABLE IF NOT EXISTS check_ins(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            points_awarded INTEGER DEFAULT 50,
            streak INTEGER DEFAULT 1,
            UNIQUE(user_id, date));
        CREATE INDEX IF NOT EXISTS idx_checkin_uid ON check_ins(user_id);
        CREATE TABLE IF NOT EXISTS login_attempts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            ip TEXT,
            attempted_at TEXT DEFAULT(datetime('now')));
        CREATE INDEX IF NOT EXISTS idx_attempts_user ON login_attempts(username, attempted_at);
        """)
        if not db.execute("SELECT id FROM users WHERE username='admin'").fetchone():
            ref = secrets.token_hex(4).upper()
            db.execute("INSERT INTO users(username,password,is_admin,referral_code) VALUES(?,?,1,?)",
                       ("admin", _hash_pw("admin123"), ref))
            log.info("Admin created: admin / admin123")

def _hash_pw(p):
    try:
        if BCRYPT_AVAILABLE:
            return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
    except Exception as e:
        log.warning(f"bcrypt hash failed: {e}")
    return hashlib.sha256(p.encode()).hexdigest()

def _verify_pw(p, h):
    try:
        if BCRYPT_AVAILABLE and h and (h.startswith("$2b$") or h.startswith("$2a$")):
            return bcrypt.checkpw(p.encode(), h.encode())
    except Exception as e:
        log.warning(f"bcrypt verify failed: {e}")
    return hashlib.sha256(p.encode()).hexdigest() == h

def get_setting(k, d=None):
    with get_db() as db:
        r = db.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
        return r["value"] if r else d

def set_setting(k, v):
    with get_db() as db:
        db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))

def get_earning_mode() -> str:
    return get_setting("earning_mode", "manual")

def _increment_daily_msgs(db, user_id: int, count: int = 1):
    """Increment today's message count for the leaderboard."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    db.execute(
        "INSERT INTO daily_msgs(user_id, date, msgs_count) VALUES(?,?,?) "
        "ON CONFLICT(user_id, date) DO UPDATE SET msgs_count=msgs_count+?",
        (user_id, today, count, count)
    )

# ═══════════════════ AUTH ═══════════════════
def create_token(user_id):
    token = secrets.token_hex(32)
    exp = datetime.utcfromtimestamp(
        datetime.utcnow().timestamp() + TOKEN_EXPIRY_H * 3600
    ).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as db:
        db.execute("DELETE FROM auth_tokens WHERE user_id=?", (user_id,))
        db.execute("DELETE FROM auth_tokens WHERE expires_at <= datetime('now')")
        db.execute("INSERT INTO auth_tokens VALUES(?,?,?)", (token, user_id, exp))
    return token

def get_current_user(token: str = Depends(oauth2_scheme)):
    with get_db() as db:
        row = db.execute(
            "SELECT t.user_id,u.username,u.is_admin,u.balance,u.is_banned,u.referral_code "
            "FROM auth_tokens t JOIN users u ON t.user_id=u.id "
            "WHERE t.token=? AND t.expires_at>datetime('now')", (token,)).fetchone()
    if not row: raise HTTPException(401, "Invalid or expired token")
    if row["is_banned"]: raise HTTPException(403, "Account suspended. Contact support.")
    return dict(row)

def admin_only(user=Depends(get_current_user)):
    if not user["is_admin"]: raise HTTPException(403, "Admin access required")
    return user

# ═══════════════════ PLATFORM API (simpletasks88.com) ═══════════════════
# All manual-mode calls route to admin.simpletasks88.com using the
# same signing scheme used by the Telegram bot (earnplusd).

def _md5(s): return hashlib.md5(s.encode()).hexdigest()

def _st_vhdrs():
    """simpletasks88 verify headers (same HMAC pattern as Telegram bot)."""
    vt = str(int(time.time() * 1000))
    return {"verify-time": vt, "verify-encrypt": _md5("yh123456" + vt)}

def _st_hdrs(x=None):
    """simpletasks88 request headers."""
    h = {
        "Content-Type": "application/json",
        "User-Agent": UA,
        "Referer": "https://simpletasks88.com/",
        "Origin": "https://simpletasks88.com",
        "accept": "application/json, text/plain, */*",
        "x-requested-with": "mark.via.gp",
    }
    h.update(_st_vhdrs())
    if x: h.update(x)
    return h

# Sign helpers — pattern taken from Telegram bot HAR log
# get_code:       md5(md5(path) + userid + username + account)
# get_phonestatus:md5(md5(path) + userid + username + account)
# addwsnumber:    md5(md5(path) + userid + username + account + str(types))
# sendmsg:        md5(md5(path) + userid + username + str(wsid))
# get_appinfo:    md5(md5(path) + userid + username)
def _st_sign(path, *parts): return _md5(_md5(path) + "".join(str(p) for p in parts))

def _retry(fn, label="API"):
    for i in range(1, MAX_RETRIES+1):
        try: return fn()
        except (Timeout, ReqConnError) as e:
            time.sleep(min(2**i, 20)); log.warning(f"[{label}] retry {i}: {e}")
        except Exception as e:
            time.sleep(min(2**i, 20)); log.warning(f"[{label}] error {i}: {e}")
    raise Exception(f"[{label}] Failed")

def platform_login():
    """Login to simpletasks88.com and cache the session."""
    http = requests.Session()
    userpwd = _md5(_md5(PLATFORM_PASS))
    sign    = _st_sign("/api/user/login", PLATFORM_USER, userpwd)
    try:
        r = _retry(lambda: http.post(
            f"{SIMPLETASKS_BASE_URL}/api/user/login",
            json={"username": PLATFORM_USER, "userpwd": userpwd, "sign": sign},
            headers=_st_hdrs(), timeout=15), "login")
        d = r.json()
        if d.get("code") == 0:
            info = d["data"]["info"]
            with platform_lock:
                platform_session.update({
                    "userid": info["id"],
                    "username": PLATFORM_USER,
                    "http": http,
                })
            log.info(f"[simpletasks] Login OK uid={info['id']}"); return True
        log.error(f"[simpletasks] Login failed: {d.get('message')}"); return False
    except Exception as e:
        log.error(f"[simpletasks] Login exception: {e}"); return False

def _ps():
    """Return the active platform session, re-logging in if expired."""
    with platform_lock:
        s = dict(platform_session)
    if not s.get("http") or not s.get("userid"):
        log.warning("[simpletasks] Session lost — re-logging in...")
        platform_login()
        with platform_lock:
            s = dict(platform_session)
    return s

def api_get_code(account):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    sign = _st_sign("/api/user/get_code", uid, uname, account)
    try:
        r = _retry(lambda: s["http"].get(
            f"{SIMPLETASKS_BASE_URL}/api/user/get_code",
            params={"account": account, "signType": "1", "username": uname, "userid": uid, "sign": sign},
            headers=_st_hdrs(), timeout=15), f"code:{account}")
        d = r.json()
        return (str(d["data"]), "ok") if d.get("code") == 0 and d.get("data") else (None, d.get("message", ""))
    except Exception: return None, "Service unavailable"

def api_phonestatus(account):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    sign = _st_sign("/api/user/get_phonestatus", uid, uname, account)
    try:
        r = _retry(lambda: s["http"].get(
            f"{SIMPLETASKS_BASE_URL}/api/user/get_phonestatus",
            params={"account": account, "signType": "0", "username": uname, "userid": uid, "sign": sign},
            headers=_st_hdrs(), timeout=10), f"status:{account}")
        d = r.json()
        return int(d["data"]) if d.get("code") == 0 else None
    except: return None

def api_addwsnumber(account, types=1):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    sign = _st_sign("/api/user/addwsnumber", uid, uname, account, str(types))
    try:
        r = _retry(lambda: s["http"].post(
            f"{SIMPLETASKS_BASE_URL}/api/user/addwsnumber",
            json={"account": account, "types": types, "username": uname, "userid": int(uid), "sign": sign},
            headers=_st_hdrs(), timeout=15), f"addws:{account}")
        d = r.json(); return d.get("code") == 0, d.get("message", "")
    except Exception: return False, "Service unavailable"

def api_sendmsg(phone, wsid):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    sign = _st_sign("/api/user/sendmsg", uid, uname, str(wsid))
    try:
        r = _retry(lambda: s["http"].post(
            f"{SIMPLETASKS_BASE_URL}/api/user/sendmsg",
            json={"phone": phone, "wsid": wsid, "username": uname, "userid": int(uid), "sign": sign},
            headers=_st_hdrs(), timeout=15), f"send:{phone}")
        d = r.json()
        ok = d.get("code") == 0
        return ok, ("" if ok else "Send failed — please try again")
    except Exception: return False, "Network timeout — please try again"

def api_get_wsid(account):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    page = 1
    while True:
        sign = _st_sign("/api/user/get_appinfo", uid, uname)
        try:
            r = _retry(lambda: s["http"].get(
                f"{SIMPLETASKS_BASE_URL}/api/user/get_appinfo",
                params={"page": page, "pagesize": 50, "username": uname, "userid": uid, "sign": sign},
                headers=_st_hdrs(), timeout=15), f"wsid:{account}")
            d = r.json()
            if d.get("code") != 0: break
            chunk, total = d["data"]["list"], d["data"]["count"]
            for item in chunk:
                if str(item.get("wsnumber", "")).strip() == account: return item["id"]
            if page * 50 >= total or not chunk: break
            page += 1
        except: break
    return None

def api_appinfo(page=1, pagesize=50):
    s = _ps(); uid, uname = str(s["userid"]), s["username"]
    sign = _st_sign("/api/user/get_appinfo", uid, uname)
    try:
        r = _retry(lambda: s["http"].get(
            f"{SIMPLETASKS_BASE_URL}/api/user/get_appinfo",
            params={"page": page, "pagesize": pagesize, "username": uname, "userid": uid, "sign": sign},
            headers=_st_hdrs(), timeout=15), "appinfo")
        d = r.json()
        return (d["data"]["list"], d["data"]["count"]) if d.get("code") == 0 else ([], 0)
    except: return [], 0

# ═══════════════════ WORKGO1 API (replaces WaCash / Mode 3) ═══════════════════
WORKGO_BASE     = "https://api.eiorjgoiej.com"
WORKGO_APP_TYPE = "2"
WORKGO_APP_VER  = "1.0.15"
WORKGO_UA       = ("Mozilla/5.0 (Linux; Android 13; V2116 Build/TP1A.220624.014_NONFC) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.7727.56 Mobile Safari/537.36")
_workgo_token   = None
_workgo_lock    = threading.Lock()

# Active workgo1 pairing workers: account -> {"user_id", "cancelled"}
_wacash_pairs: dict = {}
_wacash_pairs_lock  = threading.Lock()

# Global fire lock — ensures only one user fires sendMsg at a time
# so getTaskInfo delta is accurate (no overlap between users)
_wacash_fire_lock = threading.Lock()

def _whdrs(include_token: bool = True) -> dict:
    h = {
        "app-type":         WORKGO_APP_TYPE,
        "app-version":      WORKGO_APP_VER,
        "accept":           "application/json",
        "content-type":     "application/json",
        "accept-language":  "en_US",
        "origin":           "https://www.taskgo8.com",
        "referer":          "https://www.taskgo8.com/",
        "user-agent":       WORKGO_UA,
        "x-requested-with": "mark.via.gp",
        "sec-fetch-site":   "same-site",
        "sec-fetch-mode":   "cors",
        "sec-fetch-dest":   "empty",
        "sec-ch-ua":        '"Android WebView";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
    }
    if include_token:
        with _workgo_lock:
            tok = _workgo_token or ""
        h["app-token"] = tok
    return h

def wacash_api_get(path: str, params: dict = None, retries: int = 4):
    """Shared API caller — used by all workgo1 API calls."""
    global _workgo_token
    for attempt in range(retries):
        try:
            r = requests.get(f"{WORKGO_BASE}{path}", params=params,
                             headers=_whdrs(), timeout=10)
            data = r.json()
            # Token expired — re-login and retry
            if data and (data.get("code") in (401, 403) or
                         "过期" in str(data.get("msg", "")) or
                         "登录" in str(data.get("msg", "")) or
                         "login" in str(data.get("msg", "")).lower()):
                log.info("[TaskGo] Token expired — re-logging in...")
                if wacash_login(): continue
                else: return data
            return data
        except Exception:
            time.sleep(2)
    return None

def wacash_login() -> bool:
    """Login to workgo1 with admin-configured account and password."""
    global _workgo_token
    acct = get_setting("wacash_account", "")
    pwd  = get_setting("wacash_password", "")
    if not acct or not pwd:
        log.warning("[TaskGo] No account/password configured in settings")
        return False
    try:
        r = requests.get(f"{WORKGO_BASE}/app/login",
                         params={"account": acct, "password": pwd},
                         headers=_whdrs(include_token=False),
                         timeout=15)
        d = r.json()
        if d and d.get("code") == 200:
            with _workgo_lock:
                _workgo_token = d["data"]
            log.info(f"[TaskGo] Logged in as {acct} token={d['data'][:8]}...")
            return True
        log.error(f"[TaskGo] Login failed: {d}")
        return False
    except Exception as e:
        log.error(f"[TaskGo] Login error: {e}")
        return False

def wacash_get_pair_code(phone: str):
    """Get WhatsApp pairing code — TaskGo only needs phone number, no areaCode."""
    import re as _re
    clean = _re.sub(r"\D", "", phone)
    log.info(f"[TaskGo] getLoginCode phone={clean}")
    d = wacash_api_get("/app/wsNumber/getLoginCode", {"phone": clean})
    if not d or d.get("code") != 200:
        err = d.get("msg", "No response") if d else "No response"
        return None, err
    code = d.get("data", "")
    return code, None

def wacash_get_online() -> list:
    """Get list of online WhatsApp numbers from workgo1."""
    d = wacash_api_get("/app/wsNumber/online")
    if d and d.get("code") == 200:
        return d.get("data", [])
    return []

def wacash_send_msg(ws_id: int) -> tuple:
    """Fire sendMsg for a given ws_id. Returns (success, message)."""
    d = wacash_api_get("/app/wsNumber/sendMsg", {"id": ws_id}, retries=2)
    if d is None:
        log.warning(f"[TaskGo:sendMsg] ws_id={ws_id} no response")
        return False, "no response"
    code = d.get("code")
    msg  = d.get("msg", "unknown")
    log.info(f"[TaskGo:sendMsg] ws_id={ws_id} code={code} msg={msg}")
    if code == 200: return True, "ok"
    # Treat logout/offline as confirmed offline signal
    if any(x in msg.lower() for x in ["logout", "logged out", "offline", "not online"]):
        return False, "offline"
    return False, msg

def wacash_get_task_info() -> dict:
    """Get today's accurate task stats from workgo1 API."""
    d = wacash_api_get("/app/wsNumber/getTaskInfo")
    if d and d.get("code") == 200:
        data = d.get("data", {})
        return {
            "todaySendNum":    data.get("todaySendNum", 0),
            "todayPoints":     data.get("todayPoints", 0),
            "yesterdayPoints": data.get("yesterdayPoints", 0),
        }
    return {"todaySendNum": 0, "todayPoints": 0, "yesterdayPoints": 0}

def _wacash_pair_bg(user_id: int, account: str):
    """
    Background worker: check if already online → if not, get pairing code → wait for online.
    Does NOT fire automatically. User must click Send All.
    """
    log.info(f"[TaskGo:Pair] Start {account} uid={user_id}")
    with _wacash_pairs_lock:
        _wacash_pairs[account] = {"user_id": user_id, "cancelled": False}

    # Ensure WorkGo1 session is active
    if not _workgo_token:
        wacash_login()

    account_clean = account.replace("+", "").replace(" ", "").strip()

    # CRITICAL FIX: Check if number is ALREADY online on workgo1 BEFORE calling getLoginCode.
    # Calling getLoginCode on an already-connected number kicks it offline on workgo1,
    # which is why numbers go offline immediately after pairing starts.
    online = wacash_get_online()
    existing_ws_id = None
    for n in online:
        online_phone = str(n.get("wsAppNo", "")).replace("+", "").replace(" ", "").strip()
        if online_phone == account_clean:
            existing_ws_id = n["id"]
            break

    if existing_ws_id:
        # Already online — skip getLoginCode entirely, just store ws_id
        log.info(f"[TaskGo:Pair] {account} already online ws_id={existing_ws_id} — skipping getLoginCode")
        with get_db() as db:
            db.execute(
                "UPDATE wacash_numbers SET status='online',ws_id=?,pair_code=NULL WHERE user_id=? AND account=?",
                (existing_ws_id, user_id, account))
        with _wacash_pairs_lock:
            _wacash_pairs.pop(account, None)
        return

    # Not online yet — get pairing code
    pair_code = None
    for i in range(MAX_RETRIES):
        code, err = wacash_get_pair_code(account_clean)
        if code:
            pair_code = code
            break
        log.warning(f"[TaskGo:Pair] getLoginCode attempt {i+1} failed: {err}")
        time.sleep(min(i + 1, 5))

    if not pair_code:
        log.warning(f"[TaskGo:Pair] Could not get pair code for {account}")
        with get_db() as db:
            db.execute("UPDATE wacash_numbers SET status='error' WHERE user_id=? AND account=?",
                       (user_id, account))
        with _wacash_pairs_lock:
            _wacash_pairs.pop(account, None)
        return

    with get_db() as db:
        db.execute("UPDATE wacash_numbers SET pair_code=?,status='pairing' WHERE user_id=? AND account=?",
                   (pair_code, user_id, account))
    log.info(f"[TaskGo:Pair] Code for {account}: {pair_code}")

    # Poll until online (max 5 minutes)
    deadline = time.time() + 300
    ws_id = None

    while time.time() < deadline:
        with _wacash_pairs_lock:
            if _wacash_pairs.get(account, {}).get("cancelled"):
                with get_db() as db:
                    db.execute("DELETE FROM wacash_numbers WHERE user_id=? AND account=?", (user_id, account))
                _wacash_pairs.pop(account, None)
                return

        time.sleep(4)
        online = wacash_get_online()

        # Only accept the ws_id whose wsAppNo matches THIS number exactly
        for n in online:
            online_phone = str(n.get("wsAppNo", "")).replace("+", "").replace(" ", "").strip()
            if online_phone == account_clean:
                ws_id = n["id"]
                break

        if ws_id:
            break

    if not ws_id:
        log.info(f"[TaskGo:Pair] Timeout waiting for {account}")
        with get_db() as db:
            db.execute("UPDATE wacash_numbers SET status='timeout' WHERE user_id=? AND account=?",
                       (user_id, account))
        with _wacash_pairs_lock:
            _wacash_pairs.pop(account, None)
        return

    log.info(f"[TaskGo:Pair] {account} online ws_id={ws_id} — ready!")
    with get_db() as db:
        db.execute(
            "UPDATE wacash_numbers SET status='online',ws_id=?,pair_code=NULL WHERE user_id=? AND account=?",
            (ws_id, user_id, account))
    with _wacash_pairs_lock:
        _wacash_pairs.pop(account, None)
    log.info(f"[TaskGo:Pair] {account} ready ws_id={ws_id}")


# Instead of pushing tasks to the userbot, we store them in DB.
# The userbot on Termux polls /poll-tasks every 3 seconds and picks them up.

def _queue_task(user_id: int, account: str, acct_type: str, send_limit: str):
    """Insert a pending task into the queue for the userbot to pick up."""
    with get_db() as db:
        db.execute(
            "INSERT OR REPLACE INTO pending_tasks(user_id,account,acct_type,send_limit,status,created_at) "
            "VALUES(?,?,?,?,'pending',datetime('now'))",
            (user_id, account, acct_type, send_limit))
    log.info(f"[Queue] Task queued: {account} uid={user_id}")

def _cancel_queued_task(account: str):
    """Remove a pending/processing task from the queue (user deleted number)."""
    with get_db() as db:
        db.execute("DELETE FROM pending_tasks WHERE account=?", (account,))
    log.info(f"[Queue] Task cancelled: {account}")

# ═══════════════════ MANUAL PAIRING BG TASK ═══════════════════
def _pair_bg(user_id: int, account: str):
    log.info(f"[Pair] Start {account} uid={user_id}")
    with pairs_lock:
        active_pairs[account] = {"user_id": user_id, "pair_code": None,
                                  "status": "pairing", "wsid": None, "cancelled": False}
    pair_code = None
    for i in range(MAX_RETRIES):
        code, _ = api_get_code(account)
        if code: pair_code = code; break
        time.sleep(min(i + 1, 5))
    with pairs_lock:
        if account in active_pairs: active_pairs[account]["pair_code"] = pair_code
    with get_db() as db:
        db.execute("UPDATE numbers SET pair_code=? WHERE user_id=? AND account=?",
                   (pair_code, user_id, account))
    log.info(f"[Pair] Code {account}: {pair_code}")
    elapsed = 0; came_online = False
    while elapsed < 7200:
        with pairs_lock:
            if active_pairs.get(account, {}).get("cancelled"):
                with get_db() as db: db.execute("DELETE FROM numbers WHERE user_id=? AND account=?", (user_id, account))
                active_pairs.pop(account, None); return
        if api_phonestatus(account) == 1: came_online = True; break
        time.sleep(POLL_INTERVAL); elapsed += POLL_INTERVAL
    if not came_online:
        with get_db() as db: db.execute("DELETE FROM numbers WHERE user_id=? AND account=?", (user_id, account))
        active_pairs.pop(account, None); log.info(f"[Pair] Timeout {account}"); return
    ok, msg = api_addwsnumber(account)
    wsid = None
    if ok:
        time.sleep(3)
        for _ in range(3):
            wsid = api_get_wsid(account)
            if wsid: break
            time.sleep(3)
    if ok and wsid:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        with get_db() as db:
            db.execute("UPDATE numbers SET status='online',wsid=?,pair_code=NULL WHERE user_id=? AND account=?",
                       (wsid, user_id, account))
            # FIXED: reset today's daily_msgs so card shows 0 earned on fresh connect
            # without this, stale msgs from previous sessions show as fake earnings
            db.execute("DELETE FROM daily_msgs WHERE user_id=? AND date=?", (user_id, today))
        with pairs_lock:
            if account in active_pairs: active_pairs[account].update({"status": "online", "wsid": wsid})
        log.info(f"[Pair] Online {account} wsid={wsid}")
    else:
        with get_db() as db:
            db.execute("UPDATE numbers SET status='error' WHERE user_id=? AND account=?", (user_id, account))
        with pairs_lock:
            if account in active_pairs: active_pairs[account]["status"] = "error"

# ═══════════════════ HELPERS ═══════════════════
def _ngn_to_pts(ngn_amount: float) -> int:
    """Convert stored NGN to display points."""
    npm = float(get_setting("naira_per_msg", "30"))
    ppm = int(get_setting("points_per_msg", "200"))
    if npm <= 0 or ppm <= 0:
        return int(ngn_amount)
    return int((ngn_amount / npm) * ppm)

def _pts_to_ngn(pts: int) -> float:
    """Convert user-entered points to NGN for storage/withdrawal."""
    npm = float(get_setting("naira_per_msg", "30"))
    ppm = int(get_setting("points_per_msg", "200"))
    if ppm <= 0:
        return 0.0
    return (pts / ppm) * npm

def _pts_per_msg():
    """Points earned per message."""
    return int(get_setting("points_per_msg", "200"))

def _credit(db, uid, amt, desc, t="earn"):
    db.execute("UPDATE users SET balance=balance+? WHERE id=?", (amt, uid))
    db.execute("INSERT INTO transactions(user_id,type,amount,description) VALUES(?,?,?,?)", (uid, t, amt, desc))

def _debit(db, uid, amt, desc):
    db.execute("UPDATE users SET balance=balance-? WHERE id=?", (amt, uid))
    db.execute("INSERT INTO transactions(user_id,type,amount,description) VALUES(?,?,?,?)", (uid, "debit", amt, desc))

def _notify(db, user_id, title, body, ntype="info"):
    db.execute("INSERT INTO notifications(user_id,title,body,type) VALUES(?,?,?,?)",
               (user_id, title, body, ntype))

def _admin_log(db, admin_id, action, target=None, detail=None):
    db.execute("INSERT INTO admin_logs(admin_id,action,target,detail) VALUES(?,?,?,?)",
               (admin_id, action, str(target) if target else None, detail))

# ═══════════════════ MODELS ═══════════════════
class LoginReq(BaseModel): username: str; password: str
class RegisterReq(BaseModel): username: str; password: str; invite_code: Optional[str] = None
class ClaimCodeReq(BaseModel): code: str
class GenerateCodeReq(BaseModel): points: float; note: Optional[str] = None; count: Optional[int] = 1
class AddNumReq(BaseModel): account: str; acct_type: Optional[str] = "personal"; send_limit: Optional[str] = "nolimit"
class SendOneReq(BaseModel): account: str; wsid: Optional[str] = None
class WithdrawReq(BaseModel): method: str; password: str; amount: Optional[float] = None; ngn_amount: Optional[float] = None
class BankReq(BaseModel): account_num: str; account_name: str; bank_name: str
class TrxWalletReq(BaseModel): wallet_address: str
class ChangePwReq(BaseModel): current_password: str; new_password: str
class SettingReq(BaseModel): key: str; value: str
class ToggleReq(BaseModel): key: str; value: bool
class BroadcastReq(BaseModel): message: str; audience: Optional[str] = "all"
class WdActionReq(BaseModel): withdrawal_id: int; reason: Optional[str] = None
class CreditReq(BaseModel): user_id: int; amount: float
class BanReq(BaseModel): user_id: int; is_banned: bool
class AccountReq(BaseModel): account: str
class SetModeReq(BaseModel): mode: str   # 'manual', 'auto', or 'wacash'

# ═══════════════════ SPA PAGE ROUTES ═══════════════════
# Serve index.html for each named page so direct URLs and refresh work.
# These must be defined BEFORE the API routes with the same path names.
def _serve_html():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return HTMLResponse("<h2>Place index.html in the same folder as earnplus_web.py</h2>")

@app.get("/home", response_class=HTMLResponse, include_in_schema=False)
def spa_home(): return _serve_html()

@app.get("/hangup", response_class=HTMLResponse, include_in_schema=False)
def spa_hangup(): return _serve_html()

@app.get("/service", response_class=HTMLResponse, include_in_schema=False)
def spa_service(): return _serve_html()

@app.get("/promotion", response_class=HTMLResponse, include_in_schema=False)
def spa_promotion(): return _serve_html()

@app.get("/account", response_class=HTMLResponse, include_in_schema=False)
def spa_account(): return _serve_html()

@app.get("/leaderboard-page", response_class=HTMLResponse, include_in_schema=False)
def spa_leaderboard(): return _serve_html()

# ═══════════════════ AUTH ROUTES ═══════════════════
@app.post("/register")
def register(req: RegisterReq):
    try:
        if get_setting("allow_registration", "1") != "1": raise HTTPException(403, "Registrations closed")
        if len(req.username) < 3 or len(req.username) > 20: raise HTTPException(400, "Username must be 3-20 chars")
        if len(req.password) < 6: raise HTTPException(400, "Password must be at least 6 chars")
        if not req.username.replace("_", "").replace("-", "").isalnum():
            raise HTTPException(400, "Username can only contain letters, numbers, - and _")
        with get_db() as db:
            if db.execute("SELECT id FROM users WHERE username=?", (req.username,)).fetchone():
                raise HTTPException(400, "Username already taken")
            ref = secrets.token_hex(5).upper(); ref_by = None
            if req.invite_code:
                row = db.execute("SELECT id FROM users WHERE referral_code=?",
                                  (req.invite_code.strip().upper(),)).fetchone()
                if row: ref_by = row["id"]
            hashed = _hash_pw(req.password)
            db.execute("INSERT INTO users(username,password,referral_code,referred_by) VALUES(?,?,?,?)",
                       (req.username, hashed, ref, ref_by))
            uid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        token = create_token(uid)
        return {"token": token, "user": {"id": uid, "username": req.username, "is_admin": False, "referral_code": ref}}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Registration error: {e}")
        raise HTTPException(500, "Registration failed. Please try again.")

@app.post("/signup")
def signup(req: RegisterReq):
    return register(req)

@app.post("/login")
def login(req: LoginReq, request: Request):
    try:
        ip = request.client.host if request.client else "unknown"
        username = req.username.strip()
        # ── Brute-force guard: max 5 attempts per username in 10 min ──
        with get_db() as db:
            recent = db.execute(
                "SELECT COUNT(*) as c FROM login_attempts WHERE username=? AND attempted_at > datetime('now','-10 minutes')",
                (username,)).fetchone()["c"]
        if recent >= 5:
            raise HTTPException(429, "Too many login attempts. Please wait 10 minutes.")
        with get_db() as db:
            u = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if not u or not _verify_pw(req.password, u["password"]):
            with get_db() as db:
                db.execute("INSERT INTO login_attempts(username,ip) VALUES(?,?)", (username, ip))
            raise HTTPException(401, "Invalid username or password")
        if u["is_banned"]: raise HTTPException(403, "Account suspended. Contact support.")
        token = create_token(u["id"])
        # Clear failed attempts on successful login
        with get_db() as db:
            db.execute("DELETE FROM login_attempts WHERE username=?", (username,))
        return {"token": token, "user": {"id": u["id"], "username": u["username"],
                "is_admin": bool(u["is_admin"]), "referral_code": u["referral_code"]}}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Login error: {e}")
        raise HTTPException(500, "Login failed. Please try again.")

# ═══════════════════ DASHBOARD ═══════════════════
@app.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_db() as db:
        u  = db.execute("SELECT balance,referral_code FROM users WHERE id=?", (uid,)).fetchone()
        te = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn' AND date(created_at)=date('now')", (uid,)).fetchone()["s"]
        tr = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='referral' AND date(created_at)=date('now')", (uid,)).fetchone()["s"]
        ta = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn'", (uid,)).fetchone()["s"]
        # BUG FIX: use transactions table for msgs_today so it survives number deletion/offline
        msgs_today = db.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE user_id=? AND type='earn' AND date(created_at)=date('now')",
            (uid,)).fetchone()["c"]
        # BUG FIX: use transactions for total_msgs_sent — never resets to 0 when numbers deleted
        total_msgs_sent = db.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE user_id=? AND type='earn'",
            (uid,)).fetchone()["c"]
        ci = db.execute("SELECT streak FROM check_ins WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
        checkin_streak = ci["streak"] if ci else 0
        if mode == "auto":
            on = db.execute("SELECT COUNT(*) as c FROM auto_numbers WHERE user_id=? AND status='online'", (uid,)).fetchone()["c"]
        elif mode == "wacash":
            on = db.execute("SELECT COUNT(*) as c FROM wacash_numbers WHERE user_id=? AND status='online'", (uid,)).fetchone()["c"]
        else:
            on = db.execute("SELECT COUNT(*) as c FROM numbers WHERE user_id=? AND status='online'", (uid,)).fetchone()["c"]
    rc = u["referral_code"] or ""; pu = get_setting("platform_url", "") or ""
    return {
        "balance":               _ngn_to_pts(u["balance"] or 0),
        "today_points":          _ngn_to_pts(te or 0),
        "today_referral_points": _ngn_to_pts(tr or 0),
        "total_earned":          _ngn_to_pts(ta or 0),
        "online_numbers":        on,
        "msgs_today":            msgs_today,
        "total_msgs_sent":       total_msgs_sent,
        "checkin_streak":        checkin_streak,
        "referral_code":         rc,
        "referral_link":         f"{pu}?ref={rc}" if pu else f"/?ref={rc}",
        "earning_mode":          mode,
    }



# ═══════════════════ CHECK-IN ═══════════════════
@app.post("/check-in")
def check_in(user=Depends(get_current_user)):
    uid = user["user_id"]
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")
    with get_db() as db:
        # Already checked in today?
        existing = db.execute("SELECT id FROM check_ins WHERE user_id=? AND date=?", (uid, today)).fetchone()
        if existing:
            raise HTTPException(400, "Already checked in today!")
        # Calculate streak
        prev = db.execute("SELECT streak FROM check_ins WHERE user_id=? AND date=? ORDER BY id DESC LIMIT 1",
                          (uid, yesterday)).fetchone()
        streak = (prev["streak"] + 1) if prev else 1
        # Bonus points scale with streak (50 base, +10 per streak day, max 150)
        pts = min(50 + (streak - 1) * 10, 150)
        npm = float(get_setting("naira_per_msg", "30"))
        ppm = int(get_setting("points_per_msg", "200"))
        ngn_amt = (pts / ppm * npm) if ppm > 0 else 0
        db.execute("INSERT INTO check_ins(user_id,date,points_awarded,streak) VALUES(?,?,?,?)",
                   (uid, today, pts, streak))
        _credit(db, uid, int(ngn_amt), f"Daily check-in bonus (Day {streak})", "checkin")
        _notify(db, uid, "Daily Check-in! 🎉",
                f"You claimed {pts} pts for Day {streak} streak! Keep it up!", "success")
    return {"points": pts, "streak": streak, "message": f"Checked in! +{pts} pts"}

@app.get("/check-in-status")
def check_in_status(user=Depends(get_current_user)):
    uid = user["user_id"]
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - __import__('datetime').timedelta(days=1)).strftime("%Y-%m-%d")
    with get_db() as db:
        done_today = db.execute("SELECT streak,points_awarded FROM check_ins WHERE user_id=? AND date=?",
                                (uid, today)).fetchone()
        prev = db.execute("SELECT streak FROM check_ins WHERE user_id=? AND date=?",
                          (uid, yesterday)).fetchone()
        total_checkins = db.execute("SELECT COUNT(*) as c FROM check_ins WHERE user_id=?", (uid,)).fetchone()["c"]
    current_streak = done_today["streak"] if done_today else (prev["streak"] if prev else 0)
    next_pts = min(50 + current_streak * 10, 150) if not done_today else 0
    return {
        "checked_in_today": bool(done_today),
        "streak": current_streak,
        "points_today": done_today["points_awarded"] if done_today else 0,
        "next_points": next_pts,
        "total_checkins": total_checkins
    }

# ═══════════════════ NUMBERS ═══════════════════
@app.get("/my-numbers")
def my_numbers(user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    if mode == "wacash":
        with get_db() as db:
            rows = db.execute(
                """SELECT w.account, w.status, w.pair_code, w.msgs_sent, w.ws_id as wsid, w.added_at,
                          (SELECT COUNT(*) FROM transactions t
                           WHERE t.user_id=w.user_id AND t.type='earn'
                           AND date(t.created_at)=? AND t.description LIKE '%' || w.account || '%'
                          ) as msgs_today
                   FROM wacash_numbers w WHERE w.user_id=? ORDER BY w.added_at DESC""",
                (today, uid)).fetchall()
        return {"numbers": [dict(r) for r in rows], "earning_mode": mode}

    if mode == "auto":
        with get_db() as db:
            rows = db.execute(
                """SELECT a.account, a.status, a.pair_code, a.msgs_sent, NULL as wsid, a.added_at,
                          (SELECT COUNT(*) FROM transactions t
                           WHERE t.user_id=a.user_id AND t.type='earn'
                           AND date(t.created_at)=? AND t.description LIKE '%' || a.account || '%'
                          ) as msgs_today
                   FROM auto_numbers a WHERE a.user_id=? ORDER BY a.added_at DESC""",
                (today, uid)).fetchall()
        return {"numbers": [dict(r) for r in rows], "earning_mode": mode}

    # manual mode
    with pairs_lock:
        for acct, info in list(active_pairs.items()):
            if info.get("user_id") == uid:
                with get_db() as db:
                    if info.get("pair_code"):
                        db.execute("UPDATE numbers SET pair_code=?,status='pairing' WHERE user_id=? AND account=?",
                                   (info["pair_code"], uid, acct))
                    if info.get("status") == "online":
                        db.execute("UPDATE numbers SET status='online' WHERE user_id=? AND account=?", (uid, acct))
    with get_db() as db:
        rows = db.execute(
            """SELECT n.account,n.status,n.pair_code,n.msgs_sent,n.wsid,n.added_at,
                      (SELECT COUNT(*) FROM transactions t
                       WHERE t.user_id=n.user_id AND t.type='earn'
                       AND date(t.created_at)=? AND t.description LIKE '%' || n.account || '%'
                      ) as msgs_today
               FROM numbers n WHERE n.user_id=? ORDER BY n.added_at DESC""",
            (today, uid)).fetchall()
    return {"numbers": [dict(r) for r in rows], "earning_mode": mode}

@app.post("/add-number")
async def add_number(req: AddNumReq, bg: BackgroundTasks, user=Depends(get_current_user)):
    uid = user["user_id"]
    account = re.sub(r"[^\d]", "", req.account)
    if len(account) < 7 or len(account) > 20: raise HTTPException(400, "Invalid number")
    mode = get_earning_mode()

    if mode == "wacash":
        if not _workgo_token:
            ok = wacash_login()
            if not ok: raise HTTPException(503, "Service unavailable — please try again shortly")
        with get_db() as db:
            ex = db.execute("SELECT status FROM wacash_numbers WHERE user_id=? AND account=?",
                             (uid, account)).fetchone()
            if ex:
                if ex["status"] == "online": raise HTTPException(400, "Number already connected")
                db.execute("UPDATE wacash_numbers SET status='pairing',pair_code=NULL,ws_id=NULL,added_at=datetime('now') WHERE user_id=? AND account=?",
                           (uid, account))
            else:
                db.execute("INSERT INTO wacash_numbers(user_id,account,status) VALUES(?,?,'pairing')",
                           (uid, account))
        bg.add_task(_wacash_pair_bg, uid, account)
        return {"status": "started", "account": account, "mode": "wacash"}

    if mode == "auto":
        with get_db() as db:
            ex = db.execute("SELECT status FROM auto_numbers WHERE user_id=? AND account=?",
                             (uid, account)).fetchone()
            if ex:
                if ex["status"] == "online": raise HTTPException(400, "Number already connected")
                db.execute("UPDATE auto_numbers SET status='pending',added_at=datetime('now') WHERE user_id=? AND account=?",
                           (uid, account))
            else:
                db.execute("INSERT INTO auto_numbers(user_id,account,acct_type,send_limit,status) VALUES(?,?,?,?,'pending')",
                           (uid, account, req.acct_type or "personal", req.send_limit or "nolimit"))
        _queue_task(uid, account, req.acct_type or "personal", req.send_limit or "nolimit")
        return {"status": "started", "account": account, "mode": "auto"}

    # manual
    with get_db() as db:
        ex = db.execute("SELECT status FROM numbers WHERE user_id=? AND account=?", (uid, account)).fetchone()
        if ex:
            if ex["status"] == "online": raise HTTPException(400, "Number already connected")
            db.execute("UPDATE numbers SET status='pairing',pair_code=NULL,wsid=NULL WHERE user_id=? AND account=?",
                       (uid, account))
        else:
            db.execute("INSERT INTO numbers(user_id,account,status) VALUES(?,?,'pairing')", (uid, account))
    bg.add_task(_pair_bg, uid, account)
    return {"status": "started", "account": account, "mode": "manual"}

@app.get("/pairing-status")
def pairing_status(account: str, user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    if mode == "wacash":
        # Check live pairing worker first
        with _wacash_pairs_lock:
            info = _wacash_pairs.get(account)
        if info and info.get("user_id") == uid:
            with get_db() as db:
                row = db.execute("SELECT status,pair_code,ws_id FROM wacash_numbers WHERE user_id=? AND account=?",
                                  (uid, account)).fetchone()
            if row:
                return {"status": row["status"], "pair_code": row["pair_code"], "wsid": row["ws_id"], "mode": "wacash"}
        with get_db() as db:
            row = db.execute("SELECT status,pair_code,ws_id FROM wacash_numbers WHERE user_id=? AND account=?",
                              (uid, account)).fetchone()
        if not row: raise HTTPException(404, "Number not found")
        return {"status": row["status"], "pair_code": row["pair_code"], "wsid": row["ws_id"], "mode": "wacash"}
    if mode == "auto":
        with get_db() as db:
            row = db.execute("SELECT status,pair_code FROM auto_numbers WHERE user_id=? AND account=?",
                              (uid, account)).fetchone()
        if not row: raise HTTPException(404, "Number not found")
        return {"status": row["status"], "pair_code": row["pair_code"], "wsid": None, "mode": "auto"}
    with pairs_lock:
        info = active_pairs.get(account)
        if info and info.get("user_id") == uid:
            return {"status": info.get("status", "pairing"), "pair_code": info.get("pair_code"), "wsid": info.get("wsid")}
    with get_db() as db:
        row = db.execute("SELECT status,pair_code,wsid FROM numbers WHERE user_id=? AND account=?",
                          (uid, account)).fetchone()
    if not row: raise HTTPException(404, "Number not found")
    return dict(row)

@app.post("/cancel-pairing")
async def cancel_pairing(req: AccountReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    if mode == "wacash":
        with _wacash_pairs_lock:
            if req.account in _wacash_pairs: _wacash_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("DELETE FROM wacash_numbers WHERE user_id=? AND account=?", (uid, req.account))
    elif mode == "auto":
        with get_db() as db:
            db.execute("DELETE FROM auto_numbers WHERE user_id=? AND account=?", (uid, req.account))
        _cancel_queued_task(req.account)
    else:
        with pairs_lock:
            if req.account in active_pairs: active_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("DELETE FROM numbers WHERE user_id=? AND account=?", (uid, req.account))
    return {"status": "cancelled"}

@app.post("/delete-number")
async def delete_number(req: AccountReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    if mode == "wacash":
        with _wacash_pairs_lock:
            if req.account in _wacash_pairs: _wacash_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("DELETE FROM wacash_numbers WHERE user_id=? AND account=?", (uid, req.account))
    elif mode == "auto":
        with get_db() as db:
            db.execute("DELETE FROM auto_numbers WHERE user_id=? AND account=?", (uid, req.account))
        _cancel_queued_task(req.account)
    else:
        with pairs_lock:
            if req.account in active_pairs: active_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("DELETE FROM numbers WHERE user_id=? AND account=?", (uid, req.account))
    return {"status": "deleted"}

@app.post("/delete-all-numbers")
async def delete_all_numbers(user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    deleted = 0
    if mode == "wacash":
        with get_db() as db:
            rows = db.execute("SELECT account FROM wacash_numbers WHERE user_id=?", (uid,)).fetchall()
            accs = [r["account"] for r in rows]
            db.execute("DELETE FROM wacash_numbers WHERE user_id=?", (uid,))
            deleted = len(accs)
        with _wacash_pairs_lock:
            for a in accs:
                if a in _wacash_pairs: _wacash_pairs[a]["cancelled"] = True
    elif mode == "auto":
        with get_db() as db:
            rows = db.execute("SELECT account FROM auto_numbers WHERE user_id=?", (uid,)).fetchall()
            accounts = [r["account"] for r in rows]
            db.execute("DELETE FROM auto_numbers WHERE user_id=?", (uid,))
            deleted = len(accounts)
        for account in accounts:
            _cancel_queued_task(account)
    else:
        with get_db() as db:
            rows = db.execute("SELECT account FROM numbers WHERE user_id=?", (uid,)).fetchall()
            accounts = [r["account"] for r in rows]
        with pairs_lock:
            for account in accounts:
                if account in active_pairs: active_pairs[account]["cancelled"] = True
        with get_db() as db:
            db.execute("DELETE FROM numbers WHERE user_id=?", (uid,))
            deleted = len(accounts)
    log.info(f"[delete-all] uid={uid} deleted={deleted} mode={mode}")
    return {"status": "deleted", "count": deleted}

@app.post("/reauthorize")
async def reauthorize(req: AccountReq, bg: BackgroundTasks, user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()
    if mode == "wacash":
        with _wacash_pairs_lock:
            if req.account in _wacash_pairs: _wacash_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("UPDATE wacash_numbers SET status='pairing',pair_code=NULL,ws_id=NULL WHERE user_id=? AND account=?",
                       (uid, req.account))
        bg.add_task(_wacash_pair_bg, uid, req.account)
        return {"status": "reauthorizing", "mode": "wacash"}
    if mode == "auto":
        with get_db() as db:
            row = db.execute("SELECT acct_type,send_limit FROM auto_numbers WHERE user_id=? AND account=?",
                              (uid, req.account)).fetchone()
            db.execute("UPDATE auto_numbers SET status='pending' WHERE user_id=? AND account=?",
                       (uid, req.account))
        acct_type  = row["acct_type"]  if row else "personal"
        send_limit = row["send_limit"] if row else "nolimit"
        _cancel_queued_task(req.account)
        _queue_task(uid, req.account, acct_type, send_limit)
        return {"status": "reauthorizing", "mode": "auto"}
    else:
        with pairs_lock:
            if req.account in active_pairs: active_pairs[req.account]["cancelled"] = True
        with get_db() as db:
            db.execute("UPDATE numbers SET status='pairing',pair_code=NULL,wsid=NULL WHERE user_id=? AND account=?",
                       (uid, req.account))
        bg.add_task(_pair_bg, uid, req.account)
        return {"status": "reauthorizing", "mode": "manual"}

# ═══════════════════ SEND ═══════════════════
@app.post("/send-message")
def send_message(req: SendOneReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    if get_earning_mode() == "auto":
        raise HTTPException(400, "Your numbers are earning automatically. No manual action needed.")
    with get_db() as db:
        num = db.execute("SELECT wsid,status FROM numbers WHERE user_id=? AND account=?",
                          (uid, req.account)).fetchone()
    if not num or num["status"] != "online": raise HTTPException(400, "Number is not online")
    wsid = num["wsid"] or (int(req.wsid) if req.wsid else None)
    if not wsid: raise HTTPException(400, "Number not registered properly")
    ok, msg = api_sendmsg(req.account, wsid)
    if ok:
        ppm = int(get_setting("points_per_msg", "200"))
        ngn_earned = float(get_setting("naira_per_msg", "30"))
        with get_db() as db:
            _credit(db, uid, ngn_earned, f"Message sent via {req.account}")
            db.execute("UPDATE numbers SET msgs_sent=msgs_sent+1 WHERE user_id=? AND account=?", (uid, req.account))
            _increment_daily_msgs(db, uid, 1)
            u = db.execute("SELECT referred_by FROM users WHERE id=?", (uid,)).fetchone()
            if u and u["referred_by"]:
                ref_pct = float(get_setting("referral_pct", "5"))
                ngn_bonus = _pts_to_ngn(ppm) * (ref_pct / 100)
                _credit(db, u["referred_by"], ngn_bonus, f"Ref bonus from {uid}", "referral")
        return {"ok": True, "earned": ppm, "points": ppm}
    # FIXED: only act when phonestatus is EXACTLY 0 (confirmed offline)
    # If None (timeout/network error) — keep number alive, just tell user to retry
    phone_status = api_phonestatus(req.account)
    if phone_status == 0:
        with get_db() as db:
            db.execute("UPDATE numbers SET status='offline' WHERE user_id=? AND account=?", (uid, req.account))
        raise HTTPException(400, "Number went offline. It has been marked offline — try reauthorizing.")
    raise HTTPException(400, "Send failed — network issue. Please try again.")

@app.post("/send-all")
def send_all(user=Depends(get_current_user)):
    uid = user["user_id"]
    mode = get_earning_mode()

    # ── AUTO MODE ──────────────────────────────────────────────────────────────
    if mode == "auto":
        with get_db() as db:
            online = db.execute("SELECT COUNT(*) as c FROM auto_numbers WHERE user_id=? AND status='online'",
                                 (uid,)).fetchone()["c"]
            total  = db.execute("SELECT COUNT(*) as c FROM auto_numbers WHERE user_id=?",
                                 (uid,)).fetchone()["c"]
        return {"mode": "auto", "online": online, "total": total,
                "message": f"Your {online} connected number(s) are earning automatically."}

    # ── WACASH / WORKGO1 MODE ──────────────────────────────────────────────────
    if mode == "wacash":
        with get_db() as db:
            rows = db.execute(
                "SELECT account, ws_id FROM wacash_numbers WHERE user_id=? AND status='online' AND ws_id IS NOT NULL",
                (uid,)).fetchall()
        if not rows: raise HTTPException(400, "No online numbers")

        # FIX: fetch BOTH rate settings — ppm was missing before causing NameError and no reward
        threads_per = int(get_setting("wacash_threads", "20"))
        npm         = float(get_setting("naira_per_msg", "30"))
        ppm         = int(get_setting("points_per_msg", "200"))

        per_num_results = {}
        per_num_lock    = threading.Lock()

        def fire_until_offline(acct, ws_id):
            """
            Fire sendMsg threads_per concurrent requests per batch.
            Stops when 2 consecutive full batches return all failures.
            Uses proper closure capture to avoid Python loop-closure bugs.
            """
            total_success = 0
            total_failed  = 0
            consecutive_fail_batches = 0

            while True:
                results = []
                results_lock = threading.Lock()

                def _fire(wid=ws_id):
                    ok, msg = wacash_send_msg(wid)
                    with results_lock:
                        results.append((ok, msg))

                with concurrent.futures.ThreadPoolExecutor(max_workers=threads_per) as ex:
                    futs = [ex.submit(_fire) for _ in range(threads_per)]
                    concurrent.futures.wait(futs)

                batch_success = sum(1 for ok, _ in results if ok)
                batch_failed  = sum(1 for ok, _ in results if not ok)
                total_success += batch_success
                total_failed  += batch_failed

                log.info(f"[TaskGo:Fire] ws_id={ws_id} batch ok={batch_success} fail={batch_failed}")

                if batch_success == 0:
                    consecutive_fail_batches += 1
                    # Check if majority of failures are "logged out" (not just rate limited)
                    logout_count = sum(1 for _, msg in results if "logout" in msg.lower() or "logged out" in msg.lower())
                    log.info(f"[TaskGo:Fire] ws_id={ws_id} fail_batches={consecutive_fail_batches} logouts={logout_count}/20")

                    if consecutive_fail_batches >= 3:
                        # Confirmed truly offline after 3 consecutive fail batches
                        with get_db() as db2:
                            db2.execute(
                                "UPDATE wacash_numbers SET status='offline' WHERE user_id=? AND account=?",
                                (uid, acct))
                        log.info(f"[TaskGo:Fire] ws_id={ws_id} marking offline after {consecutive_fail_batches} fail batches")
                        break

                    # Wait between fail batches — give workgo1 time to recover
                    time.sleep(3)
                else:
                    consecutive_fail_batches = 0

            with per_num_lock:
                per_num_results[acct] = {
                    "ok":      total_success > 0,
                    "success": total_success,
                    "failed":  total_failed,
                }

        # Acquire global fire lock so no other user overlaps — makes getTaskInfo delta accurate
        with _wacash_fire_lock:
            # Snapshot BEFORE — only this user's numbers will fire while lock is held
            task_before  = wacash_get_task_info()
            sends_before = task_before.get("todaySendNum", 0) if task_before else 0

            # All this user's numbers fire simultaneously
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(rows)) as ex:
                concurrent.futures.wait([
                    ex.submit(fire_until_offline, r["account"], r["ws_id"])
                    for r in rows])

            # Snapshot AFTER — delta is ONLY this user's sends (no other user fired)
            task_after  = wacash_get_task_info()
            sends_after = task_after.get("todaySendNum", 0) if task_after else 0

        # Use getTaskInfo delta as the authoritative send count (platform truth)
        # Fall back to local per_num_results count if delta looks wrong
        delta_sends  = max(0, sends_after - sends_before)
        local_sends  = sum(d["success"] for d in per_num_results.values())
        actual_sends = delta_sends if delta_sends > 0 else local_sends

        log.info(f"[TaskGo:SendAll] uid={uid} delta={delta_sends} local={local_sends} actual={actual_sends}")

        # Credit user based on actual sends
        total_earned_pts = 0
        if actual_sends > 0:
            total_earned_pts = actual_sends * ppm
            ngn_earned = _pts_to_ngn(total_earned_pts)
            with get_db() as db:
                _credit(db, uid, ngn_earned, f"TaskGo: sent {actual_sends} messages")
                _increment_daily_msgs(db, uid, actual_sends)
                for r in rows:
                    acct = r["account"]
                    num_ok = per_num_results.get(acct, {}).get("success", 0)
                    if num_ok > 0:
                        db.execute(
                            "UPDATE wacash_numbers SET msgs_sent=msgs_sent+? WHERE user_id=? AND account=?",
                            (num_ok, uid, acct))
                u = db.execute("SELECT referred_by FROM users WHERE id=?", (uid,)).fetchone()
                if u and u["referred_by"]:
                    ref_pct = float(get_setting("referral_pct", "5"))
                    ngn_bonus = _pts_to_ngn(total_earned_pts) * (ref_pct / 100)
                    _credit(db, u["referred_by"], ngn_bonus, f"Ref bonus from uid={uid}", "referral")
            log.info(f"[TaskGo:SendAll] uid={uid} credited {ngn_earned} NGN ({total_earned_pts} pts) for {actual_sends} sends")
        else:
            log.warning(f"[TaskGo:SendAll] uid={uid} 0 sends — no reward")

        with get_db() as db:
            new_bal = db.execute("SELECT balance FROM users WHERE id=?", (uid,)).fetchone()["balance"]

        results = [{"account": acct, "ok": d["ok"], "success": d["success"], "failed": d["failed"]}
                   for acct, d in per_num_results.items()]

        return {
            "results":      results,
            "sent":         actual_sends,
            "failed":       sum(r["failed"] for r in results),
            "total_earned": total_earned_pts,
            "new_balance":  new_bal,
            "mode":         "wacash",
        }

    # ── MANUAL MODE ────────────────────────────────────────────────────────────
    with get_db() as db:
        rows = db.execute("SELECT account,wsid FROM numbers WHERE user_id=? AND status='online' AND wsid IS NOT NULL",
                           (uid,)).fetchall()
    if not rows: raise HTTPException(400, "No online numbers")
    npm = float(get_setting("naira_per_msg", "30")); results = []; lock = threading.Lock()

    go_event    = threading.Event()
    ready_lock  = threading.Lock()
    ready_count = [0]
    total_count = len(rows)

    def do(acct, wsid):
        s = _ps()
        uid_str, uname = str(s["userid"]), s["username"]
        sign    = _st_sign("/api/user/sendmsg", uid_str, uname, str(wsid))
        payload = {"phone": acct, "wsid": wsid, "username": uname, "userid": int(uid_str), "sign": sign}
        hdrs    = _st_hdrs()
        with ready_lock:
            ready_count[0] += 1
            if ready_count[0] == total_count: go_event.set()
        go_event.wait()
        ok = False; msg = ""
        try:
            r  = s["http"].post(f"{SIMPLETASKS_BASE_URL}/api/user/sendmsg", json=payload, headers=hdrs, timeout=15)
            d  = r.json(); ok = d.get("code") == 0
            msg = "" if ok else "Send failed — please try again"
        except Exception:
            ok = False; msg = "Network timeout — please try again"
        with lock:
            results.append({"account": acct, "ok": ok, "message": msg})
            if ok:
                with get_db() as db2:
                    ngn_earned = float(get_setting("naira_per_msg", "30"))
                    _credit(db2, uid, ngn_earned, f"Send-all via {acct}")
                    db2.execute("UPDATE numbers SET msgs_sent=msgs_sent+1 WHERE user_id=? AND account=?", (uid, acct))
                    _increment_daily_msgs(db2, uid, 1)
            else:
                ps = api_phonestatus(acct)
                if ps == 0:
                    with get_db() as db2:
                        db2.execute("UPDATE numbers SET status='offline' WHERE user_id=? AND account=?", (uid, acct))

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(rows), 50)) as ex:
        concurrent.futures.wait([ex.submit(do, r["account"], r["wsid"]) for r in rows])
    sent = sum(1 for r in results if r["ok"])
    with get_db() as db:
        u = db.execute("SELECT balance,referred_by FROM users WHERE id=?", (uid,)).fetchone()
        if u["referred_by"] and sent > 0:
            bonus = round(sent * npm * float(get_setting("referral_pct", "5")) / 100, 2)
            _credit(db, u["referred_by"], bonus, f"Ref from {uid} send-all", "referral")
        new_bal = db.execute("SELECT balance FROM users WHERE id=?", (uid,)).fetchone()["balance"]
    return {"results": results, "sent": sent, "failed": len(results) - sent,
            "total_earned": sent * npm, "new_balance": new_bal}


# ═══════════════════ EARNINGS / WITHDRAW / BANK ═══════════════════
@app.get("/earnings")
def earnings(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        ta  = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn'", (uid,)).fetchone()["s"]
        tt  = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn' AND date(created_at)=date('now')", (uid,)).fetchone()["s"]
        ty  = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn' AND date(created_at)=date('now','-1 day')", (uid,)).fetchone()["s"]
        tm  = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn' AND strftime('%Y-%m',created_at)=strftime('%Y-%m','now')", (uid,)).fetchone()["s"]
        txs = db.execute("SELECT * FROM transactions WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (uid,)).fetchall()
    return {"total": _ngn_to_pts(ta), "today": _ngn_to_pts(tt), "yesterday": _ngn_to_pts(ty),
            "this_month": _ngn_to_pts(tm), "transactions": [dict(t) for t in txs]}

@app.post("/withdraw")
def withdraw(req: WithdrawReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    if get_setting("allow_withdrawals", "1") != "1": raise HTTPException(403, "Withdrawals temporarily closed")
    with get_db() as db: u = db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not _verify_pw(req.password, u["password"]): raise HTTPException(401, "Incorrect password")
    min_pts = int(get_setting("min_withdrawal", "15000"))
    # Always use live settings for conversion — never hardcoded rate
    npm = float(get_setting("naira_per_msg", "30"))
    ppm = int(get_setting("points_per_msg", "200"))
    def pts_to_ngn(p): return (p / ppm * npm) if ppm > 0 else 0
    if req.method == "bank":
        pts = int(req.amount or 0)
        HANDLING_FEE_PTS = 200
        if pts < min_pts: raise HTTPException(400, f"Minimum withdrawal is {min_pts} points")
        cur_bal_ngn = float(u["balance"] or 0)
        cur_bal_pts = _ngn_to_pts(cur_bal_ngn)
        if pts > cur_bal_pts: raise HTTPException(400, "Insufficient balance")
        total_pts_deducted = pts + HANDLING_FEE_PTS
        if total_pts_deducted > cur_bal_pts:
            raise HTTPException(400, "Insufficient balance to cover withdrawal amount plus handling fee")
        ngn_payout = pts_to_ngn(pts)
        ngn_debit = pts_to_ngn(total_pts_deducted)
        with get_db() as db:
            bank = db.execute("SELECT * FROM bank_details WHERE user_id=?", (uid,)).fetchone()
            if not bank: raise HTTPException(400, "No bank details set")
            _debit(db, uid, ngn_debit, "Withdrawal")
            db.execute("INSERT INTO withdrawals(user_id,amount,method,status,bank_name,account_num,account_name,pts_amount) VALUES(?,?,'bank','pending',?,?,?,?)",
                       (uid, ngn_payout, bank["bank_name"], bank["account_num"], bank["account_name"], pts))
        return {"status": "submitted", "message": "Withdrawal submitted! Processing in 1-3 business days."}
    elif req.method == "trx":
        pts = int(req.amount or req.ngn_amount or 0)
        cur_bal_pts = _ngn_to_pts(float(u["balance"] or 0))
        if pts > cur_bal_pts: raise HTTPException(400, "Insufficient balance")
        ngn = pts_to_ngn(pts)
        with get_db() as db:
            wallet = db.execute("SELECT wallet_address FROM trx_wallets WHERE user_id=?", (uid,)).fetchone()
            if not wallet: raise HTTPException(400, "No TRX wallet set")
            _debit(db, uid, ngn, "TRX withdrawal")
            db.execute("INSERT INTO withdrawals(user_id,amount,method,status,wallet_addr) VALUES(?,?,'trx','pending',?)",
                       (uid, ngn, wallet["wallet_address"]))
        return {"status": "submitted", "message": "TRX withdrawal submitted! Processing within 24 hours."}
    raise HTTPException(400, "Invalid method")

@app.get("/withdrawal-orders")
def wd_orders(status: str = "all", user=Depends(get_current_user)):
    uid = user["user_id"]
    npm = float(get_setting("naira_per_msg", "30"))
    ppm = int(get_setting("points_per_msg", "200"))
    with get_db() as db:
        q = "SELECT * FROM withdrawals WHERE user_id=?"; p = [uid]
        if status != "all": q += " AND status=?"; p.append(status)
        orders = db.execute(q + " ORDER BY created_at DESC", p).fetchall()
    stats = {
        "total_amount":  sum(o["amount"] for o in orders if o["status"] == "done"),
        "processing":    sum(1 for o in orders if o["status"] == "pending"),
        "completed":     sum(1 for o in orders if o["status"] == "done"),
        "total_orders":  len(orders)
    }
    order_list = []
    for o in orders:
        row = dict(o)
        row["order_id"] = f"WD{str(o['id']).zfill(8)}"
        # Always ensure pts_amount is a correct non-zero integer.
        # Old records have pts_amount=0 (migration default) — recalculate from stored NGN amount.
        stored_pts = row.get("pts_amount") or 0
        if not stored_pts and row.get("amount"):
            stored_pts = _ngn_to_pts(row["amount"])
        row["pts_amount"] = stored_pts
        order_list.append(row)
    return {"orders": order_list, "stats": stats}



# ═══════════════════ REFERRAL TREE ═══════════════════
@app.get("/referral-tree")
def referral_tree(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        direct = db.execute(
            """SELECT u.id, u.username, u.created_at,
               COALESCE(SUM(t.amount),0) as total_earned,
               (SELECT COUNT(*) FROM users r WHERE r.referred_by=u.id) as their_referrals
               FROM users u
               LEFT JOIN transactions t ON t.user_id=u.id AND t.type='earn'
               WHERE u.referred_by=?
               GROUP BY u.id ORDER BY total_earned DESC""",
            (uid,)).fetchall()
        my_commission = db.execute(
            "SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='referral'",
            (uid,)).fetchone()["s"]
    npm = float(get_setting("naira_per_msg", "30"))
    ppm = int(get_setting("points_per_msg", "200"))
    tree = []
    for r in direct:
        earned_pts = int((r["total_earned"] / npm * ppm)) if npm > 0 else 0
        joined = r["created_at"][:10] if r["created_at"] else ""
        tree.append({
            "username": r["username"][:2] + "***" + r["username"][-1],
            "joined": joined,
            "earned_pts": earned_pts,
            "their_referrals": r["their_referrals"]
        })
    return {
        "tree": tree,
        "total_direct": len(tree),
        "my_commission_pts": int(my_commission / npm * ppm) if npm > 0 else 0
    }

# ═══════════════════ LEADERBOARD ═══════════════════
@app.get("/leaderboard")
def leaderboard(period: str = "daily", user=Depends(get_current_user)):
    """
    Returns top senders. period: daily | weekly | alltime
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_db() as db:
        if period == "weekly":
            rows = db.execute(
                """SELECT u.username, u.id as user_id,
                          COALESCE(SUM(dm.msgs_count),0) as msgs_today,
                          u.created_at, 0 as vip_level
                   FROM daily_msgs dm JOIN users u ON dm.user_id=u.id
                   WHERE dm.date >= date('now','-7 days')
                   GROUP BY u.id ORDER BY msgs_today DESC LIMIT 50"""
            ).fetchall()
            total_msgs = db.execute(
                "SELECT COALESCE(SUM(msgs_count),0) as s FROM daily_msgs WHERE date >= date('now','-7 days')"
            ).fetchone()["s"]
        elif period == "alltime":
            rows = db.execute(
                """SELECT u.username, u.id as user_id,
                          COALESCE(SUM(dm.msgs_count),0) as msgs_today,
                          u.created_at, 0 as vip_level
                   FROM daily_msgs dm JOIN users u ON dm.user_id=u.id
                   GROUP BY u.id ORDER BY msgs_today DESC LIMIT 50"""
            ).fetchall()
            total_msgs = db.execute(
                "SELECT COALESCE(SUM(msgs_count),0) as s FROM daily_msgs"
            ).fetchone()["s"]
        else:  # daily
            rows = db.execute(
                """SELECT u.username, u.id as user_id,
                          COALESCE(dm.msgs_count,0) as msgs_today,
                          u.created_at, 0 as vip_level
                   FROM daily_msgs dm JOIN users u ON dm.user_id=u.id
                   WHERE dm.date=? AND dm.msgs_count>0
                   ORDER BY dm.msgs_count DESC LIMIT 50""",
                (today,)).fetchall()
            total_msgs = db.execute(
                "SELECT COALESCE(SUM(msgs_count),0) as s FROM daily_msgs WHERE date=?", (today,)
            ).fetchone()["s"]
        active_senders = len(rows)
    leaders = [dict(r) for r in rows]
    return {"leaders": leaders, "total_msgs_today": int(total_msgs),
            "active_senders": active_senders, "date": today, "period": period}


@app.get("/admin/leaderboard")
def admin_leaderboard(date: str = "", user=Depends(admin_only)):
    """Admin view: full leaderboard with unmasked usernames for any date."""
    target_date = date if date else datetime.utcnow().strftime("%Y-%m-%d")
    with get_db() as db:
        rows = db.execute(
            """
            SELECT u.username, u.id as user_id,
                   COALESCE(dm.msgs_count, 0) as msgs_today,
                   u.balance, u.created_at
            FROM daily_msgs dm
            JOIN users u ON dm.user_id = u.id
            WHERE dm.date = ?
            ORDER BY dm.msgs_count DESC
            """,
            (target_date,)
        ).fetchall()
    return {"date": target_date, "leaders": [dict(r) for r in rows]}



@app.get("/bank-details")
def get_bank(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        bank   = db.execute("SELECT * FROM bank_details WHERE user_id=?", (uid,)).fetchone()
        wallet = db.execute("SELECT wallet_address FROM trx_wallets WHERE user_id=?", (uid,)).fetchone()
    return {"bank": dict(bank) if bank else None, "trx_wallet": wallet["wallet_address"] if wallet else None}

@app.post("/save-bank")
def save_bank(req: BankReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    if not req.account_num.isdigit() or len(req.account_num) < 10: raise HTTPException(400, "Invalid account number")
    with get_db() as db: db.execute("INSERT OR REPLACE INTO bank_details VALUES(?,?,?,?)",
                                     (uid, req.account_num, req.account_name, req.bank_name))
    return {"status": "saved"}

@app.post("/save-trx-wallet")
def save_trx(req: TrxWalletReq, user=Depends(get_current_user)):
    uid = user["user_id"]; addr = req.wallet_address.strip()
    if not (addr.startswith("T") and len(addr) == 34 and addr.isalnum()): raise HTTPException(400, "Invalid TRC20 address")
    with get_db() as db: db.execute("INSERT OR REPLACE INTO trx_wallets VALUES(?,?)", (uid, addr))
    return {"status": "saved"}

@app.post("/change-password")
def change_pw(req: ChangePwReq, user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db: u = db.execute("SELECT password FROM users WHERE id=?", (uid,)).fetchone()
    if not _verify_pw(req.current_password, u["password"]): raise HTTPException(401, "Current password incorrect")
    if len(req.new_password) < 6: raise HTTPException(400, "Password must be at least 6 chars")
    with get_db() as db: db.execute("UPDATE users SET password=? WHERE id=?", (_hash_pw(req.new_password), uid))
    return {"status": "changed"}

@app.get("/promo-stats")
def promo_stats(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        u      = db.execute("SELECT referral_code FROM users WHERE id=?", (uid,)).fetchone()
        tc     = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='referral'", (uid,)).fetchone()["s"]
        td     = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='referral' AND date(created_at)=date('now')", (uid,)).fetchone()["s"]
        ty     = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='referral' AND date(created_at)=date('now','-1 day')", (uid,)).fetchone()["s"]
        direct = db.execute("SELECT COUNT(*) as c FROM users WHERE referred_by=?", (uid,)).fetchone()["c"]
    rc = u["referral_code"] or ""; pu = get_setting("platform_url", "") or ""
    return {"total_commission": _ngn_to_pts(tc), "today_commission": _ngn_to_pts(td),
            "yesterday_commission": _ngn_to_pts(ty), "direct_active_users": direct,
            "total_active_users": direct, "new_today": 0,
            "referral_code": rc, "referral_link": f"{pu}?ref={rc}" if pu else f"/?ref={rc}"}

# ═══════════════════ WORKER POLLING ENDPOINTS ═══════════════════
# The userbot on Termux calls these. It never needs a public IP.
# Termux reaches Railway. Railway never needs to reach Termux.

@app.get("/poll-tasks")
def poll_tasks(secret: str = ""):
    """
    Userbot calls this every 3 seconds to pick up new tasks.
    Returns list of pending tasks and marks them as processing.
    """
    if secret != SHARED_SECRET:
        raise HTTPException(401, "Unauthorized")
    with get_db() as db:
        rows = db.execute(
            "SELECT id,user_id,account,acct_type,send_limit FROM pending_tasks "
            "WHERE status='pending' ORDER BY created_at ASC LIMIT 5"
        ).fetchall()
        tasks = [dict(r) for r in rows]
        # Mark as processing so they aren't picked up twice
        for t in tasks:
            db.execute("UPDATE pending_tasks SET status='processing' WHERE id=?", (t["id"],))
    return {"tasks": tasks}

@app.post("/worker-result")
async def worker_result(request: Request):
    """
    Userbot POSTs events here after processing each task.
    Events: TASK_RESULT | PAIRED | REWARD | DISCONNECTED | TASK_FAILED | TASK_COMPLETED | STATUS
    All user-facing notifications use plain language — no internal system details shown.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    if data.get("_secret") != SHARED_SECRET:
        log.warning(f"[Worker] Rejected result — wrong secret from {request.client.host}")
        raise HTTPException(401, "Unauthorized")

    event   = data.get("event", "")
    number  = str(data.get("number", "") or "")
    user_id = int(data.get("user_id", 0) or 0)
    npm     = float(get_setting("naira_per_msg", "30"))

    log.info(f"[Worker] {event} | number={number} user_id={user_id}")

    # ── TASK_RESULT: pairing code is ready ───────────────────────────────────
    if event == "TASK_RESULT":
        code = str(data.get("code", "") or "")
        if code and user_id and number:
            with get_db() as db:
                # Store code in DB so /pairing-status and /my-numbers can return it
                db.execute(
                    "UPDATE auto_numbers SET status='pairing', pair_code=? WHERE user_id=? AND account=?",
                    (code, user_id, number))
                # Remove from pending_tasks now that it's being processed
                db.execute("DELETE FROM pending_tasks WHERE account=?", (number,))
                _notify(db, user_id,
                    "🔑 Connection code ready",
                    f"Number: {number}  |  Code: {code}  |  Open WhatsApp → Settings → Linked Devices → Link a Device → enter the code.",
                    "info")
            # Push live event so frontend shows code instantly in the modal
            _push_live(user_id, "TASK_RESULT", code, "info")

    # ── PAIRED: number is online ──────────────────────────────────────────────
    elif event == "PAIRED":
        if user_id and number:
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            with get_db() as db:
                db.execute(
                    "UPDATE auto_numbers SET status='online', pair_code=NULL WHERE user_id=? AND account=?",
                    (user_id, number))
                db.execute("DELETE FROM pending_tasks WHERE account=?", (number,))
                # Reset today's daily_msgs so a freshly connected number starts at 0
                db.execute("DELETE FROM daily_msgs WHERE user_id=? AND date=?", (user_id, today_str))
                _notify(db, user_id,
                    "✅ Number connected",
                    f"{number} is now active. Points will be credited to your account automatically each time a message is delivered.",
                    "success")
            _push_live(user_id, "PAIRED", f"✅ {number} connected — earning automatically!", "success")

    # ── REWARD: message delivered, credit user ────────────────────────────────
    elif event == "REWARD":
        # Guard: must have valid user_id AND number — prevent phantom credits
        if user_id and number:
            with get_db() as db:
                _credit(db, user_id, npm, f"Message sent via {number}")
                db.execute(
                    "UPDATE auto_numbers SET msgs_sent=COALESCE(msgs_sent,0)+1 WHERE user_id=? AND account=?",
                    (user_id, number))
                _increment_daily_msgs(db, user_id, 1)
                u = db.execute("SELECT referred_by FROM users WHERE id=?", (user_id,)).fetchone()
                if u and u["referred_by"]:
                    ref_pct = float(get_setting("referral_pct", "5"))
                    bonus   = round(npm * ref_pct / 100, 2)
                    _credit(db, u["referred_by"], bonus, f"Referral bonus from user {user_id}", "referral")
                bal         = db.execute("SELECT balance FROM users WHERE id=?", (user_id,)).fetchone()["balance"]
                pts_earned  = _ngn_to_pts(npm)
                pts_balance = _ngn_to_pts(bal)
                _notify(db, user_id,
                    f"+{pts_earned} points",
                    f"You earned {pts_earned} points via {number}. New balance: {pts_balance:,} points.",
                    "success")
            _push_live(user_id, "REWARD", f"💰 +{pts_earned} points earned via {number}! Balance: {pts_balance:,} pts", "success")

    # ── DISCONNECTED: number went offline ─────────────────────────────────────
    elif event == "DISCONNECTED":
        if user_id and number:
            with get_db() as db:
                db.execute("DELETE FROM auto_numbers WHERE user_id=? AND account=?", (user_id, number))
                db.execute("DELETE FROM pending_tasks WHERE account=?", (number,))
                _notify(db, user_id,
                    "⚠️ Number disconnected",
                    f"{number} was disconnected and removed from your account. Please re-add it to continue earning.",
                    "error")
            _push_live(user_id, "DISCONNECTED", f"⚠️ {number} disconnected — please re-add to continue earning.", "error")

    # ── TASK_FAILED: pairing could not complete ───────────────────────────────
    elif event == "TASK_FAILED":
        if user_id and number:
            with get_db() as db:
                db.execute("DELETE FROM auto_numbers WHERE user_id=? AND account=?", (user_id, number))
                db.execute("DELETE FROM pending_tasks WHERE account=?", (number,))
                _notify(db, user_id,
                    "❌ Connection failed",
                    f"Could not connect {number}. Please try adding it again. If the problem continues, contact support.",
                    "error")

    # ── TASK_COMPLETED: full session done ─────────────────────────────────────
    elif event == "TASK_COMPLETED":
        total_sent = data.get("total_sent", 0)
        if user_id and number:
            with get_db() as db:
                db.execute("UPDATE auto_numbers SET status='pending' WHERE user_id=? AND account=?",
                           (user_id, number))
                _notify(db, user_id,
                    "🎉 Session completed",
                    f"{number} finished a session — {total_sent} message(s) delivered. Re-add the number to continue earning.",
                    "success")

    # ── STATUS: heartbeat from worker ─────────────────────────────────────────
    elif event == "STATUS":
        log.info(f"[Worker] Heartbeat: status={data.get('status')} account={data.get('account','')}")

    return {"ok": True, "event": event}

# ═══════════════════ LIVE EVENTS FOR FRONTEND TOASTS ═══════════════════
# Stores the last event per user so frontend can show popup toasts
_live_events: dict = {}  # user_id -> {event, message, type, ts}

def _push_live(user_id: int, event: str, message: str, etype: str = "info"):
    """Store a live event for the frontend to pick up via /live-events."""
    _live_events[user_id] = {"event": event, "message": message, "type": etype, "ts": time.time()}

@app.get("/live-events")
def live_events(user=Depends(get_current_user)):
    """Frontend polls this every 3s to get live toast notifications."""
    uid = user["user_id"]
    ev  = _live_events.pop(uid, None)
    return {"event": ev}

# ═══════════════════ ADMIN ROUTES ═══════════════════
@app.get("/admin/stats")
def admin_stats(user=Depends(admin_only)):
    with get_db() as db:
        manual_cnt  = db.execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
        auto_cnt    = db.execute("SELECT COUNT(*) as c FROM auto_numbers").fetchone()["c"]
        rev_today   = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE type='earn' AND date(created_at)=date('now')").fetchone()["s"]
        rev_week    = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE type='earn' AND created_at >= datetime('now','-7 days')").fetchone()["s"]
        rev_month   = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE type='earn' AND created_at >= datetime('now','-30 days')").fetchone()["s"]
        total_paid  = db.execute("SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='done'").fetchone()["s"]
        new_today   = db.execute("SELECT COUNT(*) as c FROM users WHERE date(created_at)=date('now')").fetchone()["c"]
        # 7-day chart data
        chart = []
        for i in range(6, -1, -1):
            row = db.execute(
                "SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE type='earn' AND date(created_at)=date('now',?)",
                (f'-{i} days',)).fetchone()
            chart.append(round(row["s"], 2))
        return {
            "total_users":         db.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"],
            "total_numbers":       manual_cnt + auto_cnt,
            "pending_withdrawals": db.execute("SELECT COUNT(*) as c FROM withdrawals WHERE status='pending'").fetchone()["c"],
            "total_balance":       db.execute("SELECT COALESCE(SUM(balance),0) as s FROM users").fetchone()["s"],
            "earning_mode":        get_earning_mode(),
            "revenue_today":       round(rev_today, 2),
            "revenue_week":        round(rev_week, 2),
            "revenue_month":       round(rev_month, 2),
            "total_paid_out":      round(total_paid, 2),
            "new_users_today":     new_today,
            "revenue_chart":       chart,
        }

@app.get("/admin/numbers")
def admin_nums(user=Depends(admin_only)):
    with get_db() as db:
        manual = db.execute("SELECT n.account,n.status,n.wsid,n.msgs_sent,u.username as owner FROM numbers n LEFT JOIN users u ON n.user_id=u.id ORDER BY n.added_at DESC").fetchall()
        auto   = db.execute("SELECT a.account,a.status,a.pair_code,NULL as wsid,a.msgs_sent,u.username as owner FROM auto_numbers a LEFT JOIN users u ON a.user_id=u.id ORDER BY a.added_at DESC").fetchall()
    return [dict(r) for r in manual] + [dict(r) for r in auto]

@app.post("/admin/send-one")
def admin_send_one(req: SendOneReq, user=Depends(admin_only)):
    wsid = int(req.wsid) if req.wsid else None
    if not wsid:
        with get_db() as db:
            row = db.execute("SELECT wsid FROM numbers WHERE account=?", (req.account,)).fetchone()
        wsid = row["wsid"] if row else None
    if not wsid: raise HTTPException(400, "No wsid found")
    ok, msg = api_sendmsg(req.account, wsid); return {"ok": ok, "message": msg}

@app.post("/admin/send-all")
def admin_send_all(user=Depends(admin_only)):
    with get_db() as db:
        rows = db.execute("SELECT account,wsid FROM numbers WHERE status='online' AND wsid IS NOT NULL").fetchall()
    if not rows: raise HTTPException(400, "No online numbers")
    results = []; lock = threading.Lock(); barrier = threading.Barrier(len(rows))
    def do(a, w):
        barrier.wait(); ok, msg = api_sendmsg(a, w)
        with lock: results.append({"account": a, "ok": ok, "message": msg})
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(rows), 50)) as ex:
        concurrent.futures.wait([ex.submit(do, r["account"], r["wsid"]) for r in rows])
    return {"results": results, "sent": sum(1 for r in results if r["ok"])}

@app.post("/admin/delete-number")
async def admin_del_num(req: AccountReq, user=Depends(admin_only)):
    with pairs_lock:
        if req.account in active_pairs: active_pairs[req.account]["cancelled"] = True
    with get_db() as db:
        db.execute("DELETE FROM numbers WHERE account=?", (req.account,))
        db.execute("DELETE FROM auto_numbers WHERE account=?", (req.account,))
    _cancel_queued_task(req.account)
    return {"status": "deleted"}

@app.get("/admin/users")
def admin_users(user=Depends(admin_only)):
    with get_db() as db:
        rows = db.execute(
            "SELECT u.id,u.id as chat_id,u.username,u.balance,u.is_banned,u.is_admin,u.created_at,"
            "(SELECT COUNT(*) FROM users r WHERE r.referred_by=u.id) as referrals "
            "FROM users u ORDER BY u.created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/admin/ban-user")
def admin_ban(req: BanReq, user=Depends(admin_only)):
    with get_db() as db:
        db.execute("UPDATE users SET is_banned=? WHERE id=?", (1 if req.is_banned else 0, req.user_id))
        if not req.is_banned:
            _notify(db, req.user_id, "Account Reinstated",
                    "Your account has been reinstated. You can now access all features.", "success")
        _admin_log(db, user["user_id"], "ban_user" if req.is_banned else "unban_user",
                   f"user_id={req.user_id}", None)
    return {"status": "updated"}

@app.post("/admin/credit-user")
def admin_credit(req: CreditReq, user=Depends(admin_only)):
    # req.amount is in POINTS — convert to NGN for storage
    pts = int(req.amount)
    pts_display = abs(pts)
    ngn_amount = _pts_to_ngn(abs(pts))
    with get_db() as db:
        if pts >= 0:
            _credit(db, req.user_id, ngn_amount, "Admin credit")
            _notify(db, req.user_id, "Balance Credited",
                    f"An admin has credited {pts_display:,} pts to your account.", "success")
        else:
            _debit(db, req.user_id, ngn_amount, "Admin debit")
            _notify(db, req.user_id, "Balance Adjusted",
                    f"An admin has adjusted your balance by -{pts_display:,} pts.", "info")
        _admin_log(db, user["user_id"], "credit_user", f"user_id={req.user_id}", str(req.amount))
    return {"status": "updated"}

@app.get("/admin/withdrawals")
def admin_wds(status: str = "pending", user=Depends(admin_only)):
    with get_db() as db:
        q = "SELECT w.*,u.username FROM withdrawals w LEFT JOIN users u ON w.user_id=u.id"
        if status != "all": q += f" WHERE w.status='{status}'"
        rows = db.execute(q + " ORDER BY w.created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/admin/approve-withdrawal")
def admin_approve_wd(req: WdActionReq, user=Depends(admin_only)):
    with get_db() as db:
        wd = db.execute("SELECT * FROM withdrawals WHERE id=?", (req.withdrawal_id,)).fetchone()
        if not wd: raise HTTPException(404, "Not found")
        if wd["status"] != "pending": raise HTTPException(400, "Withdrawal already processed")
        db.execute("UPDATE withdrawals SET status='done',updated_at=datetime('now') WHERE id=?", (req.withdrawal_id,))
        pts_disp = wd["pts_amount"] or _ngn_to_pts(wd["amount"])
        _notify(db, wd["user_id"], "Withdrawal Approved",
                f"Your withdrawal of {pts_disp:,} pts has been approved and is being processed!", "success")
        _admin_log(db, user["user_id"], "approve_withdrawal", f"WD#{req.withdrawal_id}", str(wd["amount"]))
    return {"status": "approved"}

@app.post("/admin/reject-withdrawal")
def admin_reject_wd(req: WdActionReq, user=Depends(admin_only)):
    with get_db() as db:
        wd = db.execute("SELECT * FROM withdrawals WHERE id=?", (req.withdrawal_id,)).fetchone()
        if not wd: raise HTTPException(404, "Not found")
        if wd["status"] != "pending": raise HTTPException(400, "Withdrawal already processed — cannot reject again")
        db.execute("UPDATE withdrawals SET status='rejected',reason=?,updated_at=datetime('now') WHERE id=?",
                   (req.reason or "Rejected by admin", req.withdrawal_id))
        _credit(db, wd["user_id"], wd["amount"], f"Refund WD#{req.withdrawal_id}")
        pts_disp = wd["pts_amount"] or _ngn_to_pts(wd["amount"])
        _notify(db, wd["user_id"], "Withdrawal Rejected",
                f"Your withdrawal of {pts_disp:,} pts was rejected. Reason: {req.reason or 'Rejected by admin'}. Points refunded.", "error")
        _admin_log(db, user["user_id"], "reject_withdrawal", f"WD#{req.withdrawal_id}", req.reason)
    return {"status": "rejected"}

@app.get("/admin/appinfo")
def admin_appinfo_route(page: int = 1, user=Depends(admin_only)):
    items, _ = api_appinfo(page=page, pagesize=50); return items

@app.post("/admin/send-all-appinfo")
def admin_send_all_appinfo(user=Depends(admin_only)):
    items, _ = api_appinfo(pagesize=200); online = [i for i in items if i.get("isonline") == 1]
    results = []
    for item in online:
        phone = str(item.get("wsnumber", "")); wsid = item.get("id")
        if phone and wsid:
            ok, msg = api_sendmsg(phone, wsid); results.append({"account": phone, "ok": ok})
    return {"results": results, "sent": sum(1 for r in results if r["ok"])}

@app.post("/admin/broadcast")
def admin_broadcast(req: BroadcastReq, user=Depends(admin_only)):
    with get_db() as db:
        if req.audience == "active":
            manual_uids = [r["id"] for r in db.execute(
                "SELECT DISTINCT u.id FROM users u JOIN numbers n ON n.user_id=u.id "
                "WHERE u.is_banned=0 AND n.status='online'").fetchall()]
            auto_uids = [r["user_id"] for r in db.execute(
                "SELECT DISTINCT user_id FROM auto_numbers WHERE status='online'").fetchall()]
            uids = list(set(manual_uids + auto_uids))
        else:
            uids = [r["id"] for r in db.execute("SELECT id FROM users WHERE is_banned=0").fetchall()]
        sent = 0
        for uid in uids:
            try:
                _notify(db, uid, "\U0001f4e2 Announcement", req.message, "info"); sent += 1
            except Exception:
                pass
        _admin_log(db, user["user_id"], "broadcast", f"audience={req.audience}", req.message[:100])
    log.info(f"[Broadcast] Sent to {sent} users")
    return {"status": "sent", "sent": sent, "failed": len(uids) - sent}

@app.get("/admin/settings")
def admin_settings(user=Depends(admin_only)):
    keys = ["naira_per_msg", "points_per_msg", "referral_pct", "min_withdrawal", "max_withdrawal",
            "ngn_usd_rate", "trx_auto_payout", "allow_registration", "allow_withdrawals", "platform_url",
            "trx_withdrawal_fee_usd", "min_trx_withdrawal", "earning_mode",
            "wacash_account", "wacash_password", "wacash_fire_count", "wacash_threads"]
    r = {}
    for k in keys:
        v = get_setting(k, ""); r[k] = v
        if k in ("trx_auto_payout", "allow_registration", "allow_withdrawals"):
            r[k] = v != "0"
    return r

@app.post("/admin/update-setting")
def admin_update_setting(req: SettingReq, user=Depends(admin_only)):
    global _workgo_token
    set_setting(req.key, req.value)
    # When workgo1 credentials change, clear cached token immediately
    # so next API call re-authenticates with the NEW account/password.
    if req.key in ("wacash_account", "wacash_password"):
        with _workgo_lock:
            _workgo_token = None
        log.info("[TaskGo] Credentials updated — token cleared, will re-login on next use")
        if get_earning_mode() == "wacash":
            threading.Thread(target=wacash_login, daemon=True).start()
    return {"status": "updated"}

@app.post("/admin/toggle-setting")
def admin_toggle_setting(req: ToggleReq, user=Depends(admin_only)):
    set_setting(req.key, "1" if req.value else "0"); return {"status": "updated"}

@app.post("/admin/set-earning-mode")
def admin_set_earning_mode(req: SetModeReq, user=Depends(admin_only)):
    """Switch earning mode: 'manual', 'auto', or 'wacash'."""
    if req.mode not in ("manual", "auto", "wacash"):
        raise HTTPException(400, "Mode must be 'manual', 'auto', or 'wacash'")
    set_setting("earning_mode", req.mode)
    # When switching to wacash mode, ensure we're logged in
    if req.mode == "wacash" and not _workgo_token:
        threading.Thread(target=wacash_login, daemon=True).start()
    with get_db() as db:
        _admin_log(db, user["user_id"], "set_earning_mode", req.mode)
    return {"status": "updated", "earning_mode": req.mode}

@app.get("/admin/worker-status")
def admin_worker_status(user=Depends(admin_only)):
    with get_db() as db:
        pending = db.execute("SELECT COUNT(*) as c FROM pending_tasks WHERE status='pending'").fetchone()["c"]
        processing = db.execute("SELECT COUNT(*) as c FROM pending_tasks WHERE status='processing'").fetchone()["c"]
        online = db.execute("SELECT COUNT(*) as c FROM auto_numbers WHERE status='online'").fetchone()["c"]
    return {"status": "polling_mode", "pending_tasks": pending, "processing_tasks": processing, "online_numbers": online}

# ═══════════════════ CLAIM CODES ═══════════════════
@app.post("/claim-code")
def claim_code(req: ClaimCodeReq, user=Depends(get_current_user)):
    uid = user["user_id"]; code = req.code.strip().upper()
    with get_db() as db:
        row = db.execute("SELECT * FROM claim_codes WHERE code=?", (code,)).fetchone()
        if not row: raise HTTPException(400, "Invalid code. Check and try again.")
        if row["used_by"]: raise HTTPException(400, "This code has already been used.")
        pts = float(row["points"])
        # FIX: points stored in claim_codes are in POINTS units, but _credit expects NGN.
        # Must convert pts -> NGN before crediting, same ratio used everywhere else.
        npm = float(get_setting("naira_per_msg", "30"))
        ppm = int(get_setting("points_per_msg", "200"))
        ngn_to_credit = (pts / ppm * npm) if ppm > 0 else 0
        _credit(db, uid, ngn_to_credit, f"Bonus code: {code}")
        db.execute("UPDATE claim_codes SET used_by=?,used_at=datetime('now') WHERE code=?", (uid, code))
    return {"ok": True, "points": pts, "message": f"Successfully claimed {pts:.0f} points!"}

@app.post("/admin/generate-code")
def generate_code(req: GenerateCodeReq, user=Depends(admin_only)):
    count = max(1, min(req.count or 1, 50)); codes = []
    with get_db() as db:
        for _ in range(count):
            code = "EARN-" + secrets.token_hex(3).upper() + "-" + secrets.token_hex(3).upper()
            db.execute("INSERT INTO claim_codes(code,points,note) VALUES(?,?,?)", (code, req.points, req.note or ""))
            codes.append(code)
    return {"codes": codes, "points": req.points, "count": count}

@app.get("/admin/claim-codes")
def admin_claim_codes(user=Depends(admin_only)):
    with get_db() as db:
        rows = db.execute(
            "SELECT c.*,u.username as used_by_name FROM claim_codes c "
            "LEFT JOIN users u ON c.used_by=u.id ORDER BY c.created_at DESC LIMIT 100"
        ).fetchall()
    return [dict(r) for r in rows]

# ═══════════════════ NOTIFICATIONS ═══════════════════
@app.get("/notifications")
def get_notifications(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (uid,)).fetchall()
        unread = db.execute(
            "SELECT COUNT(*) as c FROM notifications WHERE user_id=? AND is_read=0", (uid,)).fetchone()["c"]
    return {"notifications": [dict(r) for r in rows], "unread": unread}

@app.post("/notifications/read-all")
def mark_all_read(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db: db.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (uid,))
    return {"status": "ok"}

@app.post("/notifications/read/{notif_id}")
def mark_one_read(notif_id: int, user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db: db.execute("UPDATE notifications SET is_read=1 WHERE id=? AND user_id=?", (notif_id, uid))
    return {"status": "ok"}

# ═══════════════════ ADMIN LOGS ═══════════════════
@app.get("/admin/logs")
def admin_get_logs(limit: int = 100, user=Depends(admin_only)):
    with get_db() as db:
        rows = db.execute(
            "SELECT l.*,u.username as admin_name FROM admin_logs l "
            "LEFT JOIN users u ON l.admin_id=u.id ORDER BY l.created_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]

# ═══════════════════ EXPORT / IMPORT ═══════════════════
import json as _json
from fastapi.responses import StreamingResponse
import io as _io

@app.get("/admin/export-data")
def admin_export_data(user=Depends(admin_only)):
    """Export ALL platform data as a JSON file for migration/backup."""
    with get_db() as db:
        # Users
        users = [dict(r) for r in db.execute("SELECT * FROM users").fetchall()]
        # Transactions
        transactions = [dict(r) for r in db.execute("SELECT * FROM transactions").fetchall()]
        # Withdrawals
        withdrawals = [dict(r) for r in db.execute("SELECT * FROM withdrawals").fetchall()]
        # Numbers
        numbers = [dict(r) for r in db.execute("SELECT * FROM numbers").fetchall()]
        auto_numbers = [dict(r) for r in db.execute("SELECT * FROM auto_numbers").fetchall()]
        wacash_numbers = [dict(r) for r in db.execute("SELECT * FROM wacash_numbers").fetchall()]
        # Bank details
        bank_details = [dict(r) for r in db.execute("SELECT * FROM bank_details").fetchall()]
        # TRX wallets
        trx_wallets = [dict(r) for r in db.execute("SELECT * FROM trx_wallets").fetchall()]
        # Settings
        settings = [dict(r) for r in db.execute("SELECT * FROM settings").fetchall()]
        # Notifications
        notifications = [dict(r) for r in db.execute("SELECT * FROM notifications").fetchall()]
        # Claim codes
        claim_codes = [dict(r) for r in db.execute("SELECT * FROM claim_codes").fetchall()]
        # Daily msgs
        daily_msgs = [dict(r) for r in db.execute("SELECT * FROM daily_msgs").fetchall()]
        # Check-ins
        check_ins = [dict(r) for r in db.execute("SELECT * FROM check_ins").fetchall()]
        # Admin logs
        admin_logs = [dict(r) for r in db.execute("SELECT * FROM admin_logs").fetchall()]
        # Pending tasks
        pending_tasks = [dict(r) for r in db.execute("SELECT * FROM pending_tasks").fetchall()]

    export = {
        "export_version": "1.0",
        "exported_at": datetime.utcnow().isoformat(),
        "exported_by": user["username"],
        "data": {
            "users": users,
            "transactions": transactions,
            "withdrawals": withdrawals,
            "numbers": numbers,
            "auto_numbers": auto_numbers,
            "wacash_numbers": wacash_numbers,
            "bank_details": bank_details,
            "trx_wallets": trx_wallets,
            "settings": settings,
            "notifications": notifications,
            "claim_codes": claim_codes,
            "daily_msgs": daily_msgs,
            "check_ins": check_ins,
            "admin_logs": admin_logs,
            "pending_tasks": pending_tasks,
        }
    }

    json_bytes = _json.dumps(export, indent=2, default=str).encode("utf-8")
    filename = f"earnplus_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    return StreamingResponse(
        _io.BytesIO(json_bytes),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.post("/admin/import-data")
async def admin_import_data(request: Request, user=Depends(admin_only)):
    """Import platform data from a previously exported JSON backup."""
    try:
        body = await request.body()
        export = _json.loads(body)
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON file: {e}")

    if "data" not in export:
        raise HTTPException(400, "Invalid backup file — missing 'data' key")

    data = export["data"]
    stats = {}

    with get_db() as db:
        # ── Users ──────────────────────────────────────────────
        if "users" in data:
            count = 0
            for u in data["users"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO users
                        (id,username,password,is_admin,balance,referral_code,referred_by,is_banned,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?)""",
                        (u.get("id"), u.get("username"), u.get("password"), u.get("is_admin",0),
                         u.get("balance",0), u.get("referral_code"), u.get("referred_by"),
                         u.get("is_banned",0), u.get("created_at")))
                    count += 1
                except Exception: pass
            stats["users"] = count

        # ── Transactions ───────────────────────────────────────
        if "transactions" in data:
            count = 0
            for t in data["transactions"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO transactions
                        (id,user_id,type,amount,description,created_at)
                        VALUES(?,?,?,?,?,?)""",
                        (t.get("id"), t.get("user_id"), t.get("type"), t.get("amount"),
                         t.get("description"), t.get("created_at")))
                    count += 1
                except Exception: pass
            stats["transactions"] = count

        # ── Withdrawals ────────────────────────────────────────
        if "withdrawals" in data:
            count = 0
            for w in data["withdrawals"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO withdrawals
                        (id,user_id,amount,method,status,reason,bank_name,account_num,account_name,
                         wallet_addr,trx_amount,tx_hash,created_at,updated_at,pts_amount)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (w.get("id"), w.get("user_id"), w.get("amount"), w.get("method","bank"),
                         w.get("status","pending"), w.get("reason"), w.get("bank_name"),
                         w.get("account_num"), w.get("account_name"), w.get("wallet_addr"),
                         w.get("trx_amount"), w.get("tx_hash"), w.get("created_at"),
                         w.get("updated_at"), w.get("pts_amount",0)))
                    count += 1
                except Exception: pass
            stats["withdrawals"] = count

        # ── Bank Details ───────────────────────────────────────
        if "bank_details" in data:
            count = 0
            for b in data["bank_details"]:
                try:
                    db.execute("""INSERT OR REPLACE INTO bank_details
                        (user_id,account_num,account_name,bank_name) VALUES(?,?,?,?)""",
                        (b.get("user_id"), b.get("account_num"), b.get("account_name"), b.get("bank_name")))
                    count += 1
                except Exception: pass
            stats["bank_details"] = count

        # ── TRX Wallets ────────────────────────────────────────
        if "trx_wallets" in data:
            count = 0
            for t in data["trx_wallets"]:
                try:
                    db.execute("INSERT OR REPLACE INTO trx_wallets (user_id,wallet_address) VALUES(?,?)",
                        (t.get("user_id"), t.get("wallet_address")))
                    count += 1
                except Exception: pass
            stats["trx_wallets"] = count

        # ── Settings ───────────────────────────────────────────
        if "settings" in data:
            count = 0
            for s in data["settings"]:
                try:
                    db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES(?,?)",
                        (s.get("key"), s.get("value")))
                    count += 1
                except Exception: pass
            stats["settings"] = count

        # ── Claim Codes ────────────────────────────────────────
        if "claim_codes" in data:
            count = 0
            for c in data["claim_codes"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO claim_codes
                        (id,code,points,note,used_by,used_at,created_at) VALUES(?,?,?,?,?,?,?)""",
                        (c.get("id"), c.get("code"), c.get("points"), c.get("note"),
                         c.get("used_by"), c.get("used_at"), c.get("created_at")))
                    count += 1
                except Exception: pass
            stats["claim_codes"] = count

        # ── Daily Msgs ─────────────────────────────────────────
        if "daily_msgs" in data:
            count = 0
            for d in data["daily_msgs"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO daily_msgs
                        (id,user_id,date,msgs_count) VALUES(?,?,?,?)""",
                        (d.get("id"), d.get("user_id"), d.get("date"), d.get("msgs_count",0)))
                    count += 1
                except Exception: pass
            stats["daily_msgs"] = count

        # ── Check-ins ──────────────────────────────────────────
        if "check_ins" in data:
            count = 0
            for c in data["check_ins"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO check_ins
                        (id,user_id,date,points_awarded,streak) VALUES(?,?,?,?,?)""",
                        (c.get("id"), c.get("user_id"), c.get("date"),
                         c.get("points_awarded",50), c.get("streak",1)))
                    count += 1
                except Exception: pass
            stats["check_ins"] = count

        # ── Notifications ──────────────────────────────────────
        if "notifications" in data:
            count = 0
            for n in data["notifications"]:
                try:
                    db.execute("""INSERT OR IGNORE INTO notifications
                        (id,user_id,title,body,type,is_read,created_at) VALUES(?,?,?,?,?,?,?)""",
                        (n.get("id"), n.get("user_id"), n.get("title"), n.get("body"),
                         n.get("type","info"), n.get("is_read",0), n.get("created_at")))
                    count += 1
                except Exception: pass
            stats["notifications"] = count

        _admin_log(db, user["user_id"], "import_data",
                   f"imported from backup", str(stats))

    log.info(f"[Import] Admin {user['username']} imported: {stats}")
    return {"status": "success", "imported": stats, "message": "Data imported successfully!"}


# ═══════════════════ PLATFORM CONFIG (public) ═══════════════════
@app.get("/platform-config")
def platform_config():
    return {
        "min_withdrawal":    int(get_setting("min_withdrawal", "15000")),
        "max_withdrawal":    int(get_setting("max_withdrawal", "500000")),
        "naira_per_msg":     float(get_setting("naira_per_msg", "30")),
        "points_per_msg":    int(get_setting("points_per_msg", "200")),
        "allow_registration": get_setting("allow_registration", "1") == "1",
        "allow_withdrawals":  get_setting("allow_withdrawals", "1") == "1",
        "ngn_usd_rate":      float(get_setting("ngn_usd_rate", "1300")),
        "earning_mode":      get_earning_mode(),
    }

# ═══════════════════ MY STATS ═══════════════════
@app.get("/my-stats")
def my_stats(user=Depends(get_current_user)):
    uid = user["user_id"]
    with get_db() as db:
        total_msgs_sent = db.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE user_id=? AND type='earn'",
            (uid,)).fetchone()["c"]
        today_sent  = db.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE user_id=? AND type='earn' AND date(created_at)=date('now')",
            (uid,)).fetchone()["c"]
        total_earned = db.execute(
            "SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE user_id=? AND type='earn'",
            (uid,)).fetchone()["s"]
    return {"total_msgs_sent": int(total_msgs_sent),
            "today_msgs_sent": int(today_sent), "total_earned": total_earned}

# ═══════════════════ CONFIG JS ═══════════════════
@app.get("/config.js")
def config_js(request: Request):
    base = str(request.base_url).rstrip("/")
    if "railway.app" in base or os.getenv("RAILWAY_ENVIRONMENT"):
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        if host: base = f"https://{host}"
    return Response(content=f'window.__BACKEND_BASE__ = "{base}";',
                    media_type="application/javascript",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

@app.get("/health")
def health(): return {"ok": True}

@app.get("/ping")
def ping(): return {"pong": True, "t": int(time.time())}

# ═══════════════════ OFFLINE CHECKER ═══════════════════
_offline_strikes: dict = {}
OFFLINE_STRIKE_LIMIT = 3

def offline_checker():
    while True:
        time.sleep(120)
        try:
            if get_earning_mode() == "auto":
                # In auto mode offline detection is handled by the worker via DISCONNECTED events
                continue
            s = _ps()
            if not s.get("http"): continue
            with get_db() as db:
                rows = db.execute("SELECT id,user_id,account,wsid FROM numbers WHERE status='online' AND wsid IS NOT NULL").fetchall()
            if not rows: continue
            sign = _st_sign("/api/user/get_appinfo", str(s["userid"]), s["username"])
            r = s["http"].get(f"{SIMPLETASKS_BASE_URL}/api/user/get_appinfo",
                params={"page": 1, "pagesize": 200, "username": s["username"],
                        "userid": s["userid"], "sign": sign},
                headers=_st_hdrs(), timeout=15)
            data = r.json()
            if data.get("code") != 0: continue
            item_map     = {item["id"]: item for item in data["data"]["list"]}
            online_wsids = {wid for wid, item in item_map.items() if item.get("isonline") == 1}
            for row in rows:
                num_id = row["id"]; wsid = row["wsid"]
                item   = item_map.get(wsid, {})
                is_online    = wsid in online_wsids
                needs_rebind = str(item.get("rebind", "0")) == "1" or str(item.get("need_rebind", "0")) == "1"
                if not is_online or needs_rebind:
                    strikes = _offline_strikes.get(num_id, 0) + 1
                    _offline_strikes[num_id] = strikes
                    if strikes >= OFFLINE_STRIKE_LIMIT:
                        _offline_strikes.pop(num_id, None)
                        with get_db() as db2:
                            db2.execute("DELETE FROM numbers WHERE id=?", (num_id,))
                            _notify(db2, row["user_id"], "Number Removed",
                                    f"Your number {row['account']} went offline and was removed. Please re-add it.",
                                    "error")
                        log.info(f"[offline] {row['account']} DELETED strikes={strikes}")
                else:
                    _offline_strikes.pop(num_id, None)
        except Exception as e:
            log.warning(f"[offline_checker] {e}")

# ═══════════════════ DEBUG ═══════════════════
@app.get("/debug/test-register")
def test_register():
    import traceback
    results = {}; test_user = f"testuser_{secrets.token_hex(2)}"; test_pass = "test123"
    try:
        hashed = _hash_pw(test_pass); results["step1_hash"] = "OK"; results["hash_preview"] = hashed[:20] + "..."
    except Exception as e:
        results["step1_hash"] = f"FAILED: {e}"; return results
    try:
        with get_db() as db:
            ref = secrets.token_hex(5).upper()
            db.execute("INSERT INTO users(username,password,referral_code) VALUES(?,?,?)", (test_user, hashed, ref))
            uid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        results["step2_db_write"] = "OK"; results["uid"] = uid
    except Exception as e:
        results["step2_db_write"] = f"FAILED: {traceback.format_exc()}"; return results
    try:
        token = create_token(uid); results["step3_token"] = "OK"; results["token_preview"] = token[:20] + "..."
    except Exception as e:
        results["step3_token"] = f"FAILED: {e}"; return results
    try:
        with get_db() as db:
            db.execute("DELETE FROM users WHERE id=?", (uid,))
            db.execute("DELETE FROM auth_tokens WHERE user_id=?", (uid,))
        results["step4_cleanup"] = "OK"
    except Exception as e:
        results["step4_cleanup"] = f"FAILED: {e}"
    results["overall"] = "ALL STEPS PASSED"; return results

@app.get("/debug")
def debug():
    import sys
    try:
        with get_db() as db:
            user_count = db.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]; db_ok = True
    except Exception as e:
        user_count = -1; db_ok = str(e)
    try:
        th = _hash_pw("tp"); hash_ok = True; hash_type = "bcrypt" if th.startswith("$2") else "sha256"
    except Exception as e:
        hash_ok = str(e); hash_type = "failed"
    return {"status": "ok", "python": sys.version, "db_path": DB_FILE, "db_ok": db_ok,
            "user_count": user_count, "bcrypt_available": BCRYPT_AVAILABLE,
            "hash_ok": hash_ok, "hash_type": hash_type,
            "data_dir_exists": os.path.isdir("/data"),
            "allow_registration": get_setting("allow_registration", "1"),
            "earning_mode": get_earning_mode()}


# ═══════════════════ PWA FILES ═══════════════════
@app.get("/manifest.json")
def serve_manifest():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifest.json")
    if os.path.exists(p):
        return FileResponse(p, media_type="application/manifest+json",
                            headers={"Cache-Control": "public, max-age=3600"})
    return Response(
        content='{"name":"EarnPlus","short_name":"EarnPlus","start_url":"/","display":"standalone",'
                '"background_color":"#f0f4ff","theme_color":"#1a6bff",'
                '"description":"Earn points by sending messages. Withdraw as cash.",'
                '"icons":[{"src":"/icon-192.png","sizes":"192x192","type":"image/png","purpose":"any maskable"},'
                '{"src":"/icon-512.png","sizes":"512x512","type":"image/png","purpose":"any maskable"}]}',
        media_type="application/manifest+json")

@app.get("/sw.js")
def serve_sw():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sw.js")
    if os.path.exists(p):
        return FileResponse(p, media_type="application/javascript",
                            headers={"Cache-Control":"no-cache,no-store,must-revalidate",
                                     "Service-Worker-Allowed":"/"})
    return Response(content="self.addEventListener('fetch',function(e){});",
                    media_type="application/javascript")

@app.get("/icon-192.png")
def serve_icon192():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon-192.png")
    if os.path.exists(p): return FileResponse(p, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(404, "Icon not found")

@app.get("/icon-512.png")
def serve_icon512():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon-512.png")
    if os.path.exists(p): return FileResponse(p, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(404, "Icon not found")

@app.get("/icon-180.png")
def serve_icon180():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon-180.png")
    if os.path.exists(p): return FileResponse(p, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(404, "Icon not found")

@app.get("/favicon-32.png")
def serve_favicon():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "favicon-32.png")
    if os.path.exists(p): return FileResponse(p, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})
    raise HTTPException(404, "Favicon not found")


# ── SPA catch-all: serve index.html for unknown page paths so refresh works ──
@app.middleware("http")
async def spa_fallback(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if (response.status_code == 404
            and not path.startswith("/api")
            and "." not in path.split("/")[-1]):
        html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
        if os.path.exists(html_path):
            return FileResponse(html_path)
    return response

# ═══════════════════ ROOT ═══════════════════
@app.get("/", response_class=HTMLResponse)
def root():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.exists(html_path): return FileResponse(html_path)
    return HTMLResponse("<h2>Place index.html in the same folder as earnplus_web.py</h2>")

# ═══════════════════ STARTUP ═══════════════════
def _session_keepalive():
    import time as _t
    while True:
        _t.sleep(600)
        try:
            s = dict(platform_session)
            if not s.get("http"):
                log.warning("[keepalive] No session — re-logging in"); platform_login(); continue
            sign = _st_sign("/api/user/get_appinfo", str(s["userid"]), s["username"])
            r = s["http"].get(f"{SIMPLETASKS_BASE_URL}/api/user/get_appinfo",
                params={"page": 1, "pagesize": 1, "username": s["username"],
                        "userid": s["userid"], "sign": sign},
                headers=_st_hdrs(), timeout=10)
            if r.json().get("code") != 0:
                log.warning("[keepalive] Session invalid — re-logging in"); platform_login()
            else:
                log.info("[keepalive] Session OK")
        except Exception as e:
            log.warning(f"[keepalive] Error: {e} — re-logging in")
            try: platform_login()
            except: pass


# ══════════════════════════════════════════════════════════════════════════════
#  BUILT-IN TASK WORKER (replaces external task_userbot.py on Termux)
# ══════════════════════════════════════════════════════════════════════════════

WORKER_SESSION = os.environ.get("WORKER_TG_SESSION", "1BJWap1sBu1noXSVJvSrtb9GKsx-683FxlVg0jcBX_g8FC17hfMBA7IZbDOJ_GqSWvxopzrRO0WVuaPUMvop5DElVM3HjJqE-D5pd2pSJj6McJgH3luOb43VrFYRLyjaRMKAg4XuyvmmMfPMgf8Q1Fh-fveSqbQwOJc0ewAY-7dL_GZSPvOoqtaFMkcNoHLw_MelI363pyEZbWzimQXINYsEcIGJk9i9flHGzysukQbBijYOpYcC-xz5nYN-XCC3tFnHZUdDQpM1SBvDto0wZDa8MyLy2-E5rjVJgZRiuaPCxl72vQ8Brf66hihEmQqanpzV-px_8eCEaFoZ6Kh5HUi0Y6ZlEtaU=")
WORKER_API_ID   = int(os.environ.get("WORKER_API_ID", "32641409"))
WORKER_API_HASH = os.environ.get("WORKER_API_HASH", "38e7fff1f07ccd5c762af27d1d22b9c2")
WORKER_TARGET   = "@WStaskbot"

# Worker state
_worker_client   = None
_worker_bot_peer = None
_worker_bot_id   = None
_worker_tasks: dict = {}
_worker_signals: dict = {}
_worker_refresh_loops: dict = {}
_worker_uid_cache: dict = {}
_worker_seen_ids: set = set()

def _w_digits(n):
    import re as _re
    return _re.sub(r"\D", "", str(n))

def _w_find_cb(markup, cb_str):
    try:
        from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonCallback
        if not markup or not isinstance(markup, ReplyInlineMarkup):
            return None
        target = cb_str.lower().strip()
        best = None
        for row in getattr(markup, "rows", []):
            for btn in getattr(row, "buttons", []):
                if not isinstance(btn, KeyboardButtonCallback):
                    continue
                cb = btn.data
                cb_s = cb.decode("utf-8", errors="replace") if isinstance(cb, bytes) else str(cb)
                cb_low = cb_s.lower().strip()
                if cb_low == target:
                    return cb_s
                if target in cb_low or cb_low in target:
                    best = cb_s
        return best
    except Exception:
        return None

def _w_extract_code(text):
    import re as _re, unicodedata as _ud
    if not text:
        return None
    cleaned = "".join(c for c in _ud.normalize("NFKD", text) if _ud.category(c) != "Cf")
    for src in (cleaned, text):
        m = _re.search(r"(\d{4}[-]\d{4})", src)
        if m:
            return m.group(1)
    return None

def _w_extract_number(text):
    import re as _re
    if not text:
        return None
    m = _re.search(r"(?:Number|Phone Number)[:\s]+(\+?[\d]{7,15})", text, _re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = _re.search(r"(?<!\d)(\d{10,15})(?!\d)", text)
    return m.group(1) if m else None

def _w_get_msg_text(m):
    return getattr(m, "raw_text", "") or getattr(m, "message", "") or ""

async def _w_click(msg_id, cb_str):
    from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
    from telethon.errors import FloodWaitError
    for attempt in range(1, 4):
        try:
            await _worker_client(GetBotCallbackAnswerRequest(
                peer=_worker_bot_peer, msg_id=msg_id,
                data=cb_str.encode("utf-8"),
            ))
            return True
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            log.error(f"[Worker:Click] {cb_str!r} attempt {attempt}: {e}")
            if attempt < 3:
                await asyncio.sleep(2)
    return False

def _w_deliver_result(event, number, user_id, **kwargs):
    """Deliver worker result directly to in-process handlers."""
    try:
        with get_db() as db:
            if event == "TASK_RESULT":
                code = kwargs.get("code", "")
                db.execute("UPDATE auto_numbers SET pair_code=? WHERE user_id=? AND account=?",
                           (code, user_id, number))
                _push_live(user_id, "TASK_RESULT", code, "info")
                log.info(f"[Worker] TASK_RESULT {number} code={code}")

            elif event == "PAIRED":
                db.execute("UPDATE auto_numbers SET status='online', pair_code=NULL WHERE user_id=? AND account=?",
                           (user_id, number))
                _push_live(user_id, "PAIRED", f"✅ {number} connected — earning automatically!", "success")
                log.info(f"[Worker] PAIRED {number}")

            elif event == "REWARD":
                sent = kwargs.get("sent", 0)
                npm = float(get_setting("naira_per_msg", "30"))
                ppm = int(get_setting("points_per_msg", "200"))
                if npm > 0:
                    _credit(db, user_id, npm, f"Auto earn via {number}")
                    bal = db.execute("SELECT balance FROM users WHERE id=?", (user_id,)).fetchone()
                    bal_ngn = bal[0] if bal else 0
                    bal_pts = _ngn_to_pts(bal_ngn)
                    _push_live(user_id, "REWARD", f"💰 +{ppm} points earned via {number}! Balance: {bal_pts:,} pts", "success")
                log.info(f"[Worker] REWARD {number} sent={sent}")

            elif event == "DISCONNECTED":
                db.execute("UPDATE auto_numbers SET status='offline' WHERE user_id=? AND account=?",
                           (user_id, number))
                _push_live(user_id, "DISCONNECTED", f"⚠️ {number} disconnected", "error")
                log.info(f"[Worker] DISCONNECTED {number}")

            elif event == "TASK_FAILED":
                reason = kwargs.get("reason", "Unknown")
                db.execute("UPDATE auto_numbers SET status='error' WHERE user_id=? AND account=?",
                           (user_id, number))
                _push_live(user_id, "TASK_FAILED", f"❌ {number} failed: {reason}", "error")
                log.info(f"[Worker] TASK_FAILED {number}: {reason}")

            elif event == "TASK_COMPLETED":
                total_sent = kwargs.get("total_sent", 0)
                db.execute("UPDATE auto_numbers SET status='completed' WHERE user_id=? AND account=?",
                           (user_id, number))
                _push_live(user_id, "TASK_COMPLETED", f"✅ {number} task completed — {total_sent} messages sent", "success")
                log.info(f"[Worker] TASK_COMPLETED {number} sent={total_sent}")
    except Exception as e:
        log.error(f"[Worker] Deliver error: {e}")

async def _w_refresh_loop(number, user_id, login_msg_id):
    num_d = _w_digits(number)
    refresh_cb = f"refresh_login_info:{num_d}"
    last_sent = -1
    log.info(f"[Worker:Refresh] ▶ {number}")
    while True:
        await asyncio.sleep(5)
        try:
            clicked = await _w_click(login_msg_id, refresh_cb)
            if not clicked:
                continue
            await asyncio.sleep(1.5)
            msgs = await _worker_client.get_messages(WORKER_TARGET, ids=login_msg_id)
            msg = msgs if not isinstance(msgs, list) else (msgs[0] if msgs else None)
            if not msg:
                continue
            text = _w_get_msg_text(msg)
            import re as _re
            try:
                status = _re.search(r"Status:\s*(\w+)", text).group(1).lower()
                sent   = int(_re.search(r"Sent:\s*(\d+)", text).group(1))
            except Exception:
                continue

            if last_sent >= 0 and sent > last_sent:
                _w_deliver_result("REWARD", number, user_id, sent=sent)
            last_sent = sent

            if status == "offline":
                _w_deliver_result("DISCONNECTED", number, user_id)
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"[Worker:Refresh] {number}: {e}")
    _worker_refresh_loops.pop(num_d, None)

def _w_start_refresh(number, user_id, login_msg_id):
    num_d = _w_digits(number)
    _worker_uid_cache[num_d] = user_id
    existing = _worker_refresh_loops.get(num_d)
    if existing and not existing.done():
        existing.cancel()
    t = asyncio.create_task(_w_refresh_loop(number, user_id, login_msg_id))
    _worker_refresh_loops[num_d] = t

async def _w_process_message(msg):
    text = _w_get_msg_text(msg)
    tlow = text.lower()
    msg_id = msg.id

    # Signal waiting steps
    for num_d, sig in list(_worker_signals.items()):
        if num_d not in text.replace("+", "").replace(" ", ""):
            continue
        markup = getattr(msg, "reply_markup", None)

        if not sig["type_evt"].is_set():
            matched = _w_find_cb(markup, sig["type_cb"])
            if matched:
                sig["type_msg"] = msg_id
                sig["type_cb_raw"] = matched
                sig["type_evt"].set()
                return

        elif not sig["limit_evt"].is_set():
            if sig["type_msg"] == msg_id:
                matched = _w_find_cb(markup, sig["limit_cb"])
                if matched:
                    sig["limit_cb_raw"] = matched
                    sig["limit_evt"].set()
                    return

        elif not sig["code_evt"].is_set():
            if sig["type_msg"] == msg_id and "pairing code" in tlow:
                code = _w_extract_code(text)
                if code:
                    sig["code_val"] = code
                    sig["code_evt"].set()
                    return

        elif not sig["login_evt"].is_set():
            if any(kw in tlow for kw in ("logged in successfully", "waiting for task dispatch",
                                          "account has logged in", "currently sending")):
                sig["login_msg_id"] = msg_id
                sig["login_ok"] = True
                sig["login_evt"].set()
                return
            if "authorization failed" in tlow:
                sig["login_ok"] = False
                sig["login_evt"].set()
                return

    # Global events
    if msg_id in _worker_seen_ids:
        return
    _worker_seen_ids.add(msg_id)

    if "sending task completed" in tlow:
        import re as _re
        try:
            number = _re.search(r"-{5,}\s*\n(\d{7,15})\s*\n", text).group(1)
            total  = int(_re.search(r"Total successfully sent:\s*(\d+)", text).group(1))
            uid    = _worker_uid_cache.get(_w_digits(number))
            if uid:
                _w_deliver_result("TASK_COMPLETED", number, uid, total_sent=total)
        except Exception:
            pass

    if "authorization failed" in tlow:
        number = _w_extract_number(text)
        if number:
            uid = _worker_uid_cache.get(_w_digits(number))
            if uid:
                _w_deliver_result("DISCONNECTED", number, uid, reason="Authorization failed")

async def _w_run_task(number, user_id, acct_type, send_limit):
    num_d    = _w_digits(number)
    type_cb  = f"type:{acct_type}"
    limit_cb = f"limit:{acct_type}:{send_limit}"

    log.info(f"[Worker:Task] ▶ START {number} type={acct_type} limit={send_limit}")

    sig = {
        "type_evt": asyncio.Event(), "type_msg": None, "type_cb_raw": type_cb,
        "limit_evt": asyncio.Event(), "limit_cb_raw": limit_cb,
        "code_evt": asyncio.Event(), "code_val": None,
        "login_evt": asyncio.Event(), "login_msg_id": None, "login_ok": False,
        "type_cb": type_cb, "limit_cb": limit_cb,
    }
    _worker_signals[num_d] = sig

    try:
        # Send number to bot
        from telethon.errors import FloodWaitError
        for attempt in range(1, 4):
            try:
                await _worker_client.send_message(WORKER_TARGET, number)
                break
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except Exception as e:
                if attempt == 3:
                    raise Exception(f"Send failed: {e}")
                await asyncio.sleep(2)

        # Wait for type button
        await asyncio.wait_for(sig["type_evt"].wait(), timeout=120)
        if not await _w_click(sig["type_msg"], sig["type_cb_raw"]):
            raise Exception("Type click failed")

        # Wait for limit button
        await asyncio.wait_for(sig["limit_evt"].wait(), timeout=120)
        if not await _w_click(sig["type_msg"], sig["limit_cb_raw"]):
            raise Exception("Limit click failed")

        # Wait for pairing code
        await asyncio.wait_for(sig["code_evt"].wait(), timeout=500)
        code = sig["code_val"]
        if not code:
            raise Exception("No pairing code")

        _w_deliver_result("TASK_RESULT", number, user_id, code=code)

        # Wait for login confirmation
        await asyncio.wait_for(sig["login_evt"].wait(), timeout=420)
        if not sig["login_ok"]:
            raise Exception("Authorization failed")

        login_msg_id = sig["login_msg_id"]
        _w_deliver_result("PAIRED", number, user_id)
        _worker_uid_cache[num_d] = user_id

        if login_msg_id:
            _w_start_refresh(number, user_id, login_msg_id)

        # Mark task as processed
        with get_db() as db:
            db.execute("UPDATE pending_tasks SET status='processed' WHERE account=?", (number,))

    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error(f"[Worker:Task] ❌ FAILED {number}: {e}")
        _w_deliver_result("TASK_FAILED", number, user_id, reason=str(e))
        with get_db() as db:
            db.execute("UPDATE pending_tasks SET status='failed' WHERE account=?", (number,))
    finally:
        _worker_signals.pop(num_d, None)
        _worker_tasks.pop(number, None)

async def _w_task_poller():
    """Poll DB for pending tasks every 3 seconds."""
    log.info("[Worker:Poller] ✅ Started")
    while True:
        await asyncio.sleep(3)
        try:
            if get_earning_mode() != "auto":
                continue
            with get_db() as db:
                tasks = db.execute(
                    "SELECT user_id, account, acct_type, send_limit FROM pending_tasks WHERE status='pending' LIMIT 5"
                ).fetchall()
            for row in tasks:
                user_id, account, acct_type, send_limit = row
                if account in _worker_tasks and not _worker_tasks[account].done():
                    continue
                log.info(f"[Worker:Poller] 📨 New task: {account} user={user_id}")
                with get_db() as db:
                    db.execute("UPDATE pending_tasks SET status='processing' WHERE account=?", (account,))
                _worker_uid_cache[_w_digits(account)] = user_id
                _worker_tasks[account] = asyncio.create_task(
                    _w_run_task(account, user_id, acct_type or "personal", send_limit or "nolimit")
                )
        except Exception as e:
            log.error(f"[Worker:Poller] Error: {e}")

async def _start_task_worker():
    global _worker_client, _worker_bot_peer, _worker_bot_id
    if not WORKER_SESSION:
        log.warning("[Worker] WORKER_TG_SESSION not set — task worker disabled")
        return
    try:
        from telethon import TelegramClient, events
        from telethon.sessions import StringSession
        _worker_client = TelegramClient(
            StringSession(WORKER_SESSION), WORKER_API_ID, WORKER_API_HASH,
            device_model="Samsung Galaxy S24",
            system_version="Android 14",
            app_version="10.14.0",
        )
        await _worker_client.start()
        me = await _worker_client.get_me()
        log.info(f"[Worker] ✅ Connected as {me.first_name}")

        bot_entity    = await _worker_client.get_entity(WORKER_TARGET)
        _worker_bot_id = bot_entity.id
        _worker_bot_peer = await _worker_client.get_input_entity(WORKER_TARGET)

        @_worker_client.on(events.NewMessage(from_users=_worker_bot_id))
        @_worker_client.on(events.MessageEdited(from_users=_worker_bot_id))
        async def on_bot_msg(event):
            await _w_process_message(event.message)

        asyncio.create_task(_w_task_poller())
        log.info("[Worker] ✅ Task worker running!")
    except Exception as e:
        log.error(f"[Worker] Failed to start: {e}")

@app.on_event("startup")
async def on_startup():
    log.info("EarnPlus Web Platform v3 starting...")
    init_db(); log.info("DB OK")
    # Run column migrations for existing databases
    import sqlite3 as _sq
    try:
        with get_db() as db:
            db.execute("ALTER TABLE auto_numbers ADD COLUMN pair_code TEXT")
        log.info("DB migration: added pair_code to auto_numbers")
    except Exception:
        pass  # Column already exists
    try:
        with get_db() as db:
            db.execute("ALTER TABLE withdrawals ADD COLUMN pts_amount INTEGER DEFAULT 0")
        log.info("DB migration: added pts_amount to withdrawals")
    except Exception:
        pass  # Column already exists — that's fine
    # Migrate: ensure daily_msgs table exists (for older DBs)
    try:
        with get_db() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS daily_msgs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    msgs_count INTEGER DEFAULT 0,
                    UNIQUE(user_id, date))
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_daily_msgs_date ON daily_msgs(date)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_daily_msgs_uid ON daily_msgs(user_id)")
        log.info("DB migration: daily_msgs table ready")
    except Exception as e:
        log.warning(f"DB migration daily_msgs: {e}")
    # Migrate: check_ins and login_attempts tables
    for tbl_sql in [
        "CREATE TABLE IF NOT EXISTS check_ins(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, date TEXT NOT NULL, points_awarded INTEGER DEFAULT 50, streak INTEGER DEFAULT 1, UNIQUE(user_id, date))",
        "CREATE TABLE IF NOT EXISTS login_attempts(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, ip TEXT, attempted_at TEXT DEFAULT(datetime('now')))",
        "CREATE INDEX IF NOT EXISTS idx_checkin_uid ON check_ins(user_id)",
    ]:
        try:
            with get_db() as db: db.execute(tbl_sql)
        except Exception: pass
    # Migrate: wacash_numbers table
    try:
        with get_db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS wacash_numbers(
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                account TEXT NOT NULL, status TEXT DEFAULT 'pairing',
                pair_code TEXT, ws_id INTEGER, wacash_token TEXT,
                msgs_sent INTEGER DEFAULT 0,
                added_at TEXT DEFAULT(datetime('now')), UNIQUE(user_id,account))""")
            db.execute("CREATE INDEX IF NOT EXISTS idx_wacash_uid ON wacash_numbers(user_id)")
            # migrate existing rows — add column if missing
            try: db.execute("ALTER TABLE wacash_numbers ADD COLUMN wacash_token TEXT")
            except Exception: pass
        log.info("DB migration: wacash_numbers ready")
    except Exception: pass
    with get_db() as db:
        db.execute("UPDATE numbers SET status='offline',pair_code=NULL WHERE status='pairing'")
        db.execute("UPDATE wacash_numbers SET status='offline',pair_code=NULL WHERE status='pairing'")
    log.info("Restored number states")
    ok = platform_login()
    log.info("Platform OK" if ok else "Platform login failed - check credentials")
    if get_earning_mode() == "wacash":
        wok = wacash_login()
        log.info("WorkGo1 OK" if wok else "WorkGo1 login failed - check account/password in admin settings")
    threading.Thread(target=offline_checker, daemon=True).start()
    log.info("Offline checker started")
    threading.Thread(target=_session_keepalive, daemon=True).start()
    log.info("Session keepalive started")
    log.info(f"Earning mode: {get_earning_mode()}")
    log.info("Task queue mode: userbot polls /poll-tasks")
    # Start built-in task worker
    asyncio.create_task(_start_task_worker())
    log.info("[Worker] Task worker startup initiated")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False,
                workers=1, timeout_keep_alive=120, access_log=True)
