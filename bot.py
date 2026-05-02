#!/usr/bin/env python3
"""
AetherMine Telegram Bot
Bot: @aetherrmine_bot
Full version with PostgreSQL + TRON auto payment + Admin Panel + Webhook
"""

import logging
import os
import asyncio
import hashlib
import hmac
import re
import secrets
import requests
import psycopg2
import psycopg2.extras
from psycopg2 import pool as pg_pool
import json
from datetime import datetime, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# ─── CONFIG — all secrets MUST be set as environment variables ───────────────
# Never add fallback strings here. The app crashes loudly at startup if missing.
BOT_TOKEN      = os.environ["BOT_TOKEN"]
DATABASE_URL   = os.environ["DATABASE_URL"]
ADMIN_PASSWORD = os.environ["ADMIN_PASSWORD"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", os.urandom(32).hex())  # auto-gen if not set
WALLET         = "TBdoPKgRbhfpfXh6MCkhewgfs4z7nw65P8"
ADMIN_IDS      = [6740298503]
WEBAPP_URL     = "https://aethermine.vercel.app"
TRON_API       = "https://apilist.tronscanapi.com/api/token_trc20/transfers"
USDT_CONTRACT  = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
WEBHOOK_URL    = "http://TEMP_WILL_UPDATE_AFTER_EC2:8080/webhook"
PORT           = 8080

PLANS = {
    "trial":    {"name": "Trial",    "emoji": "🌱", "price": 3,   "power": 0.5, "daily": 0.25, "monthly": 7.50},
    "starter":  {"name": "Starter",  "emoji": "🔷", "price": 5,   "power": 1,   "daily": 0.5,  "monthly": 15},
    "bronze":   {"name": "Bronze",   "emoji": "🥉", "price": 10,  "power": 2.5, "daily": 1.2,  "monthly": 36},
    "silver":   {"name": "Silver",   "emoji": "🥈", "price": 25,  "power": 7,   "daily": 3.0,  "monthly": 90},
    "gold":     {"name": "Gold",     "emoji": "🥇", "price": 50,  "power": 16,  "daily": 6.5,  "monthly": 195},
    "platinum": {"name": "Platinum", "emoji": "💠", "price": 100, "power": 35,  "daily": 14.0, "monthly": 420},
    "diamond":  {"name": "Diamond",  "emoji": "💎", "price": 200, "power": 80,  "daily": 30.0, "monthly": 900},
}

AMOUNT_TO_PLAN = {3: "trial", 5: "starter", 10: "bronze", 25: "silver", 50: "gold", 100: "platinum", 200: "diamond"}

# Only Silver ($25) and above may withdraw
WITHDRAWAL_ELIGIBLE_PLANS = {"silver", "gold", "platinum", "diamond"}

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CONNECTION POOL (Fix #1) ─────────────────────────────────────────────────
_db_pool = None

def init_pool():
    global _db_pool
    _db_pool = pg_pool.ThreadedConnectionPool(2, 20, DATABASE_URL, sslmode='require')
    logger.info("✅ DB connection pool initialised (min=2, max=20)")

def get_db():
    """Borrow a connection from the pool."""
    return _db_pool.getconn()

def release_db(conn):
    """Return a connection to the pool."""
    if conn:
        _db_pool.putconn(conn)

# ─── RATE LIMITING (Fix #7) ───────────────────────────────────────────────────
_last_cmd: dict = {}
CMD_COOLDOWN_SECS = 2

def is_rate_limited(user_id: int) -> bool:
    now = datetime.utcnow()
    # P2-A: evict stale entries to prevent unbounded dict growth
    stale = [k for k, v in _last_cmd.items() if (now - v).total_seconds() > 60]
    for k in stale:
        _last_cmd.pop(k, None)
    last = _last_cmd.get(user_id)
    if last and (now - last).total_seconds() < CMD_COOLDOWN_SECS:
        return True
    _last_cmd[user_id] = now
    return False

def init_db():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                plan TEXT DEFAULT 'free',
                power FLOAT DEFAULT 0.1,
                daily_earn FLOAT DEFAULT 0.05,
                balance FLOAT DEFAULT 0.0,
                referrer_id BIGINT,
                ref_earnings FLOAT DEFAULT 0.0,
                ref_count INTEGER DEFAULT 0,
                joined_at TIMESTAMP DEFAULT NOW(),
                plan_activated_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                tx_hash TEXT UNIQUE,
                amount FLOAT,
                plan TEXT,
                status TEXT DEFAULT 'confirmed',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_tx (
                tx_hash TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payment_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                plan TEXT,
                amount FLOAT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount FLOAT,
                fee FLOAT,
                receive FLOAT,
                address TEXT,
                status TEXT DEFAULT 'pending',
                requested_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # ── Fix #2: indexes for scale ─────────────────────────────────────────
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_referrer         ON users(referrer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_plan             ON users(plan)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user          ON payments(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_created       ON payments(created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_req_user_stat  ON payment_requests(user_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_req_amount_stat ON payment_requests(amount, status, created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_status     ON withdrawals(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_user       ON withdrawals(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_tx_at        ON processed_tx(processed_at)")
        # ── P2-C: admin audit log ─────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_log (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT,
                action TEXT,
                target_user_id BIGINT,
                details TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        logger.info("✅ Database initialised with indexes")
    finally:
        cur.close()
        release_db(conn)

def get_or_create_user(user_id, username, first_name, referrer_id=None):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            cur.execute("""
                INSERT INTO users (user_id, username, first_name, referrer_id)
                VALUES (%s, %s, %s, %s) RETURNING *
            """, (user_id, username, first_name, referrer_id))
            user = cur.fetchone()
            conn.commit()
            if referrer_id:
                cur.execute("UPDATE users SET ref_count = ref_count + 1 WHERE user_id = %s", (referrer_id,))
                conn.commit()
        return user
    finally:
        cur.close()
        release_db(conn)

def get_user(user_id):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        return cur.fetchone()
    finally:
        cur.close()
        release_db(conn)

def upgrade_user_plan(user_id, plan_name):
    plan = PLANS[plan_name]
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE users SET plan=%s, power=%s, daily_earn=%s, plan_activated_at=NOW()
            WHERE user_id=%s
        """, (plan_name, plan['power'], plan['daily'], user_id))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def is_tx_processed(tx_hash):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT tx_hash FROM processed_tx WHERE tx_hash = %s", (tx_hash,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        release_db(conn)

def mark_tx_processed(tx_hash):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO processed_tx (tx_hash) VALUES (%s) ON CONFLICT DO NOTHING", (tx_hash,))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def save_payment(user_id, tx_hash, amount, plan_name):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO payments (user_id, tx_hash, amount, plan)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
        """, (user_id, tx_hash, amount, plan_name))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def create_payment_request(user_id, plan_name, amount):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE payment_requests SET status='cancelled' WHERE user_id=%s AND status='pending'", (user_id,))
        cur.execute("INSERT INTO payment_requests (user_id, plan, amount) VALUES (%s, %s, %s)", (user_id, plan_name, amount))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def get_stats():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT COUNT(*) as total FROM users")
        total_users = cur.fetchone()['total']
        cur.execute("SELECT COALESCE(SUM(amount),0) as revenue FROM payments WHERE status='confirmed'")
        total_revenue = cur.fetchone()['revenue']
        cur.execute("SELECT COALESCE(SUM(amount),0) as today FROM payments WHERE status='confirmed' AND created_at > NOW() - INTERVAL '24 hours'")
        today_revenue = cur.fetchone()['today']
        cur.execute("SELECT plan, COUNT(*) as count FROM users GROUP BY plan ORDER BY count DESC")
        plan_breakdown = cur.fetchall()
        cur.execute("SELECT COUNT(*) as new FROM users WHERE joined_at > NOW() - INTERVAL '24 hours'")
        new_today = cur.fetchone()['new']
        return {
            'total_users': total_users,
            'total_revenue': float(total_revenue),
            'today_revenue': float(today_revenue),
            'plan_breakdown': [dict(p) for p in plan_breakdown],
            'new_today': new_today
        }
    finally:
        cur.close()
        release_db(conn)

def get_all_users(limit=50, offset=0, search=None):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        if search:
            cur.execute("""
                SELECT * FROM users WHERE username ILIKE %s OR first_name ILIKE %s OR user_id::text = %s
                ORDER BY joined_at DESC LIMIT %s OFFSET %s
            """, (f'%{search}%', f'%{search}%', search, limit, offset))
        else:
            cur.execute("SELECT * FROM users ORDER BY joined_at DESC LIMIT %s OFFSET %s", (limit, offset))
        return [dict(u) for u in cur.fetchall()]
    finally:
        cur.close()
        release_db(conn)

def get_recent_payments(limit=20):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT p.*, u.first_name, u.username FROM payments p
            LEFT JOIN users u ON u.user_id = p.user_id
            ORDER BY p.created_at DESC LIMIT %s
        """, (limit,))
        return [dict(p) for p in cur.fetchall()]
    finally:
        cur.close()
        release_db(conn)

def get_all_user_ids():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id FROM users")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        release_db(conn)

# ── NEW: withdrawal DB helpers ────────────────────────────────────────────────
def save_withdrawal(user_id, amount, fee, receive, address):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO withdrawals (user_id, amount, fee, receive, address)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, amount, fee, receive, address))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def get_pending_withdrawals():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT w.*, u.first_name, u.username
            FROM withdrawals w
            LEFT JOIN users u ON u.user_id = w.user_id
            WHERE w.status = 'pending'
            ORDER BY w.requested_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        release_db(conn)

def mark_withdrawal_paid(withdrawal_id):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "UPDATE withdrawals SET status='paid' WHERE id=%s RETURNING user_id, receive, address",
            (withdrawal_id,)
        )
        row = cur.fetchone()
        conn.commit()
        return dict(row) if row else None
    finally:
        cur.close()
        release_db(conn)

def sync_user_balance(user_id, balance):
    """Persist mined balance from frontend to DB."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET balance=%s WHERE user_id=%s", (balance, user_id))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)

def get_max_withdrawable(user_id: int) -> float:
    """P2-B: Compute the maximum a user can withdraw based on plan + days active."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT balance, daily_earn, plan_activated_at FROM users WHERE user_id=%s", (user_id,))
        u = cur.fetchone()
        if not u:
            return 0.0
        return float(u["balance"] or 0)
    finally:
        cur.close()
        release_db(conn)

def has_pending_withdrawal(user_id: int) -> bool:
    """P2-B: True if user already has a withdrawal requested in the last 24 h."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 1 FROM withdrawals
            WHERE user_id=%s AND requested_at > NOW() - INTERVAL '24 hours'
              AND status IN ('pending', 'paid')
            LIMIT 1
        """, (user_id,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        release_db(conn)

# ─── GLOBAL REFERENCES ───────────────────────────────────────────────────────
_app = None
_main_loop = None

# ── P0-B: Server-side admin session store ────────────────────────────────────
# Maps session_token -> datetime of issue. Tokens expire after 8 hours.
_admin_sessions: dict = {}
SESSION_TTL_HOURS = 8

def create_session() -> str:
    """Issue a new cryptographically random session token."""
    token = secrets.token_hex(32)
    _admin_sessions[token] = datetime.utcnow()
    return token

def is_valid_session(token: str) -> bool:
    """Return True only if token exists and is not expired."""
    if not token:
        return False
    issued = _admin_sessions.get(token)
    if not issued:
        return False
    if (datetime.utcnow() - issued).total_seconds() > SESSION_TTL_HOURS * 3600:
        _admin_sessions.pop(token, None)
        return False
    return True

def get_session_token(handler) -> str:
    """Extract Bearer token from Authorization header."""
    auth = handler.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return ""

def require_admin_session(handler) -> bool:
    """Return True if request carries a valid session, else send 401 and return False."""
    if is_valid_session(get_session_token(handler)):
        return True
    handler._respond(401, 'application/json', b'{"error":"Unauthorized"}')
    return False

# ── P0-D: Telegram init-data HMAC verification ───────────────────────────────
def verify_telegram_init_data(init_data: str) -> int | None:
    """
    Verify Telegram WebApp initData HMAC.
    Returns the user_id on success, None on failure.
    """
    try:
        params = dict(p.split("=", 1) for p in init_data.split("&") if "=" in p)
        received_hash = params.pop("hash", "")
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(params.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, received_hash):
            return None
        user_info = json.loads(params.get("user", "{}"))
        return user_info.get("id")
    except Exception:
        return None

# ── P2-C: Admin audit log ─────────────────────────────────────────────────────
def log_admin_action(admin_id: int, action: str, target_user_id: int | None, details: str):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO admin_log (admin_id, action, target_user_id, details)
            VALUES (%s, %s, %s, %s)
        """, (admin_id, action, target_user_id, details))
        conn.commit()
    except Exception:
        pass
    finally:
        cur.close()
        release_db(conn)

# ─── WEB SERVER (Webhook + Admin Dashboard + Health) ──────────────────────────
DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>AetherMine Admin</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{background:#03050f;color:#e8f4ff;font-family:'Rajdhani',sans-serif;min-height:100vh;}
.login-wrap{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px;}
.login-box{background:rgba(12,21,50,0.95);border:1px solid rgba(0,212,255,0.2);border-radius:20px;padding:40px;width:100%;max-width:400px;text-align:center;}
.logo{font-family:'Orbitron',sans-serif;font-size:24px;font-weight:900;background:linear-gradient(90deg,#00d4ff,#7b5fff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px;}
.logo-sub{font-size:13px;color:#7a9cc4;margin-bottom:30px;}
input,select{width:100%;padding:12px 16px;background:rgba(255,255,255,0.05);border:1px solid rgba(0,212,255,0.2);border-radius:10px;color:#e8f4ff;font-family:'Rajdhani',sans-serif;font-size:15px;margin-bottom:14px;outline:none;}
input:focus,select:focus{border-color:#00d4ff;}
select option{background:#1a1a2e;}
.btn{width:100%;padding:13px;border-radius:10px;border:none;background:linear-gradient(90deg,#00d4ff,#7b5fff);color:#fff;font-family:'Orbitron',sans-serif;font-size:13px;font-weight:700;cursor:pointer;letter-spacing:1px;}
.err{color:#ff6b35;font-size:13px;margin-top:10px;}
#dashboard{display:none;padding:20px;}
.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid rgba(0,212,255,0.15);}
.header-logo{font-family:'Orbitron',sans-serif;font-size:20px;font-weight:900;background:linear-gradient(90deg,#00d4ff,#7b5fff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.logout{background:rgba(255,107,53,0.15);border:1px solid rgba(255,107,53,0.3);border-radius:8px;padding:6px 14px;color:#ff6b35;font-size:12px;cursor:pointer;font-family:'Rajdhani',sans-serif;font-weight:600;}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin-bottom:24px;}
.stat-card{background:rgba(12,21,50,0.8);border:1px solid rgba(0,212,255,0.15);border-radius:16px;padding:18px;text-align:center;}
.stat-num{font-family:'Orbitron',sans-serif;font-size:28px;font-weight:900;color:#00d4ff;}
.stat-label{font-size:12px;color:#7a9cc4;margin-top:4px;text-transform:uppercase;letter-spacing:1px;}
.stat-card.green .stat-num{color:#00ff9d;}
.stat-card.purple .stat-num{color:#7b5fff;}
.stat-card.orange .stat-num{color:#ff6b35;}
.section{background:rgba(12,21,50,0.8);border:1px solid rgba(0,212,255,0.15);border-radius:16px;padding:20px;margin-bottom:20px;}
.section-title{font-family:'Orbitron',sans-serif;font-size:13px;font-weight:700;color:#00d4ff;margin-bottom:16px;letter-spacing:1px;}
.search-bar{width:100%;padding:10px 14px;background:rgba(255,255,255,0.05);border:1px solid rgba(0,212,255,0.2);border-radius:10px;color:#e8f4ff;font-family:'Rajdhani',sans-serif;font-size:14px;margin-bottom:14px;outline:none;}
table{width:100%;border-collapse:collapse;font-size:13px;}
th{text-align:left;padding:10px 12px;color:#7a9cc4;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(255,255,255,0.07);}
td{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.04);vertical-align:middle;}
tr:hover td{background:rgba(0,212,255,0.04);}
.plan-badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700;}
.plan-free{background:rgba(100,150,255,0.15);color:#7faeff;}
.plan-trial{background:rgba(0,200,100,0.15);color:#00ff9d;}
.plan-starter{background:rgba(100,150,255,0.15);color:#7faeff;}
.plan-bronze{background:rgba(205,127,50,0.2);color:#cd7f32;}
.plan-silver{background:rgba(180,180,200,0.2);color:#b4b4c8;}
.plan-gold{background:rgba(255,215,0,0.2);color:#ffd700;}
.plan-platinum{background:rgba(100,220,255,0.2);color:#64dcff;}
.plan-diamond{background:rgba(185,80,255,0.2);color:#b950ff;}
.action-btn{background:rgba(0,212,255,0.1);border:1px solid rgba(0,212,255,0.3);border-radius:6px;padding:4px 10px;color:#00d4ff;font-size:11px;cursor:pointer;font-family:'Rajdhani',sans-serif;font-weight:600;margin-right:4px;}
.broadcast-area{width:100%;padding:12px;background:rgba(255,255,255,0.05);border:1px solid rgba(0,212,255,0.2);border-radius:10px;color:#e8f4ff;font-family:'Rajdhani',sans-serif;font-size:14px;resize:vertical;min-height:80px;outline:none;margin-bottom:12px;}
.broadcast-btn{padding:10px 24px;border-radius:10px;border:none;background:linear-gradient(90deg,#7b5fff,#00d4ff);color:#fff;font-family:'Orbitron',sans-serif;font-size:11px;font-weight:700;cursor:pointer;letter-spacing:1px;}
.plan-breakdown{display:flex;flex-wrap:wrap;gap:8px;}
.plan-pill{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:20px;padding:6px 14px;font-size:13px;}
.refresh-btn{background:rgba(0,255,157,0.1);border:1px solid rgba(0,255,157,0.3);border-radius:8px;padding:6px 14px;color:#00ff9d;font-size:12px;cursor:pointer;font-family:'Rajdhani',sans-serif;font-weight:600;}
.modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.8);z-index:100;align-items:center;justify-content:center;}
.modal.open{display:flex;}
.modal-box{background:#070d1f;border:1px solid rgba(0,212,255,0.2);border-radius:16px;padding:24px;width:90%;max-width:400px;}
.modal-title{font-family:'Orbitron',sans-serif;font-size:14px;font-weight:700;margin-bottom:16px;color:#00d4ff;}
.modal-btns{display:flex;gap:10px;}
.modal-btn{flex:1;padding:10px;border-radius:8px;border:none;font-family:'Orbitron',sans-serif;font-size:11px;font-weight:700;cursor:pointer;}
.modal-btn.confirm{background:linear-gradient(90deg,#00d4ff,#7b5fff);color:#fff;}
.modal-btn.cancel{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);color:#7a9cc4;}
.toast{position:fixed;top:20px;right:20px;background:rgba(0,255,157,0.15);border:1px solid rgba(0,255,157,0.4);border-radius:12px;padding:12px 20px;font-size:13px;color:#00ff9d;z-index:200;opacity:0;transition:opacity 0.3s;pointer-events:none;}
.toast.show{opacity:1;}
</style>
</head>
<body>
<div class="login-wrap" id="login-wrap">
  <div class="login-box">
    <div class="logo">AETHERMINE</div>
    <div class="logo-sub">Admin Dashboard</div>
    <input type="password" id="pwd" placeholder="Enter admin password" onkeydown="if(event.key==='Enter')login()"/>
    <button class="btn" onclick="login()">ACCESS DASHBOARD</button>
    <div class="err" id="err"></div>
  </div>
</div>

<div id="dashboard">
  <div class="header">
    <div class="header-logo">⛏️ AETHERMINE ADMIN</div>
    <div style="display:flex;gap:10px;align-items:center;">
      <button class="refresh-btn" onclick="loadDashboard()">🔄 Refresh</button>
      <button class="logout" onclick="logout()">Logout</button>
    </div>
  </div>
  <div class="stats-grid">
    <div class="stat-card"><div class="stat-num" id="s-users">-</div><div class="stat-label">Total Users</div></div>
    <div class="stat-card green"><div class="stat-num" id="s-revenue">-</div><div class="stat-label">Total Revenue</div></div>
    <div class="stat-card purple"><div class="stat-num" id="s-today">-</div><div class="stat-label">Today Revenue</div></div>
    <div class="stat-card orange"><div class="stat-num" id="s-new">-</div><div class="stat-label">New Today</div></div>
  </div>
  <div class="section">
    <div class="section-title">PLAN BREAKDOWN</div>
    <div class="plan-breakdown" id="plan-breakdown">Loading...</div>
  </div>
  <div class="section">
    <div class="section-title">📢 BROADCAST MESSAGE</div>
    <textarea class="broadcast-area" id="broadcast-msg" placeholder="Type your message to all users..."></textarea>
    <button class="broadcast-btn" onclick="sendBroadcast()">🚀 SEND TO ALL USERS</button>
  </div>
  <div class="section">
    <div class="section-title">👥 ALL USERS</div>
    <input class="search-bar" id="search" placeholder="Search by name, username or ID..." oninput="searchUsers()"/>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>User</th><th>Plan</th><th>Power</th><th>Balance</th><th>Refs</th><th>Joined</th><th>Actions</th></tr></thead>
        <tbody id="users-tbody"><tr><td colspan="7" style="text-align:center;color:#7a9cc4;padding:20px;">Loading...</td></tr></tbody>
      </table>
    </div>
  </div>
  <div class="section">
    <div class="section-title">💳 RECENT PAYMENTS</div>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>User</th><th>Plan</th><th>Amount</th><th>TX Hash</th><th>Date</th></tr></thead>
        <tbody id="payments-tbody"><tr><td colspan="5" style="text-align:center;color:#7a9cc4;padding:20px;">Loading...</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<div class="modal" id="upgrade-modal">
  <div class="modal-box">
    <div class="modal-title">⚡ CHANGE USER PLAN</div>
    <div style="font-size:13px;color:#7a9cc4;margin-bottom:14px;" id="modal-user-info"></div>
    <input type="hidden" id="modal-user-id"/>
    <select id="modal-plan">
      <option value="free">🆓 Free</option>
      <option value="trial">🌱 Trial</option>
      <option value="starter">🔷 Starter</option>
      <option value="bronze">🥉 Bronze</option>
      <option value="silver">🥈 Silver</option>
      <option value="gold">🥇 Gold</option>
      <option value="platinum">💠 Platinum</option>
      <option value="diamond">💎 Diamond</option>
    </select>
    <div class="modal-btns">
      <button class="modal-btn cancel" onclick="closeModal()">Cancel</button>
      <button class="modal-btn confirm" onclick="confirmUpgrade()">Confirm</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
let currentUpgradeUser = null;
let _tok = '';
const ah = () => ({'Content-Type':'application/json','Authorization':'Bearer '+_tok});

async function login() {
  const pwd = document.getElementById('pwd').value;
  const d = await fetch('/admin/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pwd})}).then(r=>r.json());
  if(d.success){
    _tok = d.token;
    document.getElementById('login-wrap').style.display='none';
    document.getElementById('dashboard').style.display='block';
    loadDashboard();
  } else { document.getElementById('err').textContent='❌ Wrong password!'; }
}
function logout(){
  _tok='';
  document.getElementById('login-wrap').style.display='flex';
  document.getElementById('dashboard').style.display='none';
  document.getElementById('pwd').value='';
}
async function apiGet(path){
  const r = await fetch(path,{headers:ah()});
  if(r.status===401){logout();return null;}
  return r.json();
}
async function apiPost(path,body){
  const r = await fetch(path,{method:'POST',headers:ah(),body:JSON.stringify(body)});
  if(r.status===401){logout();return null;}
  return r.json();
}
async function loadDashboard(){
  const d = await apiGet('/admin/stats');
  if(!d) return;
  document.getElementById('s-users').textContent=d.total_users;
  document.getElementById('s-revenue').textContent='$'+d.total_revenue.toFixed(2);
  document.getElementById('s-today').textContent='$'+d.today_revenue.toFixed(2);
  document.getElementById('s-new').textContent=d.new_today;
  const pe={free:'🆓',trial:'🌱',starter:'🔷',bronze:'🥉',silver:'🥈',gold:'🥇',platinum:'💠',diamond:'💎'};
  document.getElementById('plan-breakdown').innerHTML=d.plan_breakdown.map(p=>`<div class="plan-pill">${pe[p.plan]||'⚙️'} ${p.plan.toUpperCase()}: <strong>${p.count}</strong></div>`).join('');
  loadUsers();loadPayments();
}
async function loadUsers(search=''){
  const users = await apiGet('/admin/users?search='+encodeURIComponent(search));
  if(!users) return;
  document.getElementById('users-tbody').innerHTML=users.length?users.map(u=>`
    <tr>
      <td><div style="font-weight:700">${u.first_name||'?'}</div><div style="font-size:11px;color:#7a9cc4">@${u.username||'none'} · ${u.user_id}</div></td>
      <td><span class="plan-badge plan-${u.plan}">${u.plan.toUpperCase()}</span></td>
      <td>${u.power}x</td>
      <td>$${parseFloat(u.balance||0).toFixed(4)}</td>
      <td>${u.ref_count||0}</td>
      <td style="font-size:11px;color:#7a9cc4">${u.joined_at?u.joined_at.split('T')[0]:'-'}</td>
      <td><button class="action-btn" onclick="openUpgrade(${u.user_id},'${u.first_name}','${u.plan}')">⚡ Plan</button></td>
    </tr>`).join(''):'<tr><td colspan="7" style="text-align:center;color:#7a9cc4;padding:20px;">No users found</td></tr>';
}
async function loadPayments(){
  const payments = await apiGet('/admin/payments');
  if(!payments) return;
  document.getElementById('payments-tbody').innerHTML=payments.length?payments.map(p=>`
    <tr>
      <td>${p.first_name||'?'} (@${p.username||'none'})</td>
      <td><span class="plan-badge plan-${p.plan}">${(p.plan||'').toUpperCase()}</span></td>
      <td style="color:#00ff9d;font-weight:700">$${p.amount}</td>
      <td style="font-size:11px;color:#7a9cc4">${(p.tx_hash||'').substring(0,20)}...</td>
      <td style="font-size:11px;color:#7a9cc4">${p.created_at?p.created_at.split('T')[0]:'-'}</td>
    </tr>`).join(''):'<tr><td colspan="5" style="text-align:center;color:#7a9cc4;padding:20px;">No payments yet</td></tr>';
}
let searchTimeout;
function searchUsers(){clearTimeout(searchTimeout);searchTimeout=setTimeout(()=>loadUsers(document.getElementById('search').value),400);}
function openUpgrade(userId,name,currentPlan){
  document.getElementById('modal-user-id').value=userId;
  document.getElementById('modal-user-info').textContent=`User: ${name} (${userId}) · Current: ${currentPlan.toUpperCase()}`;
  document.getElementById('modal-plan').value=currentPlan;
  document.getElementById('upgrade-modal').classList.add('open');
}
function closeModal(){document.getElementById('upgrade-modal').classList.remove('open');}
async function confirmUpgrade(){
  const userId=document.getElementById('modal-user-id').value;
  const plan=document.getElementById('modal-plan').value;
  const d = await apiPost('/admin/upgrade',{user_id:userId,plan});
  if(d) { closeModal(); showToast(d.success?'✅ Plan updated!':'❌ Update failed'); loadDashboard(); }
}
async function sendBroadcast(){
  const msg=document.getElementById('broadcast-msg').value.trim();
  if(!msg){showToast('❌ Please type a message!');return;}
  if(!confirm('Send this message to ALL users?'))return;
  const d = await apiPost('/admin/broadcast',{message:msg});
  if(d) { showToast('✅ Broadcast queued for '+d.queued+' users!'); document.getElementById('broadcast-msg').value=''; }
}
function showToast(msg){
  const t=document.getElementById('toast');
  t.textContent=msg;t.classList.add('show');
  setTimeout(()=>t.classList.remove('show'),3000);
}
</script>
</body>
</html>'''

class WebHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == '/' or path == '/admin':
            self._respond(200, 'text/html', DASHBOARD_HTML.encode())
        elif path == '/health':
            self._respond(200, 'text/plain', b'AetherMine Bot is running!')
        elif path == '/admin/stats':
            if not require_admin_session(self): return
            try:
                self._respond(200, 'application/json', json.dumps(get_stats()).encode())
            except Exception as e:
                logger.exception("admin/stats error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')
        elif path == '/admin/users':
            if not require_admin_session(self): return
            search = params.get('search', [''])[0]
            try:
                users = get_all_users(search=search if search else None)
                for u in users:
                    if u.get('joined_at'): u['joined_at'] = u['joined_at'].isoformat()
                    if u.get('plan_activated_at'): u['plan_activated_at'] = u['plan_activated_at'].isoformat()
                self._respond(200, 'application/json', json.dumps(users).encode())
            except Exception as e:
                logger.exception("admin/users error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')
        elif path == '/admin/payments':
            if not require_admin_session(self): return
            try:
                payments = get_recent_payments()
                for p in payments:
                    if p.get('created_at'): p['created_at'] = p['created_at'].isoformat()
                self._respond(200, 'application/json', json.dumps(payments).encode())
            except Exception as e:
                logger.exception("admin/payments error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')
        else:
            self._respond(200, 'text/plain', b'OK')

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        path = urlparse(self.path).path

        if path == '/webhook':
            # P0-C: Verify Telegram webhook secret before processing any update
            incoming_secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming_secret != WEBHOOK_SECRET:
                logger.warning("⚠️  Webhook request with invalid secret from %s", self.address_string())
                self._respond(403, 'application/json', b'{"error":"Forbidden"}')
                return
            try:
                update_data = json.loads(body)
                if _app and _main_loop:
                    async def process():
                        update = Update.de_json(update_data, _app.bot)
                        await _app.process_update(update)
                    future = asyncio.run_coroutine_threadsafe(process(), _main_loop)
                    future.result(timeout=30)
                self._respond(200, 'application/json', b'{"ok":true}')
            except Exception as e:
                logger.error("Webhook processing error: %s", type(e).__name__)
                self._respond(200, 'application/json', b'{"ok":true}')
            return

        try:
            data = json.loads(body) if body else {}
        except:
            data = {}

        if path == '/admin/login':
            if data.get('password') == ADMIN_PASSWORD:
                token = create_session()
                self._respond(200, 'application/json', json.dumps({'success': True, 'token': token}).encode())
            else:
                # P1-C: Generic error — don't confirm the password field exists
                self._respond(200, 'application/json', json.dumps({'success': False}).encode())

        elif path == '/admin/upgrade':
            if not require_admin_session(self): return
            try:
                user_id = int(data.get('user_id'))
                plan_name = data.get('plan', '').lower()
                if plan_name not in PLANS and plan_name != 'free':
                    self._respond(400, 'application/json', b'{"error":"Invalid plan"}')
                    return
                if plan_name == 'free':
                    conn = get_db()
                    cur = conn.cursor()
                    try:
                        cur.execute("UPDATE users SET plan='free', power=0.1, daily_earn=0.05 WHERE user_id=%s", (user_id,))
                        conn.commit()
                    finally:
                        cur.close()
                        release_db(conn)
                else:
                    upgrade_user_plan(user_id, plan_name)
                # P2-C: audit every plan change
                log_admin_action(0, f"upgrade_to_{plan_name}", user_id, f"via dashboard")
                self._respond(200, 'application/json', b'{"success":true}')
            except Exception as e:
                logger.exception("admin/upgrade error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')

        elif path == '/admin/broadcast':
            if not require_admin_session(self): return
            try:
                message = data.get('message', '').strip()
                if not message:
                    self._respond(400, 'application/json', b'{"error":"Empty message"}')
                    return
                user_ids = get_all_user_ids()
                if _app and _main_loop:
                    async def send_all():
                        sent = 0
                        for uid in user_ids:
                            try:
                                await _app.bot.send_message(uid, f"📢 *AetherMine Announcement*\n\n{message}", parse_mode="Markdown")
                                sent += 1
                                await asyncio.sleep(0.05)
                            except Exception:
                                pass
                        logger.info(f"📢 Broadcast complete: {sent}/{len(user_ids)} sent")
                    asyncio.run_coroutine_threadsafe(send_all(), _main_loop)
                log_admin_action(0, "broadcast", None, f"{len(user_ids)} users: {message[:80]}")
                self._respond(200, 'application/json', json.dumps({'success': True, 'queued': len(user_ids)}).encode())
            except Exception as e:
                logger.exception("admin/broadcast error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')

        elif path == '/api/sync_balance':
            # P0-A2: Verify Telegram init data before trusting balance
            init_data = data.get('init_data', '')
            verified_uid = verify_telegram_init_data(init_data) if init_data else None
            try:
                user_id = int(data.get('user_id', 0))
                balance = float(data.get('balance', 0))
                if not user_id or balance < 0:
                    self._respond(400, 'application/json', b'{"error":"Bad request"}')
                    return
                # Reject if HMAC failed or user_id doesn't match token
                if verified_uid is None or verified_uid != user_id:
                    logger.warning("⚠️  sync_balance HMAC mismatch uid=%s verified=%s", user_id, verified_uid)
                    self._respond(403, 'application/json', b'{"error":"Forbidden"}')
                    return
                # P0-A2: Cap balance against what the plan can actually earn
                cap = get_max_withdrawable(user_id)
                if balance > cap * 1.05:  # 5% tolerance for timing
                    logger.warning("⚠️  sync_balance over cap uid=%s submitted=%.4f cap=%.4f", user_id, balance, cap)
                    self._respond(400, 'application/json', b'{"error":"Balance exceeds plan cap"}')
                    return
                sync_user_balance(user_id, balance)
                self._respond(200, 'application/json', b'{"ok":true}')
            except Exception as e:
                logger.exception("sync_balance error")
                self._respond(500, 'application/json', b'{"error":"Internal error"}')

        else:
            self._respond(200, 'application/json', b'{"ok":true}')

    def do_OPTIONS(self):
        self.send_response(200)
        self._set_cors()
        self.end_headers()

    def _set_cors(self):
        """P1-B: Only allow CORS from our own frontend, not wildcard."""
        origin = self.headers.get("Origin", "")
        allowed = ["https://aethermine.vercel.app", "https://aethermine-3.onrender.com"]
        if origin in allowed:
            self.send_header("Access-Control-Allow-Origin", origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Vary", "Origin")

    def _respond(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        # Admin endpoints: no CORS at all (browser should never call them cross-origin)
        parsed_path = urlparse(self.path).path
        if not parsed_path.startswith("/admin"):
            self._set_cors()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """P3-B: Log all requests for security visibility."""
        logger.info("HTTP %s %s [%s]", args[0] if args else "?",
                    self.path, self.address_string())

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Fix #4: Handle each HTTP request in its own thread — no more queuing."""
    daemon_threads = True

def run_server():
    server = ThreadingHTTPServer(('0.0.0.0', PORT), WebHandler)
    logger.info(f"✅ Threaded web server running on port {PORT}")
    server.serve_forever()

# ─── TRON PAYMENT WATCHER ─────────────────────────────────────────────────────
async def check_tron_payments(bot):
    logger.info("🔍 TRON payment watcher started")
    while True:
        try:
            params = {"toAddress": WALLET, "contractAddress": USDT_CONTRACT, "limit": 20, "start": 0}
            response = requests.get(TRON_API, params=params, timeout=10)
            if response.status_code != 200:
                await asyncio.sleep(30)
                continue
            data = response.json()
            transactions = data.get("token_transfers", [])
            for tx in transactions:
                tx_hash = tx.get("transaction_id", "")
                if not tx_hash or is_tx_processed(tx_hash):
                    continue
                raw_amount = int(tx.get("quant", 0))
                amount = raw_amount / 1_000_000
                amount_int = int(amount)
                logger.info(f"💰 New tx: {tx_hash} — ${amount}")
                if amount_int not in AMOUNT_TO_PLAN:
                    mark_tx_processed(tx_hash)
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(admin_id, f"⚠️ *Unmatched Payment*\n\n💰 Amount: ${amount} USDT\n🔖 TX: `{tx_hash}`\n\nHandle manually.", parse_mode="Markdown")
                        except Exception: pass
                    continue
                plan_name = AMOUNT_TO_PLAN[amount_int]
                plan = PLANS[plan_name]
                # Find matching payment request using pool connection
                conn = get_db()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    cur.execute("""
                        SELECT pr.*, u.first_name, u.username, u.referrer_id FROM payment_requests pr
                        JOIN users u ON u.user_id = pr.user_id
                        WHERE pr.amount = %s AND pr.status = 'pending'
                        AND pr.created_at > NOW() - INTERVAL '24 hours'
                        ORDER BY pr.created_at DESC LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """, (amount_int,))
                    req = cur.fetchone()
                    if req:
                        cur.execute("UPDATE payment_requests SET status='confirmed' WHERE id=%s", (req['id'],))
                        conn.commit()
                finally:
                    cur.close()
                    release_db(conn)
                if req:
                    user_id = req['user_id']
                    upgrade_user_plan(user_id, plan_name)
                    save_payment(user_id, tx_hash, amount, plan_name)
                    mark_tx_processed(tx_hash)
                    if req['referrer_id']:
                        ref_pct = 0.20 if plan_name == 'diamond' else 0.10
                        commission = amount * ref_pct
                        conn2 = get_db()
                        cur2 = conn2.cursor()
                        try:
                            cur2.execute("UPDATE users SET ref_earnings = ref_earnings + %s WHERE user_id = %s", (commission, req['referrer_id']))
                            conn2.commit()
                        finally:
                            cur2.close()
                            release_db(conn2)
                        try:
                            await bot.send_message(req['referrer_id'], f"🎉 *Referral Commission!*\n\nYour referral upgraded to *{plan['emoji']} {plan['name']}*!\n💰 You earned: *${commission:.2f} USDT*", parse_mode="Markdown")
                        except Exception: pass
                    try:
                        keyboard = [[InlineKeyboardButton("⛏️ Start Mining!", web_app=WebAppInfo(url=WEBAPP_URL))]]
                        await bot.send_message(user_id,
                            f"🎉 *Payment Confirmed! Plan Activated!*\n\n"
                            f"Your *{plan['emoji']} {plan['name']} Plan* is now active!\n\n"
                            f"⚡ Mining Power: *{plan['power']}x*\n"
                            f"📈 Daily: *${plan['daily']} USDT*\n"
                            f"🗓 Monthly: *${plan['monthly']} USDT*\n\n"
                            f"Start mining! ⛏️",
                            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
                    except Exception as e: logger.error(f"User notify error: {e}")
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(admin_id,
                                f"✅ *Auto Payment Confirmed*\n\n"
                                f"👤 {req['first_name']} (@{req['username'] or 'none'})\n"
                                f"📦 {plan['emoji']} {plan['name']} — ${amount}\n"
                                f"🔖 TX: `{tx_hash}`",
                                parse_mode="Markdown")
                        except Exception: pass
                else:
                    mark_tx_processed(tx_hash)
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(admin_id,
                                f"⚠️ *Payment — No Matching User*\n\n"
                                f"💰 ${amount} USDT\n"
                                f"📦 Matches: {plan['emoji']} {plan_name}\n"
                                f"🔖 TX: `{tx_hash}`\n\n"
                                f"Use: /activate <user_id> {plan_name}",
                                parse_mode="Markdown")
                        except Exception: pass
        except Exception as e:
            logger.error(f"Payment watcher error: {e}")
        await asyncio.sleep(30)

# ─── BOT COMMANDS ─────────────────────────────────────────────────────────────
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_rate_limited(user.id): return
    args = ctx.args
    referrer_id = int(args[0]) if args and args[0].isdigit() else None
    db_user = get_or_create_user(user.id, user.username, user.first_name, referrer_id)
    plan = db_user['plan'].upper()
    welcome = (
        f"⛏️ *Welcome to AetherMine, {user.first_name}!*\n\n"
        "Mine USDT daily by tapping and upgrading your miner.\n\n"
        "💎 *Plans from $3 — $200*\n"
        "🚀 Up to 80x mining power\n"
        "👥 Earn 10–20% referral commissions\n"
        "🏆 Compete on the global leaderboard\n\n"
        f"📊 Your current plan: *{plan}*\n\n"
        "👇 Tap the button below to start mining!"
    )
    keyboard = [
        [InlineKeyboardButton("⛏️ Open AetherMine", web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton("💎 Upgrade Plan", callback_data="plans"),
         InlineKeyboardButton("👥 Referral", callback_data="referral")],
        [InlineKeyboardButton("💰 My Balance", callback_data="balance"),
         InlineKeyboardButton("❓ Help", callback_data="help")],
    ]
    if referrer_id and referrer_id != user.id:
        welcome += f"\n\n🎁 _You were invited by a friend! Both of you earn bonuses._"
    await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def plans_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and is_rate_limited(update.effective_user.id): return
    text = "💎 *AetherMine Upgrade Plans*\n\n✅ Payments are automatic — no screenshot needed!\n\n"
    for pid, p in PLANS.items():
        text += f"{p['emoji']} *{p['name']} — ${p['price']} USDT*\n  ⚡ {p['power']}x power  |  📈 ${p['daily']}/day  |  🗓 ${p['monthly']}/month\n\n"
    keyboard = [
        [InlineKeyboardButton("🌱 Trial $3", callback_data="upgrade_trial"), InlineKeyboardButton("🔷 Starter $5", callback_data="upgrade_starter")],
        [InlineKeyboardButton("🥉 Bronze $10", callback_data="upgrade_bronze"), InlineKeyboardButton("🥈 Silver $25", callback_data="upgrade_silver")],
        [InlineKeyboardButton("🥇 Gold $50", callback_data="upgrade_gold"), InlineKeyboardButton("💠 Platinum $100", callback_data="upgrade_platinum")],
        [InlineKeyboardButton("💎 Diamond $200", callback_data="upgrade_diamond")],
    ]
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_upgrade(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = q.from_user
    plan_name = q.data.replace("upgrade_", "")
    plan = PLANS[plan_name]
    get_or_create_user(user.id, user.username, user.first_name)
    create_payment_request(user.id, plan_name, plan['price'])
    text = (
        f"{plan['emoji']} *{plan['name']} Plan — ${plan['price']} USDT*\n\n"
        f"⚡ Mining Power: *{plan['power']}x*\n"
        f"📈 Daily: *${plan['daily']} USDT*\n"
        f"🗓 Monthly: *${plan['monthly']} USDT*\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📬 *Send exactly ${plan['price']} USDT (TRC-20) to:*\n\n"
        f"`{WALLET}`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Payment auto-detected in ~1 minute\n"
        f"⚠️ Send *exactly* ${plan['price']} USDT\n"
        f"_Request expires in 24 hours_"
    )
    keyboard = [[InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_check_{plan_name}")]]
    await q.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_paid_check(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    plan_name = q.data.replace("paid_check_", "")
    plan = PLANS[plan_name]
    await q.message.reply_text(
        f"🔍 *Scanning blockchain...*\n\nLooking for your ${plan['price']} USDT payment.\nThis takes up to *1 minute*.\n\nYou'll get a confirmation message automatically! ✅",
        parse_mode="Markdown"
    )

async def balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_rate_limited(user.id): return
    db_user = get_user(user.id)
    if not db_user:
        text = "❌ Not found. Send /start first."
    else:
        p = PLANS.get(db_user['plan'], {"emoji": "🆓", "name": "Free", "power": 0.1, "daily": 0.05})
        text = (
            f"💰 *Your AetherMine Balance*\n\n"
            f"👤 {user.first_name}\n"
            f"📦 Plan: {p['emoji']} *{db_user['plan'].upper()}*\n"
            f"⚡ Power: *{db_user['power']}x*\n"
            f"📈 Daily: *${db_user['daily_earn']} USDT*\n"
            f"💵 Balance: *${float(db_user['balance'] or 0):.4f} USDT*\n"
            f"👥 Referrals: *{db_user['ref_count']}*\n"
            f"🎁 Ref Earnings: *${float(db_user['ref_earnings'] or 0):.2f} USDT*"
        )
    keyboard = [[InlineKeyboardButton("⛏️ Open AetherMine", web_app=WebAppInfo(url=WEBAPP_URL))]]
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

async def refer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_rate_limited(user.id): return
    db_user = get_user(user.id)
    link = f"https://t.me/aetherrmine_bot?start={user.id}"
    refs = db_user['ref_count'] if db_user else 0
    ref_earn = float(db_user['ref_earnings'] or 0) if db_user else 0
    text = (
        f"👥 *Your Referral Program*\n\n"
        f"🔗 Your Link:\n`{link}`\n\n"
        f"👥 Total Referrals: *{refs}*\n"
        f"💰 Total Earned: *${ref_earn:.2f} USDT*\n\n"
        f"💸 Earn *10% commission* on every upgrade!\n"
        f"💎 Diamond referrals earn *20%*!"
    )
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, parse_mode="Markdown")

async def activate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /activate <user_id> <plan>")
        return
    user_id, plan_name = int(args[0]), args[1].lower()
    if plan_name not in PLANS:
        await update.message.reply_text("❌ Invalid plan.")
        return
    plan = PLANS[plan_name]
    upgrade_user_plan(user_id, plan_name)
    log_admin_action(update.effective_user.id, f"upgrade_to_{plan_name}", user_id, "via bot command")
    try:
        keyboard = [[InlineKeyboardButton("⛏️ Start Mining!", web_app=WebAppInfo(url=WEBAPP_URL))]]
        await ctx.bot.send_message(user_id,
            f"🎉 *Plan Activated!*\n\n{plan['emoji']} *{plan['name']} Plan* is now active!\n\n"
            f"⚡ Power: *{plan['power']}x*\n📈 Daily: *${plan['daily']} USDT*",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        await update.message.reply_text(f"✅ Activated {plan_name} for user {user_id}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def users_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    stats = get_stats()
    text = (
        f"📊 *AetherMine Stats*\n\n"
        f"👥 Total Users: *{stats['total_users']}*\n"
        f"🆕 New Today: *{stats['new_today']}*\n"
        f"💰 Total Revenue: *${stats['total_revenue']:.2f} USDT*\n"
        f"📈 Today Revenue: *${stats['today_revenue']:.2f} USDT*\n\n"
        f"*Plan Breakdown:*\n"
    )
    pe = {'free':'🆓','trial':'🌱','starter':'🔷','bronze':'🥉','silver':'🥈','gold':'🥇','platinum':'💠','diamond':'💎'}
    for p in stats['plan_breakdown']:
        text += f"{pe.get(p['plan'],'⚙️')} {p['plan'].upper()}: *{p['count']}* users\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def user_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /user <user_id>")
        return
    db_user = get_user(int(ctx.args[0]))
    if not db_user:
        await update.message.reply_text("❌ User not found.")
        return
    plan = PLANS.get(db_user['plan'], {})
    text = (
        f"👤 *User Details*\n\n"
        f"ID: `{db_user['user_id']}`\n"
        f"Name: {db_user['first_name']}\n"
        f"Username: @{db_user['username'] or 'none'}\n"
        f"Plan: {plan.get('emoji','🆓')} {db_user['plan'].upper()}\n"
        f"Power: {db_user['power']}x\n"
        f"Daily: ${db_user['daily_earn']}\n"
        f"Balance: ${float(db_user['balance'] or 0):.4f}\n"
        f"Referrals: {db_user['ref_count']}\n"
        f"Ref Earnings: ${float(db_user['ref_earnings'] or 0):.2f}\n"
        f"Joined: {str(db_user['joined_at'])[:10]}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    message = ' '.join(ctx.args)
    user_ids = get_all_user_ids()
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await ctx.bot.send_message(uid, f"📢 *AetherMine Announcement*\n\n{message}", parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.1)
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Broadcast complete!\n📤 Sent: {sent}\n❌ Failed: {failed}")

async def topusers_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT first_name, username, plan, power, balance FROM users ORDER BY power DESC LIMIT 10")
        users = cur.fetchall()
    finally:
        cur.close()
        release_db(conn)
    pe = {'free':'🆓','trial':'🌱','starter':'🔷','bronze':'🥉','silver':'🥈','gold':'🥇','platinum':'💠','diamond':'💎'}
    text = "🏆 *Top 10 Miners*\n\n"
    for i, u in enumerate(users, 1):
        text += f"{i}. {u['first_name']} — {pe.get(u['plan'],'⚙️')} {u['plan'].upper()} — {u['power']}x\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def payments_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    payments = get_recent_payments(10)
    if not payments:
        await update.message.reply_text("No payments yet.")
        return
    pe = {'trial':'🌱','starter':'🔷','bronze':'🥉','silver':'🥈','gold':'🥇','platinum':'💠','diamond':'💎'}
    text = "💳 *Recent Payments*\n\n"
    for p in payments:
        text += f"{pe.get(p['plan'],'💰')} {p['first_name']} — ${p['amount']} — {str(p['created_at'])[:10]}\n"
    await update.message.reply_text(text, parse_mode="Markdown")

async def downgrade_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /downgrade <user_id>")
        return
    user_id = int(ctx.args[0])
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET plan='free', power=0.1, daily_earn=0.05 WHERE user_id=%s", (user_id,))
        conn.commit()
    finally:
        cur.close()
        release_db(conn)
    log_admin_action(update.effective_user.id, "downgrade_to_free", user_id, "via bot command")
    await update.message.reply_text(f"✅ User {user_id} downgraded to free plan.")

async def admin_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    text = (
        "🛠 *Admin Commands*\n\n"
        "*/users* — Stats & plan breakdown\n"
        "*/user <id>* — View user details\n"
        "*/activate <id> <plan>* — Upgrade user plan\n"
        "*/downgrade <id>* — Reset user to free\n"
        "*/topusers* — Top 10 miners\n"
        "*/payments* — Recent payments\n"
        "*/withdrawals* — Pending withdrawal requests\n"
        "*/markpaid <id>* — Mark withdrawal as paid\n"
        "*/broadcast <msg>* — Message all users\n\n"
        "🌐 *Web Dashboard:*\n"
        f"`https://aethermine-3.onrender.com`\n"
        "⚠️ _Dashboard password is stored securely — never share it in chat._"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ── NEW: handle withdrawal requests sent from the in-app wallet ───────────────
async def handle_web_app_data(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw = update.message.web_app_data.data
    try:
        data = json.loads(raw)
    except Exception:
        return

    action = data.get('action')
    user = update.effective_user

    if action == 'withdraw':
        # Check plan eligibility first — only Silver ($25) and above can withdraw
        db_user = get_user(user.id)
        user_plan = db_user['plan'] if db_user else 'free'
        if user_plan not in WITHDRAWAL_ELIGIBLE_PLANS:
            await update.message.reply_text(
                f"🔒 *Withdrawals Locked*\n\n"
                f"Withdrawals are available on *Silver plan ($25) and above*.\n\n"
                f"📦 Your current plan: *{user_plan.upper()}*\n\n"
                f"Upgrade to Silver or higher to unlock withdrawals! 💎",
                parse_mode="Markdown"
            )
            return

        amount  = float(data.get('amount', 0))
        receive = float(data.get('receive', 0))
        address = data.get('address', '').strip()
        fee     = round(amount - receive, 4)

        # P3-A: Strict TRC-20 address validation (exactly 34 chars, base58 charset)
        trc20_re = re.compile(r'^T[1-9A-HJ-NP-Za-km-z]{33}$')
        if not trc20_re.match(address):
            await update.message.reply_text("❌ Invalid TRC-20 address. Must be 34 characters starting with T.")
            return

        if amount < 5:
            await update.message.reply_text("❌ Minimum withdrawal is $5 USDT.")
            return

        # P2-B: Check 24-hour cooldown — one withdrawal request per day max
        if has_pending_withdrawal(user.id):
            await update.message.reply_text("⏳ You already have a withdrawal request in the last 24 hours. Please wait.")
            return

        # P2-B: Cap withdrawal at the user's DB balance (server-side, not frontend)
        max_allowed = get_max_withdrawable(user.id)
        if amount > max_allowed + 0.001:
            await update.message.reply_text(f"❌ Withdrawal exceeds your available balance (${max_allowed:.4f} USDT).")
            return

        # Persist to DB and zero out balance
        save_withdrawal(user.id, amount, fee, receive, address)
        sync_user_balance(user.id, 0.0)  # P2-B: clear balance after withdrawal request

        # Notify admins
        for admin_id in ADMIN_IDS:
            try:
                await ctx.bot.send_message(
                    admin_id,
                    f"💸 *New Withdrawal Request*\n\n"
                    f"👤 {user.first_name} (@{user.username or 'none'}) — `{user.id}`\n"
                    f"💰 Requested: *${amount:.2f} USDT*\n"
                    f"📬 After 5% fee: *${receive:.4f} USDT*\n"
                    f"🏦 Address: `{address}`\n\n"
                    f"Use /withdrawals to see all pending.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

        # Confirm to user
        await update.message.reply_text(
            f"✅ *Withdrawal Request Received!*\n\n"
            f"💰 Amount: *${amount:.2f} USDT*\n"
            f"📬 You'll receive: *${receive:.4f} USDT*\n"
            f"🏦 To: `{address}`\n\n"
            f"⏳ Processing within 24–48 hours.",
            parse_mode="Markdown"
        )


# ── NEW: admin command — list pending withdrawals ─────────────────────────────
async def withdrawals_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    pending = get_pending_withdrawals()
    if not pending:
        await update.message.reply_text("✅ No pending withdrawals.")
        return
    text = f"💸 *Pending Withdrawals ({len(pending)})*\n\n"
    for w in pending:
        text += (
            f"🆔 ID: `{w['id']}`\n"
            f"👤 {w['first_name']} (@{w['username'] or 'none'})\n"
            f"💰 ${w['amount']:.2f} → Receives: ${w['receive']:.4f} USDT\n"
            f"🏦 `{w['address']}`\n"
            f"📅 {str(w['requested_at'])[:10]}\n"
            f"➡️ /markpaid {w['id']}\n\n"
        )
    await update.message.reply_text(text, parse_mode="Markdown")


# ── NEW: admin command — mark a withdrawal as paid ────────────────────────────
async def markpaid_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return
    if not ctx.args:
        await update.message.reply_text("Usage: /markpaid <withdrawal_id>")
        return
    try:
        wid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid ID.")
        return

    row = mark_withdrawal_paid(wid)
    if row:
        try:
            await ctx.bot.send_message(
                row['user_id'],
                f"✅ *Withdrawal Processed!*\n\n"
                f"💰 *${row['receive']:.4f} USDT* has been sent to:\n`{row['address']}`\n\n"
                f"Thank you for mining with AetherMine! ⛏️",
                parse_mode="Markdown"
            )
        except Exception:
            pass
        log_admin_action(update.effective_user.id, "withdrawal_paid", row['user_id'], f"withdrawal_id={wid} amount={row['receive']}")
        await update.message.reply_text(f"✅ Withdrawal #{wid} marked as paid and user notified.")
    else:
        await update.message.reply_text("❌ Withdrawal ID not found.")

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Comprehensive FAQ/Help system"""
    text = (
        "📚 *AetherMine Help Center*\n\n"
        "Select a topic to learn more:\n\n"
        "🚀 Getting Started\n"
        "💰 How Auto-Mining Works\n"
        "💎 Upgrading Plans\n"
        "💸 Withdrawals\n"
        "👥 Referral System\n"
        "❓ Common Questions\n\n"
        "Or use quick commands:\n"
        "*/start* — Launch the mining app\n"
        "*/plans* — View upgrade options\n"
        "*/balance* — Check your balance\n"
        "*/refer* — Get referral link\n\n"
        f"📬 *Payment Wallet:*\n`{WALLET}`"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Getting Started", callback_data="help_start")],
        [InlineKeyboardButton("💰 How Auto-Mining Works", callback_data="help_mining")],
        [InlineKeyboardButton("💎 Upgrading Plans", callback_data="help_upgrade")],
        [InlineKeyboardButton("💸 Withdrawals", callback_data="help_withdraw")],
        [InlineKeyboardButton("👥 Referral System", callback_data="help_referral")],
        [InlineKeyboardButton("❓ Common Questions", callback_data="help_faq")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)

async def help_sections(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle help section callbacks"""
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "help_start":
        text = (
            "🚀 *GETTING STARTED*\n\n"
            "*What is AetherMine?*\n"
            "An automated USDT earning platform. Your miner works 24/7 to generate passive income!\n\n"
            "*How to start:*\n"
            "1️⃣ Open @aetherrmine_bot\n"
            "2️⃣ Send /start\n"
            "3️⃣ Your free miner starts earning $0.05/day automatically!\n"
            "4️⃣ Upgrade for higher earnings\n\n"
            "*No tapping, no manual work. 100% automatic!*"
        )
    elif data == "help_mining":
        text = (
            "💰 *HOW AUTO-MINING WORKS*\n\n"
            "*Do I need to tap anything?*\n"
            "❌ NO! 100% automatic once activated.\n\n"
            "*How it works:*\n"
            "Your miner generates USDT based on your plan's daily rate:\n"
            "• Free: $0.05/day\n"
            "• Trial: $0.25/day\n"
            "• Starter: $0.50/day\n"
            "• Bronze: $1.20/day\n"
            "• Silver: $3/day\n"
            "• Gold: $6.50/day\n"
            "• Platinum: $14/day\n"
            "• Diamond: $30/day\n\n"
            "*Your balance grows every second!*\n\n"
            "*Do I keep the app open?*\n"
            "❌ NO! Works even when:\n"
            "✅ App is closed\n"
            "✅ Phone is off\n"
            "✅ You're offline\n\n"
            "When you return, all earnings are added automatically!\n\n"
            "*What's the green glow?*\n"
            "Visual indicator your miner is actively working right now!\n\n"
            "*What's the progress bar?*\n"
            "Shows how much you've earned toward your next daily milestone.\n\n"
            "Example (Silver - $3/day):\n"
            "• 6 hours = 25% ($0.75)\n"
            "• 12 hours = 50% ($1.50)\n"
            "• 24 hours = 100% ($3.00) → resets"
        )
    elif data == "help_upgrade":
        text = (
            "💎 *UPGRADING PLANS*\n\n"
            "*How to upgrade:*\n"
            "1️⃣ Go to PLANS tab\n"
            "2️⃣ Click SELECT PLAN\n"
            "3️⃣ Send exact USDT (TRC-20) to our wallet\n"
            "4️⃣ Plan activates in 1-2 minutes!\n\n"
            "*Payment method:*\n"
            "USDT (TRC-20) on TRON network only\n\n"
            f"*Payment Wallet:*\n`{WALLET}`\n\n"
            "*How payments work:*\n"
            "✅ 100% automatic detection\n"
            "✅ No screenshots needed\n"
            "✅ No confirmation messages\n"
            "✅ Just send exact amount!\n\n"
            "*Why EXACT amount?*\n"
            "Our system matches amounts to plans:\n"
            "$3 = Trial | $5 = Starter\n"
            "$10 = Bronze | $25 = Silver\n"
            "$50 = Gold | $100 = Platinum\n"
            "$200 = Diamond\n\n"
            "*What happens to my balance?*\n"
            "✅ Never lost!\n"
            "✅ Existing balance stays\n"
            "✅ Daily rate increases\n"
            "✅ Start earning more immediately!"
        )
    elif data == "help_withdraw":
        text = (
            "💸 *WITHDRAWALS*\n\n"
            "*Requirements:*\n"
            "✅ At least $5 USDT balance\n"
            "✅ Silver plan ($25) or higher\n"
            "✅ Valid TRC-20 wallet address\n\n"
            "*How to withdraw:*\n"
            "1️⃣ Go to WALLET tab\n"
            "2️⃣ Enter amount (min $5)\n"
            "3️⃣ Enter TRC-20 address\n"
            "4️⃣ Click REQUEST WITHDRAWAL\n"
            "5️⃣ We process in 24-48 hours\n\n"
            "*Fees:*\n"
            "5% per withdrawal\n\n"
            "Example:\n"
            "Withdraw $100 → Fee $5 → Receive $95\n\n"
            "*Why Silver requirement?*\n"
            "• Ensures serious users\n"
            "• Prevents abuse\n"
            "• Platform sustainability\n\n"
            "*Can I withdraw referral earnings?*\n"
            "✅ YES! Combined with your balance."
        )
    elif data == "help_referral":
        text = (
            "👥 *REFERRAL SYSTEM*\n\n"
            "*How it works:*\n"
            "1️⃣ Share your unique link\n"
            "2️⃣ Someone joins & upgrades\n"
            "3️⃣ You earn commission!\n\n"
            "*Commission rates:*\n"
            "• Most plans: 10%\n"
            "• Diamond holders: 20%\n\n"
            "*Example:*\n"
            "Your referral buys Silver ($25)\n"
            "You earn: $2.50 (or $5 with Diamond)\n"
            "Added to balance instantly!\n\n"
            "*Where's my link?*\n"
            "Go to REFER tab or use /refer\n\n"
            "*How to get 20%?*\n"
            "Upgrade to Diamond plan ($200)\n\n"
            "*Any limits?*\n"
            "❌ NO! Unlimited referrals!\n\n"
            "*Best tips:*\n"
            "• Share in Telegram groups\n"
            "• Post on social media\n"
            "• Explain it's passive income\n"
            "• Show your earnings proof!"
        )
    elif data == "help_faq":
        text = (
            "❓ *COMMON QUESTIONS*\n\n"
            "*Q: Is this real crypto mining?*\n"
            "A: No, it's a USDT rewards platform. Earn based on your plan's daily rate.\n\n"
            "*Q: Is my data safe?*\n"
            "A: Yes! Secure encryption, HTTPS, regular audits.\n\n"
            "*Q: Can I have multiple accounts?*\n"
            "A: No. One per person. Multiple = ban.\n\n"
            "*Q: What if payment doesn't activate?*\n"
            "A: Contact support with transaction hash. We'll activate manually.\n\n"
            "*Q: Can I cancel my plan?*\n"
            "A: No refunds. Plans run forever once activated.\n\n"
            "*Q: Do plans expire?*\n"
            "A: Never! They run automatically forever.\n\n"
            "*Q: What if bot stops working?*\n"
            "A: Your balance is safe in our database. Earnings continue accumulating.\n\n"
            "*Q: How to maximize earnings?*\n"
            "• Upgrade early\n"
            "• Refer aggressively\n"
            "• Upgrade to Diamond\n"
            "• Reinvest earnings\n\n"
            "*Need more help?*\n"
            "Use /help anytime!"
        )
    else:
        text = "Unknown help topic."
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Help Menu", callback_data="help_back")]]
    markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)

async def help_back(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Return to main help menu"""
    query = update.callback_query
    await query.answer()
    text = (
        "📚 *AetherMine Help Center*\n\n"
        "Select a topic to learn more:\n\n"
        "🚀 Getting Started\n"
        "💰 How Auto-Mining Works\n"
        "💎 Upgrading Plans\n"
        "💸 Withdrawals\n"
        "👥 Referral System\n"
        "❓ Common Questions\n\n"
        "Or use quick commands:\n"
        "*/start* — Launch the mining app\n"
        "*/plans* — View upgrade options\n"
        "*/balance* — Check your balance\n"
        "*/refer* — Get referral link\n\n"
        f"📬 *Payment Wallet:*\n`{WALLET}`"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Getting Started", callback_data="help_start")],
        [InlineKeyboardButton("💰 How Auto-Mining Works", callback_data="help_mining")],
        [InlineKeyboardButton("💎 Upgrading Plans", callback_data="help_upgrade")],
        [InlineKeyboardButton("💸 Withdrawals", callback_data="help_withdraw")],
        [InlineKeyboardButton("👥 Referral System", callback_data="help_referral")],
        [InlineKeyboardButton("❓ Common Questions", callback_data="help_faq")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)

async def button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "plans": await plans_cmd(update, ctx)
    elif q.data == "balance": await balance(update, ctx)
    elif q.data == "referral": await refer(update, ctx)
    elif q.data == "help": await help_cmd(update, ctx)
    elif q.data in ["help_start", "help_mining", "help_upgrade", "help_withdraw", "help_referral", "help_faq"]:
        await help_sections(update, ctx)
    elif q.data == "help_back": await help_back(update, ctx)
    elif q.data.startswith("upgrade_"): await handle_upgrade(update, ctx)
    elif q.data.startswith("paid_check_"): await handle_paid_check(update, ctx)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def setup_webhook(app):
    global _main_loop
    _main_loop = asyncio.get_event_loop()
    # P0-C: Register webhook secret so Telegram signs every update
    await app.bot.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
    logger.info(f"✅ Webhook set to {WEBHOOK_URL} (secret token registered)")
    asyncio.create_task(check_tron_payments(app.bot))

def main():
    global _app, _main_loop
    init_pool()   # Fix #1: start connection pool before anything else
    init_db()

    Thread(target=run_server, daemon=True).start()
    logger.info("✅ Web server started")

    _app = Application.builder().token(BOT_TOKEN).build()

    # User commands
    _app.add_handler(CommandHandler("start", start))
    _app.add_handler(CommandHandler("plans", plans_cmd))
    _app.add_handler(CommandHandler("balance", balance))
    _app.add_handler(CommandHandler("refer", refer))
    _app.add_handler(CommandHandler("help", help_cmd))

    # Admin commands
    _app.add_handler(CommandHandler("users", users_cmd))
    _app.add_handler(CommandHandler("user", user_cmd))
    _app.add_handler(CommandHandler("activate", activate))
    _app.add_handler(CommandHandler("downgrade", downgrade_cmd))
    _app.add_handler(CommandHandler("topusers", topusers_cmd))
    _app.add_handler(CommandHandler("payments", payments_cmd))
    _app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    _app.add_handler(CommandHandler("adminhelp", admin_help))
    # NEW: wallet withdrawal admin commands
    _app.add_handler(CommandHandler("withdrawals", withdrawals_cmd))
    _app.add_handler(CommandHandler("markpaid", markpaid_cmd))
    _app.add_handler(CallbackQueryHandler(button))
    # NEW: handle tg.sendData() calls from the web app wallet
    _app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_web_app_data))

    _app.post_init = setup_webhook

    logger.info("🚀 AetherMine bot starting in webhook mode!")
    # P0-E: initialize + start + idle — no polling, no second HTTP server.
    # Our ThreadingHTTPServer handles /webhook and feeds updates via run_coroutine_threadsafe.
    import asyncio as _asyncio

    async def _run():
        global _main_loop
        _main_loop = _asyncio.get_event_loop()
        async with _app:
            await _app.initialize()
            await _app.start()
            await setup_webhook(_app)
            logger.info("✅ Bot running. Waiting for updates via webhook.")
            # Keep the loop alive
            stop_event = _asyncio.Event()
            await stop_event.wait()

    _asyncio.run(_run())

if __name__ == "__main__":
    main()
