# ⛏️ AetherMine

A Telegram Web App mining bot where users earn USDT daily by tapping and upgrading mining plans. Payments are detected automatically on the TRON blockchain.

---

## 📁 Project Structure

```
aethermine/
├── bot.py              # Main bot — Telegram + Web server + Admin dashboard
├── index.html          # Telegram Web App frontend (hosted on Vercel)
├── requirements.txt    # Python dependencies
├── Procfile            # Render start command
├── render.yaml         # Render infrastructure config
├── runtime.txt         # Python version pin
├── .env.example        # Environment variable template (safe to commit)
├── .env                # Your real secrets — NEVER commit this
└── .gitignore          # Blocks .env and junk from git
```

---

## 🚀 Local Setup

### 1. Clone and install dependencies
```bash
git clone https://github.com/yourname/aethermine.git
cd aethermine
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Create your `.env` file
```bash
cp .env.example .env
# Then open .env and fill in your real values
```

### 3. Run locally
```bash
python bot.py
```

> **Note:** Webhooks won't work locally without a public URL. For local testing, temporarily switch to polling by replacing the `_run()` block in `main()` with `_app.run_polling()`. Remember to revert before deploying.

---

## ☁️ Deploying to Render

### First deploy
1. Push the repo to GitHub
2. Go to [render.com](https://render.com) → New → Web Service → connect your repo
3. Render auto-detects `Procfile` and `requirements.txt`
4. Go to **Environment** and add:

| Key | Value |
|-----|-------|
| `BOT_TOKEN` | From BotFather |
| `DATABASE_URL` | From your Render Postgres service |
| `ADMIN_PASSWORD` | Your chosen admin password |
| `WEBHOOK_SECRET` | Any long random string |

5. Click **Deploy**

### Subsequent deploys
```bash
git push origin main   # Render auto-deploys on push
```

---

## 🌐 Frontend (index.html)

The frontend is a single HTML file deployed to **Vercel** as a static site.

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

After deploying, update `WEBAPP_URL` in `bot.py` to your Vercel URL, then redeploy the bot.

---

## 💰 Mining Plans

| Plan | Cost | Power | Daily |
|------|------|-------|-------|
| Free | $0 | 0.1x | $0.05 |
| Trial | $3 | 0.5x | $0.25 |
| Starter | $5 | 1x | $0.50 |
| Bronze | $10 | 2.5x | $1.20 |
| Silver | $25 | 7x | $3.00 |
| Gold | $50 | 16x | $6.50 |
| Platinum | $100 | 35x | $14.00 |
| Diamond | $200 | 80x | $30.00 |

> Withdrawals are available on **Silver ($25) and above** only.

---

## 🛠️ Admin Commands (Telegram)

| Command | Description |
|---------|-------------|
| `/users` | Stats and plan breakdown |
| `/user <id>` | View a specific user |
| `/activate <id> <plan>` | Manually upgrade a user |
| `/downgrade <id>` | Reset user to free plan |
| `/topusers` | Top 10 miners by power |
| `/payments` | Recent confirmed payments |
| `/withdrawals` | Pending withdrawal requests |
| `/markpaid <id>` | Mark withdrawal as paid |
| `/broadcast <msg>` | Message all users |
| `/adminhelp` | Show this list |

---

## 🔒 Security Features

- ✅ Server-side admin session tokens (8hr TTL, Bearer auth)
- ✅ Telegram webhook secret verification (X-Telegram-Bot-Api-Secret-Token)
- ✅ Telegram init-data HMAC verification on balance sync
- ✅ All secrets via environment variables — no hardcoded credentials
- ✅ PostgreSQL connection pooling (min 2, max 20 connections)
- ✅ 9 DB indexes for scale (1k–5k users)
- ✅ FOR UPDATE SKIP LOCKED on payment matching (race condition safe)
- ✅ Rate limiting on all user commands (2s cooldown, TTL eviction)
- ✅ Withdrawal: Silver+ plan gate, 24hr cooldown, balance cap, TRC-20 regex
- ✅ Non-blocking broadcast via asyncio
- ✅ ThreadingHTTPServer (each request in its own thread)
- ✅ Admin audit log on all plan changes and withdrawals
- ✅ Generic error responses (no stack traces to clients)
- ✅ CORS restricted to own domains only

---

## 🗄️ Database Tables

| Table | Purpose |
|-------|---------|
| `users` | User accounts, plans, balances |
| `payments` | Confirmed TRON payments |
| `processed_tx` | Deduplicate processed transactions |
| `payment_requests` | Pending plan upgrade requests |
| `withdrawals` | Withdrawal requests and status |
| `admin_log` | Audit trail for all admin actions |

---

## 🔄 Migrating to a New Host

1. Dump the database: `pg_dump $DATABASE_URL > backup.sql`
2. Restore to new DB: `psql $NEW_DATABASE_URL < backup.sql`
3. Update `DATABASE_URL` env var on the new host
4. Update `WEBHOOK_URL` in `bot.py` to the new domain
5. Push and deploy — the bot auto-registers the new webhook on startup

---

## 📬 Payment Flow

```
User selects plan
    → bot creates payment_request row
    → user sends exact USDT to TRC-20 wallet
    → TRON watcher polls every 30s
    → matches tx by amount → FOR UPDATE SKIP LOCKED
    → upgrades user plan, saves payment, notifies user + admin
```

---

## ⚙️ Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | ✅ | Telegram bot token from BotFather |
| `DATABASE_URL` | ✅ | PostgreSQL connection string |
| `ADMIN_PASSWORD` | ✅ | Admin dashboard password |
| `WEBHOOK_SECRET` | ⚠️ | Auto-generated if not set (set it manually for consistency across restarts) |
