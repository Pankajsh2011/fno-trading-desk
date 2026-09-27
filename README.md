# 🚀 F&O Algorithmic Trading Desk & Cyberpunk Cockpit (100% Python)

A high-performance, **100% Python-based** algorithmic trading platform and live interactive dashboard designed for the **Indian National Stock Exchange (NSE)** F&O market (197 equities) with real-time multi-threaded scanning, strict candlestick engulfing validation, multi-agent telemetry, and live Render cloud deployment.

---

## 🌟 Key Architecture & Features

### 1. 100% Pure Python Stack (Zero Node.js / NPM)
* **Backend & API:** Python Flask + Gunicorn multi-worker server.
* **Frontend Cockpit:** Self-contained animated Cyberpunk Glassmorphism UI (`GPTHEIST DESK`) inspired by high-frequency proprietary trading terminals.
* **Instant Cloud Deployment:** Native support for **Render.com**, Heroku, Railway, and VPS through `render.yaml` and `Procfile`.

### 2. High-Speed Multi-Threaded F&O Scanner
* Scans all **197 NSE F&O Stocks** concurrently using `ThreadPoolExecutor` (14 worker threads) in ~25–30 seconds.
* Dual data ingestion:
  * **Dhan HQ v2 Market API** (Direct live broker ticks).
  * **Real-time NSE fallback** (Zero disruption if Dhan token expires).

### 3. Strict Candlestick Engulfing Pattern Engine
Strictly validates patterns according to Photos 1–4 criteria:
* **Bullish Engulfing:** Must occur after a confirmed decline/downtrend. Green candle body completely engulfs previous red candle body ($\ge 120\%$).
* **Bearish Engulfing:** Must occur after a confirmed advance/uptrend. Red candle body completely engulfs previous green candle body ($\ge 120\%$).
* **Wick Discipline:** Upper and lower shadow wicks restricted to $\le 40\%$ of body size.
* **Risk:Reward Ratio:** Automatic Entry, Stop Loss, and minimum 1:2 R:R Target calculation.

### 4. 10 Operator Agent Command Deck
Live telemetry and status monitoring for 10 autonomous agents:
* 🧑‍🚀 **TOKYO** — Scout Engine (197 F&O scrips master).
* 🛡️ **PALERMO** — Approval Gate & Risk Manager (1.0% capital risk limit).
* ⚡ **DENVER** — Execution Striker (Rapid order execution).
* 🌊 **STOCKHOLM** — Liquidity Sentinel (Volume surges & order book depth).
* 🧠 **PROFESSOR** — Strategy Engine & Engulfing pattern brain.
* 📊 **RIO** — Rolling Candle Builder (25-min rolling bars).
* 🧱 **HELSINKI** — Defense Shield & Drawdown lock.
* 📡 **NAIROBI** — Signal Dispatcher (Telegram instant alerts).
* ⚔️ **BERLIN** — Volatility Sweeper (India VIX & ATR bands).
* 🎯 **LISBON** — Exit Coordinator (Trailing stops & profit harvest).

### 5. Advanced Cockpit Visualizations
* **Balance History / Equity Curve:** Real-time intraday equity curve canvas.
* **Tail Probability Ridge / Strike Landscape:** Multi-layer statistical distribution canvas.
* **5D Strategy Lattice (Penteract):** 3D-projected rotating 32-vertex hypercube strategy graph.
* **Relationship Graph & Signal Flow Pipeline:** 90-node, 134-edge liquidity flow network displaying $P(\text{UP})$ vs $P(\text{DOWN})$ swing probabilities and cluster convergence.
* **Live Audio Alert Synthesizer:** Real-time Web Audio API sound alerts for Buy/Sell signals.
* **1-Click Dhan Access Token Modal:** Update 24-hr broker access tokens directly from the top bar without server restarts.

---

## 💻 Local Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/fno-trading-desk.git
cd fno-trading-desk
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Credentials (Optional for Paper Trading)
Copy `.env.example` to `.env`:
```bash
copy .env.example .env
```
Fill in your Dhan HQ credentials, Telegram bot token, or OpenAlgo API key if desired. (The bot will run in full real-time paper trading mode even without API keys).

### 4. Start the Application
```bash
python main.py
```
Open your browser at: **`http://127.0.0.1:3001`**

---

## ☁️ Live Cloud Deployment on Render.com

Deploying this 100% Python bot to **Render** takes less than 2 minutes:

1. **Sign in to Render:** Go to [https://render.com](https://render.com) and log in with your GitHub account.
2. **Create New Web Service:**
   * Click **New +** at the top right and select **Web Service**.
   * Choose **Build and deploy from a Git repository**.
   * Connect your new GitHub repository: `YOUR_USERNAME/fno-trading-desk`.
3. **Configure Service Settings:**
   * **Name:** `fno-trading-desk`
   * **Language / Runtime:** `Python 3`
   * **Region:** `Singapore` (closest to NSE India for lowest latency)
   * **Branch:** `main`
   * **Build Command:** `pip install -r requirements.txt`
   * **Start Command:** `gunicorn backend.server:app --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120`
4. **Environment Variables (Optional):**
   * Under the **Environment Variables** tab, you can add `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, etc.
5. **Deploy:** Click **Deploy Web Service**! Render will build and launch your live trading cockpit with a public `https://...onrender.com` URL.

---

## 📁 Project Structure

```
fno-trading-desk/
├── backend/
│   ├── templates/
│   │   └── index.html        # Animated Cyberpunk Cockpit UI
│   ├── fno_stocks_master.csv # Master list of 197 NSE F&O Equities
│   ├── server.py             # Flask Core, Scanning Daemon, Alert Engines
│   └── requirements.txt      # Backend dependencies mirror
├── .env.example              # Configuration template
├── .gitignore                # Protects keys, tokens, and databases
├── main.py                   # Main entry point for local & cloud runs
├── Procfile                  # Gunicorn configuration for Render/Heroku
├── render.yaml               # 1-Click Render Blueprint configuration
├── runtime.txt               # Declares Python 3.11.9 runtime
├── requirements.txt          # Python package requirements
├── run_bot.bat               # 1-Click Windows execution script
└── README.md                 # Complete documentation
```

---

## 📜 License & Disclaimer
This software is intended for educational and algorithmic research purposes. Trading derivatives and equities in the Indian Stock Market carries financial risk. Always test thoroughly in paper trading mode before committing capital.
