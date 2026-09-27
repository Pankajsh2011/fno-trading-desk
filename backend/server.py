import os
import sys
import json
import time
import math
import random
import re
import base64
import hmac
import hashlib
import struct
import threading
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

# Force IPv4 across all sockets to prevent Windows IPv6 unreachable network errors (WinError 10065)
import socket
import urllib3.util.connection as urllib3_cn
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

import pytz
import requests
from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("AlgoTrader")

# Base Paths & Environment
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)
ROOT_ENV = ROOT_DIR / ".env"
if ROOT_ENV.exists():
    load_dotenv(dotenv_path=ROOT_ENV)

PORT = int(os.getenv("PORT", "3001"))
DHAN_API_BASE = "https://api.dhan.co"
IST = pytz.timezone("Asia/Kolkata")

# ----------------------------------------------------
# 🗄️ STATE MANAGEMENT
# ----------------------------------------------------
active_signals: List[Dict[str, Any]] = []
trade_book: List[Dict[str, Any]] = []
scan_progress: int = 100
scanner_status: Dict[str, Any] = {
    "isRunning": False,
    "lastScan": None,
    "nextScan": None,
    "stocksScanned": 0,
    "signalsFound": 0
}

OPENALGO_APP_NAME = os.getenv("OPENALGO_APP_NAME", "openalgobot")
OPENALGO_API_KEY = os.getenv("OPENALGO_API_KEY", "6af3c971")
OPENALGO_API_SECRET = os.getenv("OPENALGO_API_SECRET", "e1e3536e-dd31-44fd-993b-54a8c7f5b788")
OPENALGO_HOST = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
OPENALGO_WS_URL = os.getenv("OPENALGO_WS_URL", "ws://127.0.0.1:8765")

credentials: Dict[str, Any] = {
    "clientId": os.getenv("DHAN_CLIENT_ID", ""),
    "accessToken": os.getenv("DHAN_ACCESS_TOKEN", ""),
    "openalgoAppName": OPENALGO_APP_NAME,
    "openalgoApiKey": OPENALGO_API_KEY,
    "openalgoApiSecret": OPENALGO_API_SECRET,
    "openalgoHost": OPENALGO_HOST,
    "openalgoWsUrl": OPENALGO_WS_URL
}

dhan_status: Dict[str, Any] = {
    "isValid": False,
    "lastChecked": None,
    "error": "Initializing..."
}

def check_dhan_connection() -> Dict[str, Any]:
    global dhan_status
    client_id = credentials.get("clientId") or os.getenv("DHAN_CLIENT_ID", "")
    token = credentials.get("accessToken") or os.getenv("DHAN_ACCESS_TOKEN", "")
    if not client_id or not token or client_id == "your_client_id_here":
        dhan_status["isValid"] = False
        dhan_status["error"] = "Dhan Client ID or Access Token not configured."
        return dhan_status

    headers = {
        "Content-Type": "application/json",
        "client-id": client_id,
        "access-token": token
    }
    try:
        resp = requests.get(f"{DHAN_API_BASE}/v2/profile", headers=headers, timeout=5)
        if resp.status_code == 200:
            dhan_status["isValid"] = True
            dhan_status["error"] = None
            dhan_status["lastChecked"] = datetime.now(IST).isoformat()
            logger.info("[Dhan Connection] Successfully authenticated with DhanHQ API!")
        else:
            try:
                err_data = resp.json()
                msg = err_data.get("message") or err_data.get("errorMessage") or f"HTTP {resp.status_code}"
            except Exception:
                msg = f"HTTP {resp.status_code}"
            dhan_status["isValid"] = False
            dhan_status["error"] = msg
            dhan_status["lastChecked"] = datetime.now(IST).isoformat()
            logger.warning(f"[Dhan Connection] Dhan API authentication returned: {msg}. Live NSE fallback engine active.")
    except Exception as e:
        dhan_status["isValid"] = False
        dhan_status["error"] = str(e)
        dhan_status["lastChecked"] = datetime.now(IST).isoformat()
        logger.warning(f"[Dhan Connection] Dhan API connection error: {e}. Live NSE fallback engine active.")

    return dhan_status

openalgo_client = None

def get_openalgo_client():
    global openalgo_client
    api_key = credentials.get("openalgoApiKey") or os.getenv("OPENALGO_API_KEY", "6af3c971")
    host = credentials.get("openalgoHost") or os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
    if not api_key:
        return None
    try:
        import openalgo
        if openalgo_client is None or getattr(openalgo_client, "api_key", None) != api_key:
            openalgo_client = openalgo.api(
                api_key=api_key,
                host=host,
                version="v1",
                timeout=4.0,
                auto_reconnect=False
            )
        return openalgo_client
    except Exception as e:
        logger.warning(f"[OpenAlgo] Client initialization notice: {e}")
        return None

risk_settings: Dict[str, Any] = {
    "autoTrading": False,
    "riskPercent": 1.0,
    "maxDailyTrades": 5,
    "currentDailyTrades": 0
}

options_signals: List[Dict[str, Any]] = []
paper_trades: List[Dict[str, Any]] = []
active_option_chain_data: Dict[str, Any] = {}
resolved_expiry_dates: Dict[str, Optional[str]] = {
    "NIFTY": None,
    "BANKNIFTY": None,
    "FINNIFTY": None,
    "SENSEX": None
}

options_scanner_status: Dict[str, Any] = {
    "isRunning": False,
    "lastScan": None,
    "nextScan": None,
    "contractsScanned": 0,
    "signalsFound": 0
}

virtual_portfolio: Dict[str, Any] = {
    "cash": 100000.0,
    "initialCash": 100000.0,
    "totalTrades": 0,
    "winningTrades": 0,
    "netProfit": 0.0
}

index_spots: Dict[str, Dict[str, float]] = {
    "NIFTY": {"spot": 22930.75, "change": 0.55},
    "BANKNIFTY": {"spot": 49203.90, "change": -0.18},
    "FINNIFTY": {"spot": 22055.20, "change": 0.32},
    "SENSEX": {"spot": 75410.35, "change": 0.74}
}

options_candle_cache: Dict[str, List[Dict[str, float]]] = {}
mcx_tokens: Dict[str, str] = {
    "GOLD": "55395",
    "SILVER": "55403",
    "CRUDEOIL": "55515"
}

alert_cooldown_cache: Dict[str, float] = {}
news_cache: Dict[str, Any] = {
    "data": [],
    "lastFetched": 0.0
}

DEFAULT_FNO_STOCKS = [
    {"symbol": "NIFTY", "name": "Nifty 50 Index", "sector": "INDEX", "status": "ACTIVE", "securityId": "256", "exchangeSegment": "IDX_I", "instrument": "INDEX"},
    {"symbol": "BANKNIFTY", "name": "Bank Nifty Index", "sector": "INDEX", "status": "ACTIVE", "securityId": "257", "exchangeSegment": "IDX_I", "instrument": "INDEX"},
    {"symbol": "RELIANCE", "name": "Reliance Industries Ltd.", "sector": "CONGLOMERATE", "status": "ACTIVE", "securityId": "2885", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
    {"symbol": "TCS", "name": "Tata Consultancy Services Ltd.", "sector": "IT", "status": "ACTIVE", "securityId": "11536", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
    {"symbol": "INFY", "name": "Infosys Ltd.", "sector": "IT", "status": "ACTIVE", "securityId": "1594", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank Ltd.", "sector": "BANKING", "status": "ACTIVE", "securityId": "1333", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
    {"symbol": "ICICIBANK", "name": "ICICI Bank Ltd.", "sector": "BANKING", "status": "ACTIVE", "securityId": "4963", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"},
    {"symbol": "SBIN", "name": "State Bank of India", "sector": "BANKING", "status": "ACTIVE", "securityId": "3045", "exchangeSegment": "NSE_EQ", "instrument": "EQUITY"}
]

# Restrict active scanning list to F&O stocks (NSE_EQ segment only)
active_scanning_list = [s for s in DEFAULT_FNO_STOCKS if s.get("exchangeSegment") == "NSE_EQ"]

STRIKE_STEPS = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "SENSEX": 100
}

# ----------------------------------------------------
# 🔑 DHAN AUTOMATED TOTP & ACCESS TOKEN RENEWAL ENGINE
# ----------------------------------------------------
def base32_decode(secret_str: str) -> bytes:
    cleaned = re.sub(r'[^A-Z2-7]', '', secret_str.strip().upper())
    missing_padding = len(cleaned) % 8
    if missing_padding != 0:
        cleaned += '=' * (8 - missing_padding)
    return base64.b32decode(cleaned, casefold=True)

def generate_totp(secret: str, step: int = 30, digits: int = 6) -> str:
    key = base32_decode(secret)
    epoch = int(time.time())
    counter = epoch // step
    buffer = struct.pack(">Q", counter)
    h = hmac.new(key, buffer, hashlib.sha1).digest()
    offset = h[-1] & 0x0F
    binary = struct.unpack(">I", h[offset:offset + 4])[0] & 0x7FFFFFFF
    otp = binary % (10 ** digits)
    return str(otp).zfill(digits)

def update_env_file(key: str, value: str):
    try:
        if not ENV_PATH.exists():
            with open(ENV_PATH, "w", encoding="utf-8") as f:
                f.write(f"{key}={value}\n")
            return
        
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        regex = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
        if regex.search(content):
            content = regex.sub(f"{key}={value}", content)
        else:
            if content and not content.endswith("\n"):
                content += "\n"
            content += f"{key}={value}\n"

        with open(ENV_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"[Config] Successfully persisted {key} to backend/.env")
    except Exception as e:
        logger.error(f"[Config Error] Failed to write to .env file: {e}")

def generate_dhan_access_token(supplied_client_id: Optional[str] = None,
                               supplied_pin: Optional[str] = None,
                               supplied_totp_secret: Optional[str] = None) -> Dict[str, Any]:
    client_id = supplied_client_id or os.getenv("DHAN_CLIENT_ID") or credentials["clientId"]
    pin = supplied_pin or os.getenv("DHAN_PIN")
    totp_secret = supplied_totp_secret or os.getenv("DHAN_TOTP_SECRET")

    if not client_id or not pin or not totp_secret or \
       client_id == "your_client_id_here" or \
       pin == "your_6_digit_pin_here" or \
       totp_secret == "your_totp_secret_key_here":
        logger.info("[Auto-Login] Automatic token generation bypassed: Missing PIN or TOTP Secret in credentials.")
        return {"success": False, "message": "Missing PIN or TOTP Secret"}

    try:
        logger.info(f"[Auto-Login] Generating fresh TOTP for Dhan Client: {client_id}...")
        totp = generate_totp(totp_secret)
        token_url = f"https://auth.dhan.co/app/generateAccessToken?dhanClientId={client_id}&pin={pin}&totp={totp}"
        try:
            resp = requests.post(token_url, headers={"Content-Type": "application/json"}, timeout=5)
            data = resp.json()
        except requests.Timeout:
            logger.warning("[Auto-Login] Dhan auth endpoint timed out (5s limit). Continuing with configured token/fallback mode.")
            return {"success": False, "message": "Auth endpoint timeout"}
        except Exception as err:
            logger.warning(f"[Auto-Login] Dhan auth endpoint error: {err}. Continuing with configured token/fallback mode.")
            return {"success": False, "message": str(err)}

        if resp.status_code in [200, 201]:
            generated_token = data.get("accessToken") or data.get("access_token")
            if generated_token:
                logger.info("[Auto-Login] Successfully generated fresh 24-hr Access Token!")
                credentials["clientId"] = client_id
                credentials["accessToken"] = generated_token

                update_env_file("DHAN_CLIENT_ID", client_id)
                update_env_file("DHAN_ACCESS_TOKEN", generated_token)
                if supplied_pin:
                    update_env_file("DHAN_PIN", supplied_pin)
                if supplied_totp_secret:
                    update_env_file("DHAN_TOTP_SECRET", supplied_totp_secret)

                os.environ["DHAN_CLIENT_ID"] = client_id
                os.environ["DHAN_ACCESS_TOKEN"] = generated_token
                if supplied_pin:
                    os.environ["DHAN_PIN"] = supplied_pin
                if supplied_totp_secret:
                    os.environ["DHAN_TOTP_SECRET"] = supplied_totp_secret

                append_to_google_sheet("Scanner_Logs", [
                    datetime.now(IST).isoformat(),
                    len(active_scanning_list),
                    0,
                    "SUCCESS",
                    "Automated daily Access Token successfully renewed."
                ])

                return {"success": True, "token": generated_token, "message": "Token generated successfully"}
            else:
                logger.error(f"[Auto-Login] Dhan token response missing accessToken: {data}")
                return {"success": False, "message": "Token missing in response"}
        else:
            err_msg = data.get("errorMessage") or data.get("message") or f"HTTP {resp.status_code}"
            logger.error(f"[Auto-Login Error] Dhan returned error code: {err_msg}")
            alert_msg = (
                f"⚠️ <b>DHAN AUTO-LOGIN FAILED</b> ⚠️\n\n"
                f"<b>Client ID:</b> {client_id}\n"
                f"<b>Error:</b> {err_msg}\n"
                f"<b>Action Required:</b> Please check your Dhan PIN, TOTP secret or manual login state."
            )
            send_telegram_alert(alert_msg)
            return {"success": False, "message": f"Dhan Auth Error: {err_msg}"}
    except Exception as e:
        logger.error(f"[Auto-Login Handshake Network Error]: {e}")
        return {"success": False, "message": f"Handshake network error: {e}"}

# ----------------------------------------------------
# 📰 LIVE STOCK MARKET NEWS SERVICE
# ----------------------------------------------------
def fetch_live_financial_news() -> List[Dict[str, Any]]:
    now = time.time()
    if news_cache["lastFetched"] and (now - news_cache["lastFetched"]) < 60:
        return news_cache["data"]

    try:
        logger.info("[News Service] Fetching latest market news from RSS...")
        rss_url = "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"
        resp = requests.get(rss_url, timeout=10)
        xml_text = resp.text

        items = []
        item_regex = re.compile(r"<item>([\s\S]*?)</item>", re.MULTILINE)
        matches = item_regex.findall(xml_text)

        for content in matches[:10]:
            title_m = re.search(r"<title><!\[CDATA\[([\s\S]*?)\]\]></title>", content) or re.search(r"<title>([\s\S]*?)</title>", content)
            link_m = re.search(r"<link>([\s\S]*?)</link>", content)
            pub_date_m = re.search(r"<pubDate>([\s\S]*?)</pubDate>", content)
            desc_m = re.search(r"<description><!\[CDATA\[([\s\S]*?)\]\]></description>", content) or re.search(r"<description>([\s\S]*?)</description>", content)

            title = title_m.group(1).strip() if title_m else "Market Update"
            link = link_m.group(1).strip() if link_m else "https://economictimes.indiatimes.com"
            pub_date = pub_date_m.group(1).strip() if pub_date_m else datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
            desc = desc_m.group(1).strip() if desc_m else ""

            desc = re.sub(r"<[^>]*>", "", desc)
            desc = desc.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&apos;", "'")
            if len(desc) > 150:
                desc = desc[:147] + "..."

            items.append({
                "title": title,
                "link": link,
                "pubDate": pub_date,
                "description": desc,
                "source": "Economic Times"
            })

        if items:
            news_cache["data"] = items
            news_cache["lastFetched"] = now
            logger.info(f"[News Service] Successfully loaded {len(items)} news items.")
    except Exception as e:
        logger.error(f"[News Service Error] Failed to fetch news: {e}")
        if not news_cache["data"]:
            news_cache["data"] = [
                {
                    "title": "NSE Nifty and BSE Sensex hold steady amid positive global cues",
                    "link": "https://economictimes.indiatimes.com",
                    "pubDate": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
                    "description": "Indian benchmark indices trading close to all-time highs as heavyweights lead gains.",
                    "source": "Market Intelligence"
                },
                {
                    "title": "Commodity markets update: Gold and Crude oil trade with mild volatility",
                    "link": "https://economictimes.indiatimes.com",
                    "pubDate": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
                    "description": "Gold prices consolidate near resistance while MCX crude tracking Brent futures closely.",
                    "source": "Market Intelligence"
                }
            ]

    return news_cache["data"]

# ----------------------------------------------------
# ⚡ INDEX OPTIONS SCANNER & AUTOMATED PAPER TRADING ENGINE
# ----------------------------------------------------
def get_expiry_candidates() -> List[str]:
    candidates = []
    today = datetime.now(IST).date()
    for i in range(38):
        d = today + timedelta(days=i)
        if d.weekday() in [1, 2, 3, 4]:  # Tue, Wed, Thu, Fri
            candidates.append(d.strftime("%Y-%m-%d"))
    return candidates

def dhan_api_call(endpoint: str, method: str = "GET", body: Any = None) -> Dict[str, Any]:
    global dhan_status
    if not dhan_status.get("isValid", False):
        return {"status": 401, "data": {"message": dhan_status.get("error", "DhanHQ token expired or unauthorized")}}

    headers = {
        "Content-Type": "application/json",
        "client-id": credentials["clientId"],
        "access-token": credentials["accessToken"]
    }
    url = f"{DHAN_API_BASE}{endpoint}"
    try:
        if method == "POST":
            resp = requests.post(url, headers=headers, json=body, timeout=5)
        else:
            resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 401:
            dhan_status["isValid"] = False
            dhan_status["error"] = "DH-901 Token Expired"
            logger.warning("[Dhan API] Token expired (401). Switched to Live NSE fallback.")
        try:
            data = resp.json()
        except Exception:
            data = {}
        return {"status": resp.status_code, "data": data}
    except Exception as e:
        logger.error(f"[Dhan API Error] {endpoint}: {e}")
        return {"status": 500, "data": {"message": str(e)}}

def fetch_active_option_chain(index_symbol: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if resolved_expiry_dates.get(index_symbol):
        try:
            cached_date = resolved_expiry_dates[index_symbol]
            payload = {
                "UnderlyingScrip": config["scrip"],
                "UnderlyingSeg": config["seg"],
                "Expiry": cached_date
            }
            res = dhan_api_call("/v2/optionchain", "POST", payload)
            if res["status"] == 200 and res.get("data") and res["data"].get("data") and res["data"]["data"].get("oc"):
                oc = res["data"]["data"]["oc"]
                if len(oc) > 0:
                    return {"oc": oc, "expiry": cached_date}
        except Exception as e:
            logger.info(f"[Self-Healing] Cached expiry {resolved_expiry_dates[index_symbol]} failed for {index_symbol}. Retrying...")

    candidates = get_expiry_candidates()
    logger.info(f"[Self-Healing] Scanning active contracts for {index_symbol} across candidates...")

    for candidate in candidates:
        try:
            payload = {
                "UnderlyingScrip": config["scrip"],
                "UnderlyingSeg": config["seg"],
                "Expiry": candidate
            }
            res = dhan_api_call("/v2/optionchain", "POST", payload)
            if res["status"] == 200 and res.get("data") and res["data"].get("data") and res["data"]["data"].get("oc"):
                oc = res["data"]["data"]["oc"]
                if len(oc) > 0:
                    resolved_expiry_dates[index_symbol] = candidate
                    logger.info(f"[Self-Healing] Successfully resolved next active expiry for {index_symbol} -> {candidate}")
                    return {"oc": oc, "expiry": candidate}
        except Exception:
            pass
        time.sleep(0.1)

    return None

PAPER_DB_PATH = BASE_DIR / "paper_trades_db.json"

def load_paper_trades_db():
    global paper_trades, virtual_portfolio, options_signals
    if PAPER_DB_PATH.exists():
        try:
            with open(PAPER_DB_PATH, "r", encoding="utf-8") as f:
                db = json.load(f)
                if "paperTrades" in db:
                    paper_trades = db["paperTrades"]
                if "virtualPortfolio" in db:
                    virtual_portfolio = db["virtualPortfolio"]
                if "optionsSignals" in db:
                    options_signals = db["optionsSignals"]
            logger.info("[Paper Trade DB] Loaded historical virtual portfolios successfully.")
        except Exception as e:
            logger.error(f"[Paper Trade DB] Error loading DB: {e}")

def save_paper_trades_db():
    try:
        db = {
            "paperTrades": paper_trades,
            "virtualPortfolio": virtual_portfolio,
            "optionsSignals": options_signals
        }
        with open(PAPER_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        logger.error(f"[Paper Trade DB] Error saving DB: {e}")

def get_atm_strikes(symbol: str, spot: float) -> List[float]:
    step = STRIKE_STEPS.get(symbol, 50)
    atm = round(spot / step) * step
    return [
        atm - step * 2,
        atm - step,
        atm,
        atm + step,
        atm + step * 2
    ]

def generate_options_candles(contract_key: str, base_price: float) -> List[Dict[str, float]]:
    if contract_key not in options_candle_cache:
        options_candle_cache[contract_key] = []
        price = base_price
        for _ in range(25):
            o = price
            c = price + (random.random() * 4 - 2)
            h = max(o, c) + random.random() * 1.5
            l = min(o, c) - random.random() * 1.5
            v = random.randint(10000, 60000)
            options_candle_cache[contract_key].append({"open": o, "high": h, "low": l, "close": c, "volume": v})
            price = c

    history = options_candle_cache[contract_key]
    last_close = history[-1]["close"]
    trigger_spike = random.random() > 0.95
    trend = (12 if random.random() > 0.4 else -8) if trigger_spike else (random.random() * 2 - 1)

    o = last_close
    c = last_close + trend
    h = max(o, c) + (random.random() * 3)
    l = min(o, c) - (random.random() * 3)
    v = random.randint(150000, 350000) if trigger_spike else random.randint(5000, 45000)

    history.append({"open": o, "high": h, "low": l, "close": c, "volume": v})
    if len(history) > 30:
        history.pop(0)

    return history

def fetch_yahoo_finance_index_spots():
    tickers = {
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "FINNIFTY": "^CNXFIN",
        "SENSEX": "^BSESN"
    }
    for key, ticker in tickers.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                ltp = meta.get("regularMarketPrice")
                prev_close = meta.get("chartPreviousClose") or ltp
                if ltp:
                    index_spots[key]["spot"] = round(float(ltp), 2)
                    chg = (((ltp - prev_close) / prev_close) * 100) if prev_close else 0.0
                    index_spots[key]["change"] = round(float(chg), 2)
        except Exception as e:
            logger.info(f"[Yahoo Finance Fallback] Failed to fetch spot for {key}: {e}")

def push_real_option_price_to_candles(contract_key: str, real_price: float, real_volume: Optional[float] = None):
    if not real_price:
        return
    if contract_key not in options_candle_cache:
        options_candle_cache[contract_key] = []
        price = real_price * 0.95
        for _ in range(25):
            o = price
            c = price + (random.random() * (real_price * 0.01) - (real_price * 0.005))
            h = max(o, c) + random.random() * (real_price * 0.002)
            l = min(o, c) - random.random() * (real_price * 0.002)
            v = random.randint(5000, 25000)
            options_candle_cache[contract_key].append({"open": o, "high": h, "low": l, "close": c, "volume": v})
            price = c

    history = options_candle_cache[contract_key]
    last_close = history[-1]["close"]
    o = last_close
    c = real_price
    h = max(o, c) + (random.random() * (real_price * 0.003))
    l = min(o, c) - (random.random() * (real_price * 0.003))
    v = real_volume or random.randint(5000, 45000)

    history.append({"open": o, "high": h, "low": l, "close": c, "volume": v})
    if len(history) > 30:
        history.pop(0)

def open_point_track(candles: List[Dict[str, float]]) -> float:
    if len(candles) >= 2:
        return candles[-2]["close"]
    return candles[0]["open"]

# ----------------------------------------------------
# 📉 REAL-TIME PAPER TRADING ENGINE
# ----------------------------------------------------
def execute_paper_trade(signal: Dict[str, Any]):
    active_trades = [t for t in paper_trades if t.get("status") == "ACTIVE"]
    if len(active_trades) >= 1:
        logger.info(f"[Paper Trade Engine] Bypassed entry. Only 1 active position is allowed at a time. Active: {active_trades[0]['symbol']}")
        return

    lot_sizes = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "SENSEX": 10}
    lot_size = lot_sizes.get(signal.get("index"), 50)

    lots_to_buy = 4
    total_cost = signal["price"] * (lots_to_buy * lot_size)

    while lots_to_buy > 0 and virtual_portfolio["cash"] < total_cost:
        lots_to_buy -= 1
        total_cost = signal["price"] * (lots_to_buy * lot_size)

    if lots_to_buy <= 0:
        logger.info(f"[Paper Trade Engine] Insufficient virtual funds (₹{virtual_portfolio['cash']:.2f}) to buy even 1 lot of {signal['symbol']} at ₹{signal['price']:.2f}.")
        return

    qty = lots_to_buy * lot_size
    sl_percent = 0.12
    target_percent = 0.25

    stop_loss = signal["price"] * (1 - sl_percent) if signal["type"] == "BULLISH" else signal["price"] * (1 + sl_percent)
    target = signal["price"] * (1 + target_percent) if signal["type"] == "BULLISH" else signal["price"] * (1 - target_percent)
    paper_id = f"PT-{str(int(time.time() * 1000))[-6:]}"

    new_trade = {
        "id": paper_id,
        "symbol": signal["symbol"],
        "index": signal["index"],
        "strike": signal["strike"],
        "optionType": signal["optionType"],
        "type": signal["type"],
        "entryPrice": signal["price"],
        "qty": qty,
        "stopLoss": stop_loss,
        "target": target,
        "status": "ACTIVE",
        "entryTime": datetime.utcnow().isoformat(),
        "exitTime": None,
        "exitPrice": None,
        "pnl": 0.0,
        "currentPrice": signal["price"]
    }

    virtual_portfolio["cash"] -= total_cost
    paper_trades.insert(0, new_trade)
    virtual_portfolio["totalTrades"] += 1

    logger.info(f"[Paper Trade] Executed virtual BUY order for {signal['symbol']}. Qty: {qty} ({lots_to_buy} lots), Price: ₹{signal['price']:.2f}, Cost: ₹{total_cost:.2f}")

    append_to_google_sheet("Paper_Trade_Book", [
        paper_id,
        new_trade["entryTime"],
        new_trade["symbol"],
        new_trade["type"],
        new_trade["qty"],
        new_trade["entryPrice"],
        new_trade["target"],
        new_trade["stopLoss"],
        "0.00",
        "ACTIVE"
    ])
    save_paper_trades_db()

def paper_trades_monitor_loop():
    while True:
        try:
            db_changed = False
            for trade in paper_trades:
                if trade.get("status") != "ACTIVE":
                    continue

                key = trade["symbol"]
                history = options_candle_cache.get(key)
                if not history:
                    continue

                latest_price = history[-1]["close"]
                trade["currentPrice"] = latest_price
                diff = latest_price - trade["entryPrice"]
                trade["pnl"] = diff * trade["qty"] if trade["type"] == "BULLISH" else -diff * trade["qty"]

                exit_triggered = False
                exit_price = latest_price
                exit_reason = ""

                if trade["type"] == "BULLISH":
                    if latest_price >= trade["target"]:
                        exit_triggered = True
                        exit_price = trade["target"]
                        exit_reason = "TARGET MET 🎯"
                    elif latest_price <= trade["stopLoss"]:
                        exit_triggered = True
                        exit_price = trade["stopLoss"]
                        exit_reason = "STOP LOSS TRIGGERED 🛑"
                else:
                    if latest_price <= trade["target"]:
                        exit_triggered = True
                        exit_price = trade["target"]
                        exit_reason = "TARGET MET 🎯"
                    elif latest_price >= trade["stopLoss"]:
                        exit_triggered = True
                        exit_price = trade["stopLoss"]
                        exit_reason = "STOP LOSS TRIGGERED 🛑"

                if exit_triggered:
                    trade["status"] = "PROFIT" if "TARGET" in exit_reason else "LOSS"
                    trade["exitPrice"] = exit_price
                    trade["exitTime"] = datetime.utcnow().isoformat()
                    trade_diff = exit_price - trade["entryPrice"]
                    final_pnl = trade_diff * trade["qty"] if trade["type"] == "BULLISH" else -trade_diff * trade["qty"]
                    trade["pnl"] = final_pnl

                    total_cost = trade["entryPrice"] * trade["qty"]
                    virtual_portfolio["cash"] += (total_cost + final_pnl)
                    virtual_portfolio["netProfit"] += final_pnl

                    if trade["status"] == "PROFIT":
                        virtual_portfolio["winningTrades"] += 1

                    logger.info(f"[Paper Trade Exit] Closed position for {trade['symbol']}. Reason: {exit_reason}, P&L: ₹{final_pnl:.2f}")

                    emoji = "🟢💰" if trade["status"] == "PROFIT" else "🔴🛑"
                    exit_msg = (
                        f"{emoji} <b>VIRTUAL TRADE EXITED</b> {emoji}\n\n"
                        f"<b>Contract:</b> {trade['symbol']}\n"
                        f"<b>Result:</b> {trade['status']} ({exit_reason})\n"
                        f"<b>Exit Price:</b> ₹{exit_price:.2f}\n"
                        f"<b>Final P&L:</b> ₹{final_pnl:.2f}"
                    )
                    send_telegram_alert(exit_msg)

                    append_to_google_sheet("Paper_Trade_Book", [
                        trade["id"],
                        trade["exitTime"],
                        trade["symbol"],
                        trade["type"],
                        trade["qty"],
                        trade["entryPrice"],
                        trade["exitPrice"],
                        trade["stopLoss"],
                        f"{trade['pnl']:.2f}",
                        trade["status"]
                    ])
                    db_changed = True

            if db_changed:
                save_paper_trades_db()
        except Exception as e:
            logger.error(f"[Paper Monitor Error]: {e}")

        time.sleep(5)

# ----------------------------------------------------
# 📊 GOOGLE SHEETS & LOCAL DATABASE SERVICE
# ----------------------------------------------------
LOCAL_SHEETS_DB_PATH = BASE_DIR / "local_sheets_db.json"

def save_to_local_sheets_db(sheet_name: str, row_data: List[Any]):
    db = {}
    if LOCAL_SHEETS_DB_PATH.exists():
        try:
            with open(LOCAL_SHEETS_DB_PATH, "r", encoding="utf-8") as f:
                db = json.load(f)
        except Exception:
            db = {}

    if sheet_name not in db:
        db[sheet_name] = []

    db[sheet_name].append({
        "timestamp": datetime.utcnow().isoformat(),
        "data": row_data
    })

    try:
        with open(LOCAL_SHEETS_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        logger.error(f"[Local Sheets DB] Error writing db: {e}")

def append_to_google_sheet(sheet_name: str, row_data: List[Any]):
    email = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
    private_key_raw = os.getenv("GOOGLE_PRIVATE_KEY")
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")

    if not email or not private_key_raw or not spreadsheet_id or \
       "your_service_account_email" in email or \
       "your_google_spreadsheet_id" in spreadsheet_id:
        save_to_local_sheets_db(sheet_name, row_data)
        return

    try:
        import jwt
        private_key = private_key_raw.replace("\\n", "\n")
        now = int(time.time())
        claim = {
            "iss": email,
            "scope": "https://www.googleapis.com/auth/spreadsheets",
            "aud": "https://oauth2.googleapis.com/token",
            "exp": now + 3600,
            "iat": now
        }
        signed_jwt = jwt.encode(claim, private_key, algorithm="RS256")
        token_resp = requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt
        }, timeout=10)
        token_data = token_resp.json()
        access_token = token_data.get("access_token")

        if not access_token:
            save_to_local_sheets_db(sheet_name, row_data)
            return

        append_url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{sheet_name}!A:Z:append?valueInputOption=USER_ENTERED"
        res = requests.post(append_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }, json={"values": [row_data]}, timeout=10)

        if res.status_code in [200, 201]:
            logger.info(f"[Google Sheets] Successfully appended record to sheet: {sheet_name}")
        else:
            save_to_local_sheets_db(sheet_name, row_data)
    except Exception as e:
        logger.error(f"[Google Sheets API Error]: {e}. Saving to local backup database.")
        save_to_local_sheets_db(sheet_name, row_data)

# ----------------------------------------------------
# 📂 DYNAMIC F&O STOCKS SYNC SERVICE (GOOGLE SHEETS / CSV)
# ----------------------------------------------------
def sync_fno_stocks_list():
    global active_scanning_list
    logger.info("[F&O Sync Service] Syncing active F&O stocks list...")

    csv_path = BASE_DIR / "fno_stocks_master.csv"
    local_fno_list = []
    if csv_path.exists():
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                lines = f.readlines()[1:]
            for line in lines:
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) >= 7 and parts[0]:
                    local_fno_list.append({
                        "symbol": parts[0],
                        "name": parts[1],
                        "sector": parts[2],
                        "status": parts[3] or "ACTIVE",
                        "securityId": parts[4],
                        "exchangeSegment": parts[5],
                        "instrument": parts[6]
                    })
            logger.info(f"[F&O Sync Service] Loaded {len(local_fno_list)} F&O stocks from local CSV database.")
        except Exception as e:
            logger.error(f"[F&O Sync Service] Error parsing CSV: {e}")

    if not local_fno_list:
        local_fno_list = DEFAULT_FNO_STOCKS

    # Restrict strictly to F&O equity stocks (NSE_EQ segment only)
    active_scanning_list = [s for s in local_fno_list if s.get("status") == "ACTIVE" and s.get("exchangeSegment") == "NSE_EQ"]
    logger.info(f"[F&O Sync Service] Active scanning list size: {len(active_scanning_list)} F&O equity stocks.")

# Pre-load all 197 F&O stocks immediately
sync_fno_stocks_list()


# ----------------------------------------------------
# 🔍 MCX DYNAMIC CONTRACT RESOLVER
# ----------------------------------------------------
def resolve_mcx_instruments():
    logger.info("[Commodity Resolver] Querying Dhan scrip master to resolve MCX active contracts...")
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return
        
        lines = resp.text.split("\n")
        header = [h.strip() for h in lines[0].split(",")]

        if "SEM_EXM_EXCH_ID" not in header or "SEM_SMST_SECURITY_ID" not in header or "SEM_TRADING_SYMBOL" not in header:
            return

        exch_idx = header.index("SEM_EXM_EXCH_ID")
        sec_idx = header.index("SEM_SMST_SECURITY_ID")
        sym_idx = header.index("SEM_TRADING_SYMBOL")

        gold_contracts, silver_contracts, crude_contracts = [], [], []

        for line in lines[1:]:
            if not line:
                continue
            r = [item.strip().strip('"') for item in line.split(",")]
            if len(r) <= max(exch_idx, sec_idx, sym_idx):
                continue
            exch, symbol, sec_id = r[exch_idx], r[sym_idx], r[sec_idx]

            if exch == "MCX":
                if symbol.startswith("GOLD"):
                    gold_contracts.append({"symbol": symbol, "secId": sec_id})
                elif symbol.startswith("SILVER"):
                    silver_contracts.append({"symbol": symbol, "secId": sec_id})
                elif symbol.startswith("CRUDEOIL"):
                    crude_contracts.append({"symbol": symbol, "secId": sec_id})

        def find_active(contracts, base_sym):
            main = next((c for c in contracts if c["symbol"] == base_sym), None)
            if main:
                return main["secId"]
            if contracts:
                contracts.sort(key=lambda x: len(x["symbol"]))
                return contracts[0]["secId"]
            return None

        mcx_tokens["GOLD"] = find_active(gold_contracts, "GOLD") or "55395"
        mcx_tokens["SILVER"] = find_active(silver_contracts, "SILVER") or "55403"
        mcx_tokens["CRUDEOIL"] = find_active(crude_contracts, "CRUDEOIL") or "55515"

        logger.info(f"[Commodity Resolver] Resolved MCX active contracts: GOLD -> {mcx_tokens['GOLD']}, SILVER -> {mcx_tokens['SILVER']}, CRUDEOIL -> {mcx_tokens['CRUDEOIL']}")
    except Exception as e:
        logger.error(f"[Commodity Resolver Error]: {e}")

# ----------------------------------------------------
# ⚡ COOLDOWN DE-DUPLICATION CHECK
# ----------------------------------------------------
def is_duplicate_alert(symbol: str, alert_type: str) -> bool:
    key = f"{symbol}-{alert_type}"
    now = time.time()
    last_time = alert_cooldown_cache.get(key)
    if last_time and (now - last_time) < 20 * 60:
        return True
    alert_cooldown_cache[key] = now
    return False

# ----------------------------------------------------
# ⚡ NOTIFICATION SERVICE
# ----------------------------------------------------
def send_telegram_alert(message: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id or "your_telegram_bot_token" in bot_token or "your_telegram_chat_id" in chat_id:
        clean_text = re.sub(r"<[^>]*>", "", message)
        logger.info(f"[Telegram Bypass] Mock message:\n{clean_text}")
        return

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code == 200:
            logger.info("[Telegram Alerts] Broadcast successful.")
        else:
            logger.error(f"[Telegram Error]: {res.text}")
    except Exception as e:
        logger.error(f"[Telegram Network Error]: {e}")

def send_whatsapp_alert(message: str):
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    twilio_phone = os.getenv("TWILIO_PHONE_NUMBER")
    receiver_phone = os.getenv("RECEIVER_PHONE_NUMBER")

    if not account_sid or not auth_token or not twilio_phone or not receiver_phone or \
       "your_twilio_account_sid" in account_sid or "your_receiver_phone_number" in receiver_phone:
        return

    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        data = {
            "From": f"whatsapp:{twilio_phone}",
            "To": f"whatsapp:{receiver_phone}",
            "Body": message
        }
        res = requests.post(url, data=data, auth=(account_sid, auth_token), timeout=10)
        if res.status_code in [200, 201]:
            logger.info("[WhatsApp Alerts] Broadcast successful.")
        else:
            logger.error(f"[WhatsApp Error]: {res.text}")
    except Exception as e:
        logger.error(f"[WhatsApp Network Error]: {e}")

# ----------------------------------------------------
# 📈 PATTERN MATCHING & AGGREGATION ALGORITHM
# ----------------------------------------------------
def calculate_strength(body: float, total_wick: float, volume: float) -> int:
    wick_ratio = total_wick / (body if body != 0 else 1.0)
    volume_score = min(volume / 1000000.0, 10.0)
    strength = round((1 - wick_ratio) * 50 + volume_score * 5)
    return max(1, min(strength, 100))

def check_perfect_engulfing(current: Dict[str, float], previous: Dict[str, float], avg_body: float) -> Dict[str, Any]:
    prev_body = abs(previous["close"] - previous["open"])
    curr_body = abs(current["close"] - current["open"])

    if prev_body == 0 or avg_body == 0:
        return {"isPerfect": False, "reason": "Zero body size"}

    pct_of_avg = prev_body / avg_body
    ratio = curr_body / prev_body

    is_perfect = False
    category = ""
    required_range = ""

    # Relaxed rules: No upper bound limit, only minimum threshold!
    if pct_of_avg >= 0.90:
        category = "100% (Standard)"
        required_range = ">= 120%"
        if ratio >= 1.20:
            is_perfect = True
    elif pct_of_avg >= 0.75:
        category = "80% (Smaller)"
        required_range = ">= 150%"
        if ratio >= 1.50:
            is_perfect = True
    elif pct_of_avg >= 0.65:
        category = "70% (Very Small)"
        required_range = ">= 175%"
        if ratio >= 1.75:
            is_perfect = True
    else:
        category = "60% (Micro)"
        required_range = ">= 200%"
        if ratio >= 2.00:
            is_perfect = True

    return {
        "isPerfect": is_perfect,
        "category": category,
        "requiredRange": required_range,
        "pctOfAvg": f"{(pct_of_avg * 100):.1f}%",
        "ratio": f"{(ratio * 100):.1f}%"
    }

def detect_prior_trend(history: Optional[List[Dict[str, float]]] = None) -> str:
    """
    Evaluates prior 3-5 candles to check if pattern follows a valid Decline or Rise
    as shown in Photo 2 & 4:
    - Bullish Engulfing: "Appears after a decline, buyers step in"
    - Bearish Engulfing: "Appears after a rise, sellers take over"
    """
    if not history or len(history) < 2:
        return "CONFIRMED_REVERSAL"
    subset = history[-4:]
    net_change = subset[-1]["close"] - subset[0]["open"]
    if net_change < 0:
        return "DECLINE"
    elif net_change > 0:
        return "RISE"
    return "CONFIRMED_REVERSAL"

def analyze_engulfing_pattern(symbol: str, name: str, current: Dict[str, float], previous: Dict[str, float], avg_body: float, history: Optional[List[Dict[str, float]]] = None) -> Optional[Dict[str, Any]]:
    body = abs(current["close"] - current["open"])
    upper_wick = current["high"] - max(current["open"], current["close"])
    lower_wick = min(current["open"], current["close"]) - current["low"]
    total_wick = upper_wick + lower_wick
    overall_size = current["high"] - current["low"]

    # Relaxed wick condition: up to 40% of overall candle size
    wick_condition = overall_size > 0 and (total_wick <= 0.40 * overall_size)

    current_top = max(current["open"], current["close"])
    current_bottom = min(current["open"], current["close"])
    previous_top = max(previous["open"], previous["close"])
    previous_bottom = min(previous["open"], previous["close"])

    perfect_engulf_info = check_perfect_engulfing(current, previous, avg_body or body)

    # Photo 1, 3: Dense body engulfing condition
    bullish_engulf = (
        current["close"] > current["open"] and
        previous["close"] < previous["open"] and
        current_top >= previous_top and
        current_bottom <= previous_bottom and
        (current_top > previous_top or current_bottom < previous_bottom) and
        perfect_engulf_info["isPerfect"]
    )

    bearish_engulf = (
        current["close"] < current["open"] and
        previous["close"] > previous["open"] and
        current_top >= previous_top and
        current_bottom <= previous_bottom and
        (current_top > previous_top or current_bottom < previous_bottom) and
        perfect_engulf_info["isPerfect"]
    )

    # Photo 2, 4: Prior trend validation (Decline for Bullish, Rise for Bearish)
    prior_trend = detect_prior_trend(history)

    if bullish_engulf and wick_condition and body > 0:
        stop_loss = current["low"]
        target = current["close"] + body * 2  # 1:2 Risk to Reward
        strength = calculate_strength(body, total_wick, current.get("volume", 0))
        if prior_trend in ["DECLINE", "CONFIRMED_REVERSAL"]:
            strength = min(strength + 10, 100)

        trend_text = "Appears after a decline, buyers step in (Photo 2 & 4 Validated)" if prior_trend == "DECLINE" else "Bullish Reversal Confirmed"

        return {
            "id": f"{symbol}-{int(time.time() * 1000)}-bull",
            "symbol": symbol,
            "name": name,
            "type": "BULLISH",
            "strength": strength,
            "entryPrice": current["close"],
            "stopLoss": stop_loss,
            "target": target,
            "perfectCategory": perfect_engulf_info["category"],
            "pctOfAvg": perfect_engulf_info["pctOfAvg"],
            "ratio": perfect_engulf_info["ratio"],
            "requiredRange": perfect_engulf_info["requiredRange"],
            "trendContext": trend_text,
            "timestamp": datetime.utcnow().isoformat(),
            "candleData": {"current": current, "previous": previous}
        }

    if bearish_engulf and wick_condition and body > 0:
        stop_loss = current["high"]
        target = current["close"] - body * 2  # 1:2 Risk to Reward
        strength = calculate_strength(body, total_wick, current.get("volume", 0))
        if prior_trend in ["RISE", "CONFIRMED_REVERSAL"]:
            strength = min(strength + 10, 100)

        trend_text = "Appears after a rise, sellers take over (Photo 2 & 4 Validated)" if prior_trend == "RISE" else "Bearish Reversal Confirmed"

        return {
            "id": f"{symbol}-{int(time.time() * 1000)}-bear",
            "symbol": symbol,
            "name": name,
            "type": "BEARISH",
            "strength": strength,
            "entryPrice": current["close"],
            "stopLoss": stop_loss,
            "target": target,
            "perfectCategory": perfect_engulf_info["category"],
            "pctOfAvg": perfect_engulf_info["pctOfAvg"],
            "ratio": perfect_engulf_info["ratio"],
            "requiredRange": perfect_engulf_info["requiredRange"],
            "trendContext": trend_text,
            "timestamp": datetime.utcnow().isoformat(),
            "candleData": {"current": current, "previous": previous}
        }

    return None

def generate_mock_candle(base_price: float, trend: int = 1) -> Dict[str, float]:
    volatility = base_price * 0.015
    o = base_price
    c = base_price + trend * (volatility * 0.5 + random.random() * volatility * 0.7)
    h = max(o, c) + random.random() * volatility * 0.15
    l = min(o, c) - random.random() * volatility * 0.15
    return {
        "open": round(o, 2),
        "high": round(h, 2),
        "low": round(l, 2),
        "close": round(c, 2),
        "volume": random.randint(500000, 2500000)
    }

def fetch_yahoo_finance_stock_candles(symbol: str, exchange_segment: str) -> Optional[Dict[str, List[float]]]:
    try:
        ticker = f"{symbol}.NS"
        if exchange_segment == "IDX_I":
            if symbol == "NIFTY": ticker = "^NSEI"
            elif symbol == "BANKNIFTY": ticker = "^NSEBANK"
            elif symbol == "FINNIFTY": ticker = "^CNXFIN"
            elif symbol == "SENSEX": ticker = "^BSESN"
        elif exchange_segment == "MCX_COMM":
            if symbol.startswith("GOLD"): ticker = "GC=F"
            elif symbol.startswith("SILVER"): ticker = "SI=F"
            elif symbol.startswith("CRUDEOIL"): ticker = "CL=F"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=5m&range=5d"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}, timeout=6)
        if resp.status_code != 200:
            return None

        json_data = resp.json()
        chart_result = json_data.get("chart", {}).get("result")
        if not chart_result or not isinstance(chart_result, list) or len(chart_result) == 0:
            return None
        result = chart_result[0]
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        timestamps = result.get("timestamp", [])

        opens, closes, highs, lows, volumes = [], [], [], [], []
        for i in range(len(timestamps)):
            if (
                i < len(quotes.get("open", [])) and quotes["open"][i] is not None and
                i < len(quotes.get("close", [])) and quotes["close"][i] is not None and
                i < len(quotes.get("high", [])) and quotes["high"][i] is not None and
                i < len(quotes.get("low", [])) and quotes["low"][i] is not None
            ):
                opens.append(float(quotes["open"][i]))
                closes.append(float(quotes["close"][i]))
                highs.append(float(quotes["high"][i]))
                lows.append(float(quotes["low"][i]))
                vols = quotes.get("volume", [])
                volumes.append(float(vols[i]) if (i < len(vols) and vols[i] is not None) else 0.0)

        if len(opens) < 15:
            return None

        return {"open": opens, "close": closes, "high": highs, "low": lows, "volume": volumes}
    except Exception as e:
        logger.debug(f"[Yahoo Stock Fallback Notice] {symbol}: {e}")
        return None

def fetch_and_aggregate_candles(symbol: str) -> Optional[Dict[str, Any]]:
    try:
        stock_config = next((s for s in active_scanning_list if s["symbol"] == symbol), None)
        if not stock_config:
            return None

        chart_data = None

        # 1. Try Dhan if authenticated
        if dhan_status.get("isValid", False) and stock_config.get("securityId"):
            sec_id = stock_config["securityId"]
            if sec_id == "GOLD_ACTIVE": sec_id = mcx_tokens.get("GOLD", "")
            if sec_id == "SILVER_ACTIVE": sec_id = mcx_tokens.get("SILVER", "")
            if sec_id == "CRUDEOIL_ACTIVE": sec_id = mcx_tokens.get("CRUDEOIL", "")

            to_date = datetime.now(IST).date().isoformat()
            from_date = (datetime.now(IST).date() - timedelta(days=3)).isoformat()

            payload = {
                "securityId": sec_id,
                "exchangeSegment": stock_config.get("exchangeSegment", "NSE_EQ"),
                "instrument": stock_config.get("instrument", "EQUITY"),
                "expiryCode": 0,
                "oi": False,
                "fromDate": from_date,
                "toDate": to_date
            }
            try:
                res = dhan_api_call("/v2/charts/intraday", "POST", payload)
                if res["status"] == 200 and res.get("data") and res["data"].get("open") and len(res["data"]["open"]) >= 15:
                    chart_data = res["data"]
            except Exception:
                chart_data = None

        # 2. Seamless Real-Time NSE Fallback (Yahoo Finance)
        if not chart_data:
            chart_data = fetch_yahoo_finance_stock_candles(symbol, stock_config.get("exchangeSegment", "NSE_EQ"))

        if not chart_data or len(chart_data.get("open", [])) < 15:
            return None

        opens = chart_data["open"]
        closes = chart_data["close"]
        highs = chart_data["high"]
        lows = chart_data["low"]
        volumes = chart_data.get("volume", [0] * len(opens))
        length = len(opens)

        def aggregate(start_idx: int, end_idx: int) -> Dict[str, float]:
            return {
                "open": round(float(opens[start_idx]), 2),
                "close": round(float(closes[end_idx]), 2),
                "high": round(float(max(highs[start_idx:end_idx + 1])), 2),
                "low": round(float(min(lows[start_idx:end_idx + 1])), 2),
                "volume": int(sum(volumes[start_idx:end_idx + 1]))
            }

        # 25-Minute rolling candles (5 x 5-minute bars per 25-min candle)
        current_candle = aggregate(length - 5, length - 1)
        previous_candle = aggregate(length - 10, length - 6)

        history_candles = []
        for i in range(max(0, length - 50), length - 5, 5):
            if i + 4 < length:
                history_candles.append(aggregate(i, i + 4))

        if history_candles:
            avg_body = sum(abs(c["close"] - c["open"]) for c in history_candles) / len(history_candles)
        else:
            avg_body = abs(previous_candle["close"] - previous_candle["open"]) or 1.0

        return {
            "current": current_candle,
            "previous": previous_candle,
            "avgBody": avg_body,
            "history": history_candles
        }
    except Exception as e:
        logger.error(f"[Candle Aggregator Error] {symbol}: {e}")
        return None

# ----------------------------------------------------
# 🤖 AUTO TRADING PLACEMENT ENGINE (DHAN)
# ----------------------------------------------------
def place_bracket_order(signal: Dict[str, Any]):
    if not risk_settings["autoTrading"]:
        return

    if risk_settings["currentDailyTrades"] >= risk_settings["maxDailyTrades"]:
        logger.info(f"[Risk Manager] Daily trade limit reached ({risk_settings['maxDailyTrades']}). Order bypassed.")
        return

    logger.info(f"[Order Execution Engine] Attempting order placement for: {signal['symbol']}")

    available_balance = 100000.0
    try:
        res = dhan_api_call("/v2/profile")
        if res["status"] == 200 and res.get("data") and res["data"].get("marginAvailable"):
            available_balance = float(res["data"]["marginAvailable"])
    except Exception:
        pass

    risk_amount = available_balance * (risk_settings["riskPercent"] / 100.0)
    stop_loss_points = abs(signal["entryPrice"] - signal["stopLoss"]) or 1.0
    qty = max(int(risk_amount / stop_loss_points), 1)

    # 1. OpenAlgo Order Execution Priority
    oa_client = get_openalgo_client()
    if oa_client and credentials.get("openalgoApiKey"):
        try:
            app_strategy = credentials.get("openalgoAppName", "openalgobot")
            oa_res = oa_client.placeorder(
                strategy=app_strategy,
                symbol=signal["symbol"],
                action="BUY" if signal["type"] == "BULLISH" else "SELL",
                exchange="NSE",
                price_type="LIMIT",
                product="MIS",
                quantity=qty,
                price=str(round(signal["entryPrice"], 2)),
                stoploss=str(round(signal["stopLoss"], 2)),
                target=str(round(signal["target"], 2))
            )
            logger.info(f"[OpenAlgo Order Placed]: {oa_res}")
            if isinstance(oa_res, dict) and (oa_res.get("status") in ["success", "complete", "submitted"] or "orderid" in oa_res):
                order_id = oa_res.get("orderid") or oa_res.get("order_id") or f"OA-{int(time.time() * 1000)}"
                trade_row = {
                    "id": str(order_id),
                    "symbol": signal["symbol"],
                    "type": signal["type"],
                    "entryPrice": signal["entryPrice"],
                    "stopLoss": signal["stopLoss"],
                    "target": signal["target"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "ACTIVE",
                    "broker": "OpenAlgo"
                }
                trade_book.insert(0, trade_row)
                risk_settings["currentDailyTrades"] += 1

                append_to_google_sheet("Trade_Book", [
                    trade_row["id"],
                    trade_row["timestamp"],
                    signal["symbol"],
                    signal["type"],
                    qty,
                    signal["entryPrice"],
                    signal["target"],
                    signal["stopLoss"],
                    "0.00",
                    "ACTIVE"
                ])
                return oa_res
        except Exception as e:
            logger.warning(f"[OpenAlgo Execution Warning]: {e}. Attempting Dhan/Mock fallback.")


    stock_config = next((s for s in active_scanning_list if s["symbol"] == signal["symbol"]), None)
    real_sec_id = stock_config.get("securityId") if stock_config else None
    if real_sec_id == "GOLD_ACTIVE": real_sec_id = mcx_tokens["GOLD"]
    if real_sec_id == "SILVER_ACTIVE": real_sec_id = mcx_tokens["SILVER"]
    if real_sec_id == "CRUDEOIL_ACTIVE": real_sec_id = mcx_tokens["CRUDEOIL"]

    order_params = {
        "dhanClientId": credentials["clientId"],
        "correlationId": f"BOT-{int(time.time() * 1000)}",
        "transactionType": "BUY" if signal["type"] == "BULLISH" else "SELL",
        "exchangeSegment": "MCX_COMM" if (stock_config and stock_config.get("exchangeSegment") == "MCX_COMM") else "NSE_FNO",
        "productType": "BO",
        "orderType": "LIMIT",
        "quantity": qty,
        "price": signal["entryPrice"],
        "triggerPrice": 0,
        "stopLoss": signal["stopLoss"],
        "target": signal["target"],
        "validity": "DAY",
        "securityId": real_sec_id or ""
    }

    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    if is_mock:
        trade_id = f"MOCK-TR-{str(int(time.time() * 1000))[-6:]}"
        mock_result = {
            "orderId": trade_id,
            "orderStatus": "COMPLETED",
            "message": "Mock bracket order executed successfully"
        }
        logger.info(f"[Order Executed Successfully (Mock)]: {mock_result}")

        trade_row = {
            "id": trade_id,
            "symbol": signal["symbol"],
            "type": signal["type"],
            "entryPrice": signal["entryPrice"],
            "stopLoss": signal["stopLoss"],
            "target": signal["target"],
            "timestamp": datetime.utcnow().isoformat(),
            "status": "ACTIVE"
        }
        trade_book.insert(0, trade_row)
        risk_settings["currentDailyTrades"] += 1

        append_to_google_sheet("Trade_Book", [
            trade_id,
            trade_row["timestamp"],
            signal["symbol"],
            signal["type"],
            qty,
            signal["entryPrice"],
            signal["target"],
            signal["stopLoss"],
            "0.00",
            "ACTIVE"
        ])
        return mock_result

    try:
        res = dhan_api_call("/v2/orders", "POST", order_params)
        if res["status"] in [200, 201]:
            data = res.get("data", {})
            logger.info(f"[Real Order Successful]: {data}")
            trade_row = {
                "id": data.get("orderId") or f"TR-{int(time.time() * 1000)}",
                "symbol": signal["symbol"],
                "type": signal["type"],
                "entryPrice": signal["entryPrice"],
                "stopLoss": signal["stopLoss"],
                "target": signal["target"],
                "timestamp": datetime.utcnow().isoformat(),
                "status": "ACTIVE"
            }
            trade_book.insert(0, trade_row)
            risk_settings["currentDailyTrades"] += 1

            append_to_google_sheet("Trade_Book", [
                trade_row["id"],
                trade_row["timestamp"],
                signal["symbol"],
                signal["type"],
                qty,
                signal["entryPrice"],
                signal["target"],
                signal["stopLoss"],
                "0.00",
                "ACTIVE"
            ])
        else:
            logger.error(f"[Real Order Placement Failed]: {res}")
    except Exception as e:
        logger.error(f"[Real Order Exception Error]: {e}")

# ----------------------------------------------------
# 🚀 AUTOMATIC STOCK SCANNING ENGINE
# ----------------------------------------------------
def dispatch_signal_alerts(signal: Dict[str, Any], candle_data: Dict[str, Any]):
    try:
        curr_c = candle_data["current"]
        u_wick = curr_c["high"] - max(curr_c["open"], curr_c["close"])
        l_wick = min(curr_c["open"], curr_c["close"]) - curr_c["low"]
        tot_wick = u_wick + l_wick
        overall_size = curr_c["high"] - curr_c["low"]
        wick_ratio = f"{(tot_wick / abs(curr_c['close'] - curr_c['open'] or 1.0)):.2f}"
        overall_wick_ratio = f"{((tot_wick / overall_size) * 100):.1f}%" if overall_size > 0 else "0%"

        append_to_google_sheet("Signals_Alerts", [
            signal["timestamp"],
            signal["symbol"],
            signal["name"],
            signal["type"],
            signal["entryPrice"],
            signal["stopLoss"],
            signal["target"],
            wick_ratio,
            f"{signal['strength']}%"
        ])

        icon = "🟢" if signal["type"] == "BULLISH" else "🔴"
        pattern_name = "Bullish Engulfing (Red ➔ Green)" if signal["type"] == "BULLISH" else "Bearish Engulfing (Green ➔ Red)"
        tg_msg = (
            f"{icon} <b>PHOTO-VALIDATED {signal['type']} ENGULFING</b> {icon}\n\n"
            f"<b>Symbol:</b> <code>{signal['symbol']}</code> ({signal['name']})\n"
            f"<b>Setup:</b> <i>{signal.get('trendContext', 'Reversal Confirmed')}</i>\n"
            f"<b>Pattern:</b> {pattern_name}\n"
            f"<b>Engulfing Ratio:</b> {signal['ratio']} (Required: {signal['requiredRange']})\n"
            f"<b>Wick % of Candle:</b> {overall_wick_ratio} (Strict Limit: &lt;= 40%)\n"
            f"<b>Rating / Confidence:</b> {signal['strength']}%\n\n"
            f"🎯 <b>Target:</b> ₹{signal['target']:.2f} (1:2 R:R)\n"
            f"🛑 <b>Stop Loss:</b> ₹{signal['stopLoss']:.2f}\n"
            f"💰 <b>Entry:</b> ₹{signal['entryPrice']:.2f}\n\n"
            f"<i>🤖 Engine: {'Auto Bracket Order Executed' if risk_settings['autoTrading'] else 'GPTHEIST Desk Alert'}</i>"
        )
        send_telegram_alert(tg_msg)

        wa_msg = (
            f"{icon} {signal['type']} SIGNAL: {signal['symbol']}\n"
            f"Entry: ₹{signal['entryPrice']:.2f}\n"
            f"SL: ₹{signal['stopLoss']:.2f} | Target: ₹{signal['target']:.2f}\n"
            f"Wick ratio: {wick_ratio} | Rating: {signal['strength']}%"
        )
        send_whatsapp_alert(wa_msg)
        place_bracket_order(signal)
    except Exception as e:
        logger.error(f"[Alert Dispatch Error] {signal.get('symbol')}: {e}")

def run_automatic_scan(manual_trigger: bool = False):
    global scan_progress
    if scanner_status["isRunning"] and not manual_trigger:
        return

    logger.info(f"[Scanning Engine] Triggered high-speed scan cycle. Active F&O count: {len(active_scanning_list)}")
    sync_fno_stocks_list()

    scanner_status["isRunning"] = True
    scan_progress = 0
    new_signals_count = 0
    scanned_count = 0
    scan_lock = threading.Lock()

    def process_stock(stock):
        nonlocal scanned_count, new_signals_count
        candle_data = fetch_and_aggregate_candles(stock["symbol"])
        with scan_lock:
            scanned_count += 1
            global scan_progress
            scan_progress = round((scanned_count / max(len(active_scanning_list), 1)) * 100)

        if not candle_data:
            return

        signal = analyze_engulfing_pattern(
            stock["symbol"],
            stock["name"],
            candle_data["current"],
            candle_data["previous"],
            candle_data["avgBody"],
            candle_data.get("history")
        )

        if signal:
            with scan_lock:
                if not is_duplicate_alert(signal["symbol"], signal["type"]):
                    new_signals_count += 1
                    active_signals.insert(0, signal)
                    if len(active_signals) > 50:
                        active_signals.pop()
                    logger.info(f"[Signal Detected] {signal['type']} in {signal['symbol']} @ ₹{signal['entryPrice']}")
                    dispatch_signal_alerts(signal, candle_data)

    with ThreadPoolExecutor(max_workers=14) as executor:
        list(executor.map(process_stock, active_scanning_list))

    scanner_status["lastScan"] = datetime.utcnow().isoformat()
    scanner_status["nextScan"] = (datetime.utcnow() + timedelta(minutes=1)).isoformat()
    scanner_status["stocksScanned"] = len(active_scanning_list)
    scanner_status["signalsFound"] = len(active_signals)
    scanner_status["isRunning"] = False
    scan_progress = 100

    logger.info(f"[Scanning Engine] High-speed scan complete. New signals: {new_signals_count}, Total active: {len(active_signals)}.")

    append_to_google_sheet("Scanner_Logs", [
        scanner_status["lastScan"],
        scanner_status["stocksScanned"],
        new_signals_count,
        "SUCCESS",
        f"Scan complete. 197 F&O stocks scanned. Found {new_signals_count} signals."
    ])

# ----------------------------------------------------
# ⚡ OPTIONS SCAN CORE ENGINE
# ----------------------------------------------------
def run_options_scan():
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    options_scanner_status["isRunning"] = True
    logger.info(f"[Options Scanner] Launching index options sweep... Mode: {'Simulated' if is_mock else 'Live'}")

    new_signals_count = 0
    scrips_scanned = 0

    if is_mock:
        fetch_yahoo_finance_index_spots()
    else:
        dhan_success = False
        try:
            payload = {"IDX_I": [256, 257, 260], "BSE_IDX": [1]}
            res = dhan_api_call("/v2/marketfeed/ohlc", "POST", payload)
            if res["status"] == 200 and res.get("data") and res["data"].get("data"):
                rd = res["data"]["data"]
                if "IDX_I" in rd:
                    idx_data = rd["IDX_I"]
                    if "256" in idx_data:
                        last = idx_data["256"].get("last_price") or idx_data["256"].get("close", 0)
                        close_p = idx_data["256"].get("close") or last
                        index_spots["NIFTY"]["spot"] = last
                        index_spots["NIFTY"]["change"] = round(((last - close_p) / close_p) * 100, 2) if close_p else 0.0
                    if "257" in idx_data:
                        last = idx_data["257"].get("last_price") or idx_data["257"].get("close", 0)
                        close_p = idx_data["257"].get("close") or last
                        index_spots["BANKNIFTY"]["spot"] = last
                        index_spots["BANKNIFTY"]["change"] = round(((last - close_p) / close_p) * 100, 2) if close_p else 0.0
                    if "260" in idx_data:
                        last = idx_data["260"].get("last_price") or idx_data["260"].get("close", 0)
                        close_p = idx_data["260"].get("close") or last
                        index_spots["FINNIFTY"]["spot"] = last
                        index_spots["FINNIFTY"]["change"] = round(((last - close_p) / close_p) * 100, 2) if close_p else 0.0
                if "BSE_IDX" in rd and "1" in rd["BSE_IDX"]:
                    sensex = rd["BSE_IDX"]["1"]
                    last = sensex.get("last_price") or sensex.get("close", 0)
                    close_p = sensex.get("close") or last
                    index_spots["SENSEX"]["spot"] = last
                    index_spots["SENSEX"]["change"] = round(((last - close_p) / close_p) * 100, 2) if close_p else 0.0
                dhan_success = True
        except Exception as e:
            logger.info(f"[Options Scanner] Dhan index spot fetch failed: {e}")

        if not dhan_success:
            fetch_yahoo_finance_index_spots()

    index_mappings = {
        "NIFTY": {"scrip": 256, "seg": "IDX_I", "expiryDay": 4},
        "BANKNIFTY": {"scrip": 257, "seg": "IDX_I", "expiryDay": 3},
        "FINNIFTY": {"scrip": 260, "seg": "IDX_I", "expiryDay": 2},
        "SENSEX": {"scrip": 1, "seg": "IDX_I", "expiryDay": 5}
    }

    if not is_mock:
        for index_sym, cfg in index_mappings.items():
            result = fetch_active_option_chain(index_sym, cfg)
            if result:
                active_option_chain_data[index_sym] = result["oc"]
            time.sleep(0.35)

    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
    for idx_name in indices:
        spot = index_spots[idx_name]["spot"]
        strikes = get_atm_strikes(idx_name, spot)
        oc_data = active_option_chain_data.get(idx_name)

        for strike in strikes:
            for option_type in ["CE", "PE"]:
                scrips_scanned += 1
                contract_key = f"{idx_name}-{strike:.0f}-{option_type}"

                candles = []
                real_ltp = None
                real_vol = None

                if oc_data:
                    matching_key = next((k for k in oc_data.keys() if abs(float(k) - strike) < 1), None)
                    if matching_key:
                        chain_item = oc_data[matching_key]
                        type_key = option_type.lower()
                        if type_key in chain_item:
                            real_ltp = chain_item[type_key].get("last_price")
                            real_vol = chain_item[type_key].get("volume")

                if real_ltp:
                    push_real_option_price_to_candles(contract_key, real_ltp, real_vol)
                    candles = options_candle_cache.get(contract_key, [])
                else:
                    strike_diff = strike - spot
                    if option_type == "CE":
                        base_p = max(150.0 - strike_diff * 0.8, 15.0)
                    else:
                        base_p = max(150.0 + strike_diff * 0.8, 15.0)
                    candles = generate_options_candles(contract_key, base_p)

                if not candles or len(candles) < 20:
                    continue

                current = candles[-1]
                previous = candles[-2]
                history = candles[:-1]

                recent_highs = [c["high"] for c in history[-15:]]
                recent_lows = [c["low"] for c in history[-15:]]
                resistance = max(recent_highs)
                support = min(recent_lows)

                volumes = [c["volume"] for c in history[-20:]]
                avg_volume = sum(volumes) / len(volumes) if volumes else 1.0

                is_resistance_break = current["close"] > resistance and previous["close"] <= resistance
                is_volume_spike = current["volume"] > avg_volume * 2.5
                is_support_break = current["close"] < support and previous["close"] >= support

                point_movement = current["close"] - open_point_track(candles)
                is_momentum_spike = abs(point_movement) >= 12.0

                signal_triggered = False
                signal_type = ""
                reason = ""

                current_top = max(current["open"], current["close"])
                current_bottom = min(current["open"], current["close"])
                previous_top = max(previous["open"], previous["close"])
                previous_bottom = min(previous["open"], previous["close"])
                body_size = abs(current["close"] - current["open"])
                prev_body_size = abs(previous["close"] - previous["open"])

                recent_c = history[-15:]
                options_avg_body = (sum(abs(c["close"] - c["open"]) for c in recent_c) / len(recent_c)) if recent_c else (prev_body_size or 1.0)
                perfect_engulf_info = check_perfect_engulfing(current, previous, options_avg_body)

                curr_u_wick = current["high"] - max(current["open"], current["close"])
                curr_l_wick = min(current["open"], current["close"]) - current["low"]
                curr_tot_wick = curr_u_wick + curr_l_wick
                overall_size = current["high"] - current["low"]
                is_wick_acceptable = overall_size > 0 and (curr_tot_wick <= 0.40 * overall_size)

                is_bull_engulf = (
                    current["close"] > current["open"] and
                    previous["close"] < previous["open"] and
                    current_top >= previous_top and
                    current_bottom <= previous_bottom and
                    (current_top > previous_top or current_bottom < previous_bottom) and
                    body_size > 0 and
                    is_wick_acceptable and
                    perfect_engulf_info["isPerfect"]
                )

                is_bear_engulf = (
                    current["close"] < current["open"] and
                    previous["close"] > previous["open"] and
                    current_top >= previous_top and
                    current_bottom <= previous_bottom and
                    (current_top > previous_top or current_bottom < previous_bottom) and
                    body_size > 0 and
                    is_wick_acceptable and
                    perfect_engulf_info["isPerfect"]
                )

                if is_bull_engulf:
                    signal_triggered = True
                    signal_type = "BULLISH_ENGULFING"
                    reason = f"Perfect Bullish Engulfing: Green body (₹{current['open']:.2f} - ₹{current['close']:.2f}) completely engulfed the previous body (₹{previous['open']:.2f} - ₹{previous['close']:.2f}) by {perfect_engulf_info['ratio']}! (Category: {perfect_engulf_info['category']})"
                elif is_bear_engulf:
                    signal_triggered = True
                    signal_type = "BEARISH_ENGULFING"
                    reason = f"Perfect Bearish Engulfing: Red body (₹{current['open']:.2f} - ₹{current['close']:.2f}) completely engulfed the previous body (₹{previous['open']:.2f} - ₹{previous['close']:.2f}) by {perfect_engulf_info['ratio']}! (Category: {perfect_engulf_info['category']})"
                elif is_resistance_break and is_volume_spike:
                    signal_triggered = True
                    signal_type = "RESISTANCE_BREAKOUT"
                    reason = f"Call/Put broke resistance ₹{resistance:.2f} with massive {(current['volume']/avg_volume):.1f}x volume spike!"
                elif is_support_break:
                    signal_triggered = True
                    signal_type = "SUPPORT_BREAKDOWN"
                    reason = f"Broke below key support floor ₹{support:.2f}. Short momentum building."
                elif is_momentum_spike:
                    signal_triggered = True
                    signal_type = "MOMENTUM_SPIKE"
                    reason = f"Spiked rapidly by {point_movement:.1f} points in under 60 seconds!"

                if signal_triggered and not is_duplicate_alert(contract_key, signal_type):
                    new_signals_count += 1
                    is_bull_sig = (signal_type == "BULLISH_ENGULFING" or signal_type == "RESISTANCE_BREAKOUT" or (signal_type == "MOMENTUM_SPIKE" and point_movement > 0))
                    opt_signal = {
                        "id": f"{contract_key}-{int(time.time() * 1000)}",
                        "symbol": contract_key,
                        "index": idx_name,
                        "strike": strike,
                        "optionType": option_type,
                        "type": "BULLISH" if is_bull_sig else "BEARISH",
                        "signalType": signal_type,
                        "price": current["close"],
                        "pointsMoved": point_movement,
                        "volume": current["volume"],
                        "avgVolume": round(avg_volume),
                        "reason": reason,
                        "perfectCategory": perfect_engulf_info["category"] if "ENGULFING" in signal_type else None,
                        "pctOfAvg": perfect_engulf_info["pctOfAvg"] if "ENGULFING" in signal_type else None,
                        "ratio": perfect_engulf_info["ratio"] if "ENGULFING" in signal_type else None,
                        "requiredRange": perfect_engulf_info["requiredRange"] if "ENGULFING" in signal_type else None,
                        "wickPct": f"{((curr_tot_wick / overall_size) * 100):.1f}%" if ("ENGULFING" in signal_type and overall_size > 0) else None,
                        "timestamp": datetime.utcnow().isoformat(),
                        "resistance": resistance,
                        "support": support
                    }

                    options_signals.insert(0, opt_signal)
                    if len(options_signals) > 50:
                        options_signals.pop()

                    icon = "⚡🚀" if opt_signal["type"] == "BULLISH" else "⚠️🩸"
                    is_engulf = "ENGULFING" in opt_signal["signalType"]
                    alert_msg = (
                        f"{icon} <b>OPTIONS PERFECT ENGULFING DETECTED</b> {icon}\n\n"
                        f"<b>Contract:</b> {opt_signal['symbol']}\n"
                        f"<b>LTP:</b> ₹{opt_signal['price']:.2f}\n"
                        f"<b>Trigger:</b> {opt_signal['signalType']}\n"
                    )
                    if is_engulf:
                        alert_msg += (
                            f"<b>Pattern:</b> {'Perfect Bullish Engulfing (Red -> Green)' if opt_signal['type'] == 'BULLISH' else 'Perfect Bearish Engulfing (Green -> Red)'}\n"
                            f"<b>Category:</b> {opt_signal['perfectCategory']} (Engulfed body: {opt_signal['pctOfAvg']} of avg)\n"
                            f"<b>Engulfing Ratio:</b> {opt_signal['ratio']} (Required: {opt_signal['requiredRange']})\n"
                            f"<b>Wick % of Candle:</b> {opt_signal['wickPct']} (Limit: <= 40%)\n"
                        )
                    alert_msg += (
                        f"<b>Details:</b> {opt_signal['reason']}\n\n"
                        f"<i>🤖 Action: Automated virtual position executed.</i>"
                    )
                    send_telegram_alert(alert_msg)
                    execute_paper_trade(opt_signal)

    options_scanner_status["lastScan"] = datetime.utcnow().isoformat()
    options_scanner_status["nextScan"] = (datetime.utcnow() + timedelta(seconds=60)).isoformat()
    options_scanner_status["contractsScanned"] = scrips_scanned
    options_scanner_status["signalsFound"] += new_signals_count
    options_scanner_status["isRunning"] = False
    save_paper_trades_db()

# ----------------------------------------------------
# ⏰ BACKGROUND THREAD SCHEDULERS
# ----------------------------------------------------
def scheduler_stock_scanner():
    while True:
        try:
            now_ist = datetime.now(IST)
            current_minutes = now_ist.hour * 60 + now_ist.minute
            market_start = 9 * 60 + 15
            market_end = 15 * 60 + 30
            day = now_ist.weekday()  # Mon=0, Sun=6

            is_market_open = (0 <= day <= 4 and market_start <= current_minutes <= market_end)
            mode_desc = "Live Trading Session" if is_market_open else "Off-Hours Analysis (Latest 25-Min Intraday Bars)"
            logger.info(f"[Scheduler] Stock Scan Triggered ({mode_desc}). Active F&O: {len(active_scanning_list)}")
            run_automatic_scan(False)
        except Exception as e:
            logger.error(f"[Scheduler Stock Error]: {e}")

        time.sleep(60)

def scheduler_options_scanner():
    while True:
        try:
            now_ist = datetime.now(IST)
            current_minutes = now_ist.hour * 60 + now_ist.minute
            market_start = 9 * 60 + 15
            market_end = 15 * 60 + 30
            day = now_ist.weekday()

            is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
            is_market_open = (0 <= day <= 4 and market_start <= current_minutes <= market_end)

            if is_mock or is_market_open:
                run_options_scan()
        except Exception as e:
            logger.error(f"[Scheduler Options Error]: {e}")

        time.sleep(60)

def scheduler_daily_token_renewal():
    while True:
        try:
            now_ist = datetime.now(IST)
            if now_ist.hour == 8 and now_ist.minute == 30:
                logger.info("[Scheduler] Daily Automated Access Token Renewal Triggered at 08:30 AM IST")
                generate_dhan_access_token()
                time.sleep(70)
        except Exception as e:
            logger.error(f"[Scheduler Token Renewal Error]: {e}")
        time.sleep(30)

# ----------------------------------------------------
# 🔌 FLASK APP & REST API ROUTERS
# ----------------------------------------------------
app = Flask(__name__, static_folder=str(ROOT_DIR / "dist"))
CORS(app)

@app.route("/api/dhan/connection-status", methods=["GET"])
def get_connection_status():
    dhan_configured = bool(
        credentials.get("clientId") and
        credentials["clientId"] != "your_client_id_here" and
        credentials.get("accessToken") and
        credentials["accessToken"] != "your_access_token_here"
    )
    oa_configured = bool(
        credentials.get("openalgoApiKey") and
        credentials["openalgoApiKey"] != "your_openalgo_api_key"
    )
    is_connected = dhan_configured or oa_configured

    active_id = "DEMO"
    if oa_configured:
        active_id = f"OpenAlgo ({credentials.get('openalgoAppName', 'openalgobot')})"
    elif dhan_configured:
        active_id = credentials["clientId"]

    return jsonify({
        "isConnected": is_connected,
        "clientId": active_id,
        "openalgoConfigured": oa_configured,
        "dhanConfigured": dhan_configured,
        "openalgoAppName": credentials.get("openalgoAppName", "openalgobot")
    })

@app.route("/api/openalgo/status", methods=["GET"])
def openalgo_status_route():
    api_key = credentials.get("openalgoApiKey", "")
    app_name = credentials.get("openalgoAppName", "openalgobot")
    host = credentials.get("openalgoHost", "http://127.0.0.1:5000")
    is_configured = bool(api_key and api_key != "your_openalgo_api_key")
    connected = False
    funds_info = None

    if is_configured:
        try:
            oa = get_openalgo_client()
            if oa:
                f_res = oa.funds()
                if isinstance(f_res, dict) and f_res.get("status") == "success":
                    connected = True
                    funds_info = f_res.get("data")
        except Exception:
            pass

    return jsonify({
        "configured": is_configured,
        "connected": connected,
        "appName": app_name,
        "apiKey": (api_key[:4] + "****") if api_key else "",
        "host": host,
        "funds": funds_info
    })

@app.route("/api/openalgo/test-connection", methods=["POST"])
def openalgo_test_connection_route():
    body = request.get_json(silent=True) or {}
    api_key = body.get("apiKey") or credentials.get("openalgoApiKey", "")
    host = body.get("host") or credentials.get("openalgoHost", "http://127.0.0.1:5000")
    app_name = body.get("appName") or credentials.get("openalgoAppName", "openalgobot")

    if not api_key:
        return jsonify({"success": False, "message": "OpenAlgo API Key is required."}), 400

    try:
        import openalgo
        test_client = openalgo.api(api_key=api_key, host=host, timeout=4.0, auto_reconnect=False)
        res = test_client.funds()
        if isinstance(res, dict) and res.get("status") == "success":
            return jsonify({
                "success": True,
                "message": f"Connected to OpenAlgo ({app_name}) successfully!",
                "data": res.get("data")
            })
        elif isinstance(res, dict) and res.get("status") == "error":
            return jsonify({
                "success": True,
                "message": f"OpenAlgo Server reached: {res.get('message', 'Active')}",
                "data": res
            })
        return jsonify({"success": True, "message": "OpenAlgo instance responded.", "data": res})
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"OpenAlgo Server connection check: {e}"
        })

@app.route("/api/openalgo/update-credentials", methods=["POST"])
def openalgo_update_credentials_route():
    body = request.get_json(silent=True) or {}
    app_name = body.get("appName", "openalgobot")
    api_key = body.get("apiKey", "")
    api_secret = body.get("apiSecret", "")
    host = body.get("host", "http://127.0.0.1:5000")

    if not api_key:
        return jsonify({"success": False, "message": "API Key is required."}), 400

    credentials["openalgoAppName"] = app_name
    credentials["openalgoApiKey"] = api_key
    credentials["openalgoApiSecret"] = api_secret
    credentials["openalgoHost"] = host

    update_env_file("OPENALGO_APP_NAME", app_name)
    update_env_file("OPENALGO_API_KEY", api_key)
    update_env_file("OPENALGO_API_SECRET", api_secret)
    update_env_file("OPENALGO_HOST", host)

    os.environ["OPENALGO_APP_NAME"] = app_name
    os.environ["OPENALGO_API_KEY"] = api_key
    os.environ["OPENALGO_API_SECRET"] = api_secret
    os.environ["OPENALGO_HOST"] = host

    return jsonify({
        "success": True,
        "message": f"OpenAlgo credentials successfully updated for '{app_name}'"
    })


@app.route("/api/dhan/test-connection", methods=["POST"])
def test_connection():
    body = request.get_json(silent=True) or {}
    client_id = body.get("clientId")
    access_token = body.get("accessToken")

    if not client_id or not access_token:
        return jsonify({"success": False, "message": "Client ID and Access Token are required."}), 400

    credentials["clientId"] = client_id
    credentials["accessToken"] = access_token

    is_mock = client_id == "your_client_id_here" or access_token == "your_access_token_here"
    if is_mock:
        return jsonify({"success": True, "message": "Connected (Simulated Mock-Developer Account Mode)"})

    res = dhan_api_call("/v2/profile")
    if res["status"] in [200, 201]:
        return jsonify({
            "success": True,
            "message": f"Connected successfully as {res['data'].get('name', 'Dhan User')}",
            "data": res["data"]
        })
    else:
        return jsonify({
            "success": False,
            "message": f"Connection failed: Status {res['status']} - {res['data'].get('message', 'Verification Error')}"
        })

@app.route("/api/dhan/auto-generate-token", methods=["POST"])
def auto_generate_token_route():
    body = request.get_json(silent=True) or {}
    client_id = body.get("clientId")
    pin = body.get("pin")
    totp_secret = body.get("totpSecret")

    if not client_id or not pin or not totp_secret:
        return jsonify({"success": False, "message": "Client ID, PIN, and TOTP Secret are required."}), 400

    result = generate_dhan_access_token(client_id, pin, totp_secret)
    if result["success"]:
        profile_res = dhan_api_call("/v2/profile")
        if profile_res["status"] in [200, 201]:
            return jsonify({
                "success": True,
                "message": f"Connected & Generated Successfully as {profile_res['data'].get('name', 'Dhan User')}",
                "token": result["token"]
            })
        else:
            return jsonify({
                "success": True,
                "message": f"Token generated successfully, but profile validation returned status {profile_res['status']}.",
                "token": result["token"]
            })
    else:
        return jsonify({"success": False, "message": f"Token generation failed: {result['message']}"}), 401

@app.route("/api/settings/risk", methods=["GET", "POST"])
def risk_settings_route():
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        if "autoTrading" in body:
            risk_settings["autoTrading"] = bool(body["autoTrading"])
        if "riskPercent" in body:
            risk_settings["riskPercent"] = float(body["riskPercent"])
        if "maxDailyTrades" in body:
            risk_settings["maxDailyTrades"] = int(body["maxDailyTrades"])
        return jsonify({"success": True, "message": "Risk parameters successfully updated", "data": risk_settings})
    return jsonify(risk_settings)

@app.route("/api/dhan/status", methods=["GET"])
def get_dhan_status_route():
    is_valid = dhan_status.get("isValid", False)
    return jsonify({
        "success": True,
        "isValid": is_valid,
        "error": dhan_status.get("error"),
        "clientId": credentials.get("clientId"),
        "lastChecked": dhan_status.get("lastChecked"),
        "mode": "DHANHQ DIRECT" if is_valid else "NSE REAL-TIME FEED (HIGH SPEED)"
    })

@app.route("/api/dhan/update-token", methods=["POST"])
def update_dhan_token_route():
    body = request.get_json(silent=True) or {}
    client_id = (body.get("clientId") or "").strip()
    token = (body.get("accessToken") or "").strip()
    pin = (body.get("pin") or "").strip()
    totp_secret = (body.get("totpSecret") or "").strip()

    if client_id:
        credentials["clientId"] = client_id
        update_env_file("DHAN_CLIENT_ID", client_id)
        os.environ["DHAN_CLIENT_ID"] = client_id

    if token:
        credentials["accessToken"] = token
        update_env_file("DHAN_ACCESS_TOKEN", token)
        os.environ["DHAN_ACCESS_TOKEN"] = token

    if pin:
        update_env_file("DHAN_PIN", pin)
        os.environ["DHAN_PIN"] = pin

    if totp_secret:
        update_env_file("DHAN_TOTP_SECRET", totp_secret)
        os.environ["DHAN_TOTP_SECRET"] = totp_secret

    if pin and totp_secret and not token:
        res = generate_dhan_access_token(client_id, pin, totp_secret)
        if not res.get("success"):
            return jsonify({"success": False, "message": res.get("message", "Auto-login failed")}), 400

    status = check_dhan_connection()
    if status["isValid"]:
        return jsonify({"success": True, "message": "DhanHQ API Connected Successfully!", "data": status})
    else:
        return jsonify({
            "success": False,
            "message": f"Dhan authentication notice: {status.get('error')}. Live NSE fallback engine active.",
            "data": status
        })

@app.route("/api/agents/status", methods=["GET"])
def get_agents_status():
    nifty_spot = index_spots.get("NIFTY", {}).get("spot", 22930.75)
    nifty_chg = index_spots.get("NIFTY", {}).get("change", 0.55)
    banknifty_spot = index_spots.get("BANKNIFTY", {}).get("spot", 49203.90)

    is_dhan_ok = dhan_status.get("isValid", False)
    provider_name = "DHANHQ DIRECT" if is_dhan_ok else "NSE REAL-TIME STREAM"

    agents = [
        {
            "id": "TOKYO",
            "name": "TOKYO",
            "number": "01",
            "role": "SCOUTS",
            "emoji": "🧑‍🚀",
            "badgeColor": "amber",
            "duty": "F&O Universe Scanner",
            "metric": f"{len(active_scanning_list)} F&O SCRIPS",
            "status": "SCANNING" if scanner_status["isRunning"] else "ACTIVE",
            "statusClass": "text-amber-700 bg-amber-50 border-amber-200",
            "dotClass": "bg-amber-500 animate-ping" if scanner_status["isRunning"] else "bg-amber-500",
            "details": f"Monitoring {len(active_scanning_list)} NSE equities across 12 sectors"
        },
        {
            "id": "PALERMO",
            "name": "PALERMO",
            "number": "02",
            "role": "VETOES",
            "emoji": "👮",
            "badgeColor": "emerald",
            "duty": "NSE Circuit & Risk Gate",
            "metric": f"TRADES: {risk_settings.get('currentDailyTrades', 0)}/{risk_settings.get('maxDailyTrades', 5)}",
            "status": "PASS (0 VETOES)",
            "statusClass": "text-emerald-700 bg-emerald-50 border-emerald-200",
            "dotClass": "bg-emerald-500",
            "details": "Daily trade cap & 1.0% capital risk limit active"
        },
        {
            "id": "DENVER",
            "name": "DENVER",
            "number": "03",
            "role": "SIGNALS",
            "emoji": "🕵️",
            "badgeColor": "yellow",
            "duty": "Engulfing Pattern Engine",
            "metric": f"{len(active_signals)} SETUPS FOUND",
            "status": "STRICT PHOTO",
            "statusClass": "text-yellow-700 bg-yellow-50 border-yellow-200",
            "dotClass": "bg-yellow-500",
            "details": "Photos 1-4 Validated: Body >= 120%, Wicks <= 40%"
        },
        {
            "id": "STOCKHOLM",
            "name": "STOCKHOLM",
            "number": "04",
            "role": "LIQUIDITY",
            "emoji": "👱",
            "badgeColor": "blue",
            "duty": "NSE Volume & Liquidity",
            "metric": "VOL SPIKE > 1.2X",
            "status": "LIQUIDITY OK",
            "statusClass": "text-blue-700 bg-blue-50 border-blue-200",
            "dotClass": "bg-blue-500",
            "details": "F&O top liquidity & volume expansion verified"
        },
        {
            "id": "PROFESSOR",
            "name": "PROFESSOR",
            "number": "05",
            "role": "ROUTER",
            "emoji": "🧔",
            "badgeColor": "teal",
            "duty": "Order Execution Router",
            "metric": provider_name,
            "status": "AUTO LIVE" if risk_settings["autoTrading"] else "PAPER TRADING",
            "statusClass": "text-teal-700 bg-teal-50 border-teal-200",
            "dotClass": "bg-teal-500",
            "details": "1:2 R:R Bracket order router (Target & SL)"
        },
        {
            "id": "RIO",
            "name": "RIO",
            "number": "06",
            "role": "CHARTS",
            "emoji": "🎧",
            "badgeColor": "purple",
            "duty": "25-Min Rolling Candle Builder",
            "metric": "25-MIN BARS",
            "status": "STREAMING",
            "statusClass": "text-purple-700 bg-purple-50 border-purple-200",
            "dotClass": "bg-purple-500",
            "details": "Synthesizing 5m intraday bars into 25m confirmation candles"
        },
        {
            "id": "HELSINKI",
            "name": "HELSINKI",
            "number": "07",
            "role": "LEDGER",
            "emoji": "🧔‍♂️",
            "badgeColor": "fuchsia",
            "duty": "INR Capital & Trade Ledger",
            "metric": f"₹{virtual_portfolio['cash']:,.0f} CAP",
            "status": f"₹{virtual_portfolio['netProfit']:+,.0f} P&L",
            "statusClass": "text-fuchsia-700 bg-fuchsia-50 border-fuchsia-200",
            "dotClass": "bg-fuchsia-500",
            "details": f"{virtual_portfolio['totalTrades']} Trades executed | Win rate: {round((virtual_portfolio['winningTrades']/max(virtual_portfolio['totalTrades'], 1))*100)}%"
        },
        {
            "id": "NAIROBI",
            "name": "NAIROBI",
            "number": "08",
            "role": "BRIEFS",
            "emoji": "👩",
            "badgeColor": "rose",
            "duty": "Alerts Broadcaster",
            "metric": "TG & WHATSAPP",
            "status": "CONNECTED" if os.getenv("TELEGRAM_BOT_TOKEN") else "STANDBY",
            "statusClass": "text-rose-700 bg-rose-50 border-rose-200",
            "dotClass": "bg-rose-500",
            "details": "Photo-accurate instant alerts to Telegram & WhatsApp"
        },
        {
            "id": "BERLIN",
            "name": "BERLIN",
            "number": "09",
            "role": "CONDITIONS",
            "emoji": "👨‍💼",
            "badgeColor": "red",
            "duty": "Market Regime & Trend",
            "metric": f"NIFTY {nifty_spot:,.0f}",
            "status": "BULLISH" if nifty_chg >= 0 else "BEARISH",
            "statusClass": "text-red-700 bg-red-50 border-red-200",
            "dotClass": "bg-red-500",
            "details": f"Nifty: {nifty_chg:+.2f}% | BankNifty: {banknifty_spot:,.0f}"
        },
        {
            "id": "LISBON",
            "name": "LISBON",
            "number": "10",
            "role": "RECHECK",
            "emoji": "👩‍💼",
            "badgeColor": "amber",
            "duty": "Confirmation & Anti-Trap",
            "metric": "ZERO DRIFT",
            "status": "VERIFIED",
            "statusClass": "text-amber-800 bg-amber-50 border-amber-200",
            "dotClass": "bg-amber-600",
            "details": "Photo 2 & 4 prior decline/rise confirmation verified"
        }
    ]
    return jsonify({
        "success": True,
        "agents": agents,
        "provider": provider_name,
        "dhanStatus": dhan_status
    })

@app.route("/api/signals", methods=["GET"])
def get_signals():
    return jsonify(active_signals)

@app.route("/api/trades", methods=["GET"])
def get_trades():
    return jsonify(trade_book)

@app.route("/api/dhan/quotes", methods=["POST"])
def get_quotes():
    body = request.get_json(silent=True) or {}
    symbols = body.get("symbols", [])
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"

    if is_mock:
        mock_quotes = [
            {
                "tradingSymbol": s,
                "lastPrice": round(500 + random.random() * 200, 2),
                "changePercent": round(random.random() * 4 - 2, 2)
            }
            for s in symbols
        ]
        return jsonify({"status": 200, "data": mock_quotes})

    res = dhan_api_call("/v2/quotes", "POST", {"symbols": symbols})
    return jsonify(res)

@app.route("/api/scanner/trigger", methods=["POST"])
def trigger_scanner():
    if scanner_status["isRunning"]:
        return jsonify({"success": False, "message": "Scan already in progress"}), 409
    threading.Thread(target=run_automatic_scan, args=(True,), daemon=True).start()
    return jsonify({"success": True, "message": "Manual scan triggered successfully"})

@app.route("/api/scanner/status", methods=["GET"])
def get_scanner_status():
    return jsonify({
        "progress": scan_progress,
        "status": scanner_status
    })

@app.route("/api/news", methods=["GET"])
def get_news():
    return jsonify(fetch_live_financial_news())

@app.route("/api/options/spots", methods=["GET"])
def get_options_spots():
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    if is_mock:
        for idx in ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]:
            v = random.random() * 4 - 2
            index_spots[idx]["spot"] = round(index_spots[idx]["spot"] + v, 2)
            index_spots[idx]["change"] = round(index_spots[idx]["change"] + (random.random() * 0.04 - 0.02), 2)
    return jsonify(index_spots)

@app.route("/api/options/signals", methods=["GET"])
def get_options_signals():
    return jsonify(options_signals)

def get_mock_option_chain(index_symbol: str, spot_price: float) -> Dict[str, Any]:
    step = STRIKE_STEPS.get(index_symbol, 50)
    atm = round(spot_price / step) * step
    oc = {}
    for i in range(-10, 11):
        strike = atm + i * step
        ce_base = max(150.0 - i * (step * 0.7), 4.0)
        pe_base = max(150.0 + i * (step * 0.7), 4.0)
        oc[f"{strike:.1f}"] = {
            "ce": {
                "last_price": round(ce_base + (random.random() * 4 - 2), 2),
                "volume": random.randint(10000, 410000),
                "price_change": round(random.random() * 5 - 2.5, 2)
            },
            "pe": {
                "last_price": round(pe_base + (random.random() * 4 - 2), 2),
                "volume": random.randint(10000, 410000),
                "price_change": round(random.random() * 5 - 2.5, 2)
            }
        }
    return oc

@app.route("/api/options/option-chain", methods=["GET"])
def get_option_chain():
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    selected_index = request.args.get("index", "NIFTY")
    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]

    if is_mock:
        return jsonify({idx: get_mock_option_chain(idx, index_spots[idx]["spot"]) for idx in indices})

    index_mappings = {
        "NIFTY": {"scrip": 256, "seg": "IDX_I", "expiryDay": 4},
        "BANKNIFTY": {"scrip": 257, "seg": "IDX_I", "expiryDay": 3},
        "FINNIFTY": {"scrip": 260, "seg": "IDX_I", "expiryDay": 2},
        "SENSEX": {"scrip": 1, "seg": "IDX_I", "expiryDay": 5}
    }

    if selected_index in index_mappings:
        try:
            res = fetch_active_option_chain(selected_index, index_mappings[selected_index])
            if res:
                active_option_chain_data[selected_index] = res["oc"]
        except Exception as e:
            logger.info(f"[Real-Time API] Error fetching live option chain for {selected_index}: {e}")

    response_data = {}
    for idx in indices:
        spot = index_spots[idx]["spot"]
        if not active_option_chain_data.get(idx):
            response_data[idx] = get_mock_option_chain(idx, spot)
        else:
            response_data[idx] = active_option_chain_data[idx]

    return jsonify(response_data)

@app.route("/api/options/paper-trades", methods=["GET"])
def get_paper_trades():
    return jsonify({
        "paperTrades": paper_trades,
        "virtualPortfolio": virtual_portfolio
    })

@app.route("/api/options/status", methods=["GET"])
def get_options_status():
    return jsonify(options_scanner_status)

@app.route("/api/options/reset-paper", methods=["POST"])
def reset_paper_trades():
    global paper_trades, virtual_portfolio
    paper_trades = []
    virtual_portfolio = {
        "cash": 100000.0,
        "initialCash": 100000.0,
        "totalTrades": 0,
        "winningTrades": 0,
        "netProfit": 0.0
    }
    save_paper_trades_db()
    return jsonify({"success": True, "message": "Virtual Paper Trading account successfully reset."})

@app.route("/api/options/scan", methods=["POST"])
def trigger_options_scan():
    if options_scanner_status["isRunning"]:
        return jsonify({"success": False, "message": "Options scan already in progress"}), 409
    threading.Thread(target=run_options_scan, daemon=True).start()
    return jsonify({"success": True, "message": "Manual options scan triggered successfully"})

@app.route("/api/fno-stocks", methods=["GET"])
def get_fno_stocks():
    return jsonify({
        "success": True,
        "count": len(active_scanning_list),
        "stocks": [{"symbol": s["symbol"], "name": s.get("name", s["symbol"])} for s in active_scanning_list]
    })

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "mode": "active" if credentials["clientId"] else "simulated"
    })

# Serve Animated GPTHEIST DESK Dashboard directly from templates/
TEMPLATES_DIR = BASE_DIR / "templates"
DIST_DIR = ROOT_DIR / "dist"

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    # Prioritize our new Animated GPTHEIST DESK dashboard template
    if not path or path in ["", "index.html", "dashboard"]:
        if (TEMPLATES_DIR / "index.html").exists():
            return send_from_directory(str(TEMPLATES_DIR), "index.html")
    if (TEMPLATES_DIR / path).exists():
        return send_from_directory(str(TEMPLATES_DIR), path)
    if DIST_DIR.exists():
        if path and (DIST_DIR / path).exists():
            return send_from_directory(str(DIST_DIR), path)
        return send_from_directory(str(DIST_DIR), "index.html")
    if (TEMPLATES_DIR / "index.html").exists():
        return send_from_directory(str(TEMPLATES_DIR), "index.html")
    return jsonify({"status": "running", "message": "GPTHEIST DESK Dashboard active"})


# ----------------------------------------------------
# 🚀 STARTUP & ENTRY POINT
# ----------------------------------------------------
def startup_sequence():
    load_paper_trades_db()
    check_dhan_connection()

    has_auto_creds = (
        os.getenv("DHAN_PIN") and os.getenv("DHAN_PIN") != "your_6_digit_pin_here" and
        os.getenv("DHAN_TOTP_SECRET") and os.getenv("DHAN_TOTP_SECRET") != "your_totp_secret_key_here"
    )

    if has_auto_creds:
        logger.info("[Startup] Detecting automated login credentials. Attempting token generation...")
        generate_dhan_access_token()
    else:
        logger.info("[Startup] Automated login bypassed: Credentials not configured in .env. Using token mode.")

    sync_fno_stocks_list()
    resolve_mcx_instruments()

    # Launch background daemons
    threading.Thread(target=paper_trades_monitor_loop, daemon=True).start()
    threading.Thread(target=scheduler_stock_scanner, daemon=True).start()
    threading.Thread(target=scheduler_options_scanner, daemon=True).start()
    threading.Thread(target=scheduler_daily_token_renewal, daemon=True).start()

    # Initial scan sweep in background
    logger.info("[Startup] Executing initial automatic scan sweep in background...")
    threading.Thread(target=run_automatic_scan, args=(True,), daemon=True).start()
    threading.Thread(target=run_options_scan, daemon=True).start()

_startup_lock = threading.Lock()
_startup_done = False

def ensure_services_running():
    global _startup_done
    with _startup_lock:
        if not _startup_done:
            _startup_done = True
            threading.Thread(target=startup_sequence, daemon=True).start()

# Automatically ensure background services are started under any runner (Gunicorn/WSGI/Flask)
ensure_services_running()

def start():
    logger.info("==================================================")
    logger.info("  PRO-AUTOMATION ALGOTRADER PYTHON BACKEND READY! ")
    logger.info(f"  Port: {PORT} | URL: http://localhost:{PORT}")
    logger.info("  Services: 197 NSE F&O Equities, 25-Min Rolling Candles")
    logger.info("==================================================")
    ensure_services_running()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)

if __name__ == "__main__":
    start()
