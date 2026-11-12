import os
import sys
import json
import time
import math
import random
import re
import html as html_lib
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

# No pre-seeded signals - fresh scan will detect with strict wick ≤ 20% and full body engulf
INITIAL_VERIFIED_SIGNALS = []

active_signals: List[Dict[str, Any]] = list(INITIAL_VERIFIED_SIGNALS)
trade_book: List[Dict[str, Any]] = []
scan_progress: int = 100
scan_start_time: float = 0.0
scanner_status: Dict[str, Any] = {
    "isRunning": False,
    "lastScan": datetime.utcnow().isoformat(),
    "nextScan": (datetime.utcnow() + timedelta(minutes=1)).isoformat(),
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
    "autoTrading": True,  # Activated by default for paper trading
    "riskPercent": 1.0,
    "maxDailyTrades": 10,
    "currentDailyTrades": 0,
    "maxSlippagePct": 2.0
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
    "cash": 2000000.0,
    "initialCash": 2000000.0,
    "totalTrades": 0,
    "winningTrades": 0,
    "netProfit": 0.0
}

INDEX_MAPPINGS: Dict[str, Dict[str, Any]] = {
    "NIFTY": {"scrip": 13, "seg": "IDX_I", "expiryDay": 4, "lotSize": 65, "step": 50},
    "BANKNIFTY": {"scrip": 25, "seg": "IDX_I", "expiryDay": 3, "lotSize": 30, "step": 100},
    "FINNIFTY": {"scrip": 27, "seg": "IDX_I", "expiryDay": 2, "lotSize": 60, "step": 50},
    "SENSEX": {"scrip": 51, "seg": "IDX_I", "expiryDay": 5, "lotSize": 10, "step": 100}
}
INDIA_VIX_SCRIP = 21

index_spots: Dict[str, Dict[str, Any]] = {
    "NIFTY": {"spot": 25255.05, "change": 0.0, "pointsChange": 0.0, "prevClose": 25255.05, "open": 25255.05, "high": 25255.05, "low": 25255.05},
    "BANKNIFTY": {"spot": 60792.40, "change": 0.0, "pointsChange": 0.0, "prevClose": 60792.40, "open": 60792.40, "high": 60792.40, "low": 60792.40},
    "FINNIFTY": {"spot": 27663.40, "change": 0.0, "pointsChange": 0.0, "prevClose": 27663.40, "open": 27663.40, "high": 27663.40, "low": 27663.40},
    "SENSEX": {"spot": 81879.40, "change": 0.0, "pointsChange": 0.0, "prevClose": 81879.40, "open": 81879.40, "high": 81879.40, "low": 81879.40},
    "INDIA_VIX": {"spot": 17.56, "change": 0.0, "pointsChange": 0.0, "prevClose": 17.56, "open": 17.56, "high": 17.56, "low": 17.56}
}

daily_prev_close_cache: Dict[str, float] = {}
index_technical_levels: Dict[str, Dict[str, Any]] = {}
equity_history: List[float] = [2000000.0]

momentum_snapshot: Dict[str, Any] = {
    "topGainers": [],
    "topLosers": [],
    "rvolBreakouts": [],
    "circuitNear": [],
    "scannedAt": None,
    "topLeaderText": "Waiting for live F&O sweep"
}

news_intelligence: Dict[str, Any] = {
    "sentimentScore": 0,
    "sentimentLabel": "NEUTRAL",
    "bullishCount": 0,
    "bearishCount": 0,
    "neutralCount": 0,
    "totalHeadlines": 0,
    "topHeadline": "Initializing Indian market RSS intelligence...",
    "impactSectors": [],
    "lastUpdated": None
}

index_regime: Dict[str, Any] = {
    "regimeLabel": "CALIBRATING",
    "vix": 17.56,
    "vixChange": 0.0,
    "vixZone": "MODERATE VOL",
    "niftyVsVwap": "AT VWAP",
    "pcrConsensus": 1.0,
    "expectedMove": "±0.0%",
    "lastUpdated": None
}

trap_stats: Dict[str, Any] = {
    "trapsBlocked": 0,
    "sweepsDetected": 0,
    "oiWallBlocks": 0,
    "dojiBlocked": 0,
    "lastTrapSymbol": None,
    "lastTrapReason": "No liquidity sweep traps active"
}

risk_audit_log: Dict[str, Any] = {
    "totalChecked": 0,
    "vetoCount": 0,
    "spreadVetoes": 0,
    "slippageVetoes": 0,
    "circuitAlerts": 0,
    "beTrailedCount": 0,
    "profitLockedCount": 0,
    "lastVetoReason": "All checks nominal"
}

_decision_log_lock = threading.Lock()
trade_decision_log: List[Dict[str, Any]] = []

def record_trade_decision(
    action: str,
    symbol: str,
    agent: str,
    reason: str,
    price: Optional[float] = None,
    equity_pct: Optional[float] = None,
    slippage_pct: Optional[float] = None,
    allocated_capital: Optional[float] = None,
    pnl: Optional[float] = None,
    direction: Optional[str] = None
) -> Dict[str, Any]:
    ist_now = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M:%S")
    entry: Dict[str, Any] = {
        "id": f"DEC-{int(time.time() * 1000)}-{random.randint(100, 999)}",
        "time": ist_now,
        "timestamp": datetime.utcnow().isoformat(),
        "action": str(action or "FILTERED").upper(),
        "symbol": str(symbol or "--"),
        "agent": str(agent or "SYSTEM").upper(),
        "reason": str(reason or "No reason logged"),
        "direction": direction,
        "price": round(float(price), 2) if price is not None else None,
        "equityPct": round(float(equity_pct), 1) if equity_pct is not None else None,
        "slippagePct": round(float(slippage_pct), 2) if slippage_pct is not None else None,
        "allocatedCapital": round(float(allocated_capital), 2) if allocated_capital is not None else None,
        "pnl": round(float(pnl), 2) if pnl is not None else None
    }
    with _decision_log_lock:
        trade_decision_log.insert(0, entry)
        if len(trade_decision_log) > 80:
            del trade_decision_log[80:]
    return entry

hot_strikes_board: List[Dict[str, Any]] = []

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
    {"symbol": "NIFTY", "name": "Nifty 50 Index", "sector": "INDEX", "status": "ACTIVE", "securityId": "13", "exchangeSegment": "IDX_I", "instrument": "INDEX"},
    {"symbol": "BANKNIFTY", "name": "Bank Nifty Index", "sector": "INDEX", "status": "ACTIVE", "securityId": "25", "exchangeSegment": "IDX_I", "instrument": "INDEX"},
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
_NEWS_BULL_KW = [
    "rally", "surge", "jump", "gain", "soar", "high", "record", "bull", "buy", "upgrade",
    "outperform", "profit", "beat", "order win", "contract", "rate cut", "fii buying",
    "inflow", "rebound", "breakout", "growth", "strong", "positive", "boost", "rise", "up "
]
_NEWS_BEAR_KW = [
    "fall", "drop", "slump", "crash", "plunge", "decline", "bear", "sell", "downgrade",
    "loss", "miss", "warning", "outflow", "fii selling", "inflation", "tariff", "weak",
    "negative", "pressure", "sink", "drag", "down ", "probe", "penalty", "fraud", "cut"
]
_SECTOR_KW = {
    "BANKING": ["bank", "rbi", "nbfc", "hdfc", "icici", "sbi", "axis", "kotak", "credit", "repo"],
    "IT": ["it ", "tech", "tcs", "infosys", "wipro", "hcl", "ai ", "software"],
    "AUTO": ["auto", "ev ", "maruti", "tata motors", "mahindra", "m&m", "bajaj", "tvs", "eicher"],
    "ENERGY": ["oil", "gas", "crude", "ongc", "reliance", "bpcl", "ioc", "power", "ntpc", "coal"],
    "METALS": ["steel", "metal", "copper", "aluminium", "tata steel", "hindalco", "jsw", "vedanta"],
    "PHARMA": ["pharma", "fda", "drug", "sun pharma", "cipla", "dr reddy", "lupin", "biocon"],
    "INDEX": ["nifty", "sensex", "banknifty", "vix", "f&o", "expiry", "sebi", "fii", "dii"]
}

def _score_news_headline(title: str, desc: str) -> Dict[str, Any]:
    text = f" {title} {desc} ".lower()
    bull_hits = sum(1 for kw in _NEWS_BULL_KW if kw in text)
    bear_hits = sum(1 for kw in _NEWS_BEAR_KW if kw in text)
    net = bull_hits - bear_hits
    if net > 0:
        sentiment = "BULLISH"
        score = min(100, net * 35)
    elif net < 0:
        sentiment = "BEARISH"
        score = max(-100, net * 35)
    else:
        sentiment = "NEUTRAL"
        score = 0

    sectors = [sec for sec, kws in _SECTOR_KW.items() if any(k in text for k in kws)]
    impact_sector = sectors[0] if sectors else "MARKET"

    # Match F&O symbols appearing as whole uppercase words
    upper_title = f" {title.upper()} "
    matched_syms = []
    for st in active_scanning_list:
        sym = st.get("symbol", "")
        if len(sym) >= 3 and re.search(rf"\b{re.escape(sym)}\b", upper_title):
            matched_syms.append(sym)
            if len(matched_syms) >= 3:
                break

    return {
        "sentiment": sentiment,
        "score": score,
        "impactSector": impact_sector,
        "relatedSymbols": matched_syms
    }

def fetch_live_financial_news(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    if not force and news_cache["lastFetched"] and (now - news_cache["lastFetched"]) < 120 and news_cache["data"]:
        return news_cache["data"]

    feeds = [
        ("Economic Times", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
        ("Moneycontrol", "https://www.moneycontrol.com/rss/marketreports.xml"),
        ("LiveMint", "https://www.livemint.com/rss/markets")
    ]
    items: List[Dict[str, Any]] = []
    seen_titles = set()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    item_regex = re.compile(r"<item>([\s\S]*?)</item>", re.MULTILINE)

    for source_name, rss_url in feeds:
        try:
            resp = requests.get(rss_url, headers=headers, timeout=5)
            if resp.status_code != 200:
                continue
            matches = item_regex.findall(resp.text)
            for content in matches[:6]:
                title_m = re.search(r"<title><!\[CDATA\[([\s\S]*?)\]\]></title>", content) or re.search(r"<title>([\s\S]*?)</title>", content)
                link_m = re.search(r"<link><!\[CDATA\[([\s\S]*?)\]\]></link>", content) or re.search(r"<link>([\s\S]*?)</link>", content)
                pub_date_m = re.search(r"<pubDate>([\s\S]*?)</pubDate>", content)
                desc_m = re.search(r"<description><!\[CDATA\[([\s\S]*?)\]\]></description>", content) or re.search(r"<description>([\s\S]*?)</description>", content)

                title = (title_m.group(1) if title_m else "Market Update").strip()
                title = html_lib.unescape(html_lib.unescape(re.sub(r"<[^>]*>", "", title))).replace("#39;", "'").strip()
                if not title or title.lower() in seen_titles:
                    continue
                seen_titles.add(title.lower())

                link = (link_m.group(1) if link_m else "https://economictimes.indiatimes.com").strip()
                pub_date = (pub_date_m.group(1) if pub_date_m else datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")).strip()
                desc = (desc_m.group(1) if desc_m else "").strip()
                desc = html_lib.unescape(html_lib.unescape(re.sub(r"<[^>]*>", "", desc))).replace("#39;", "'").strip()
                if len(desc) > 150:
                    desc = desc[:147] + "..."

                analysis = _score_news_headline(title, desc)
                items.append({
                    "title": title,
                    "link": link,
                    "pubDate": pub_date,
                    "description": desc,
                    "source": source_name,
                    **analysis
                })
        except Exception as e:
            logger.debug(f"[News Service] {source_name} RSS notice: {e}")

    if items:
        news_cache["data"] = items[:15]
        news_cache["lastFetched"] = now
        bull_c = sum(1 for it in news_cache["data"] if it["sentiment"] == "BULLISH")
        bear_c = sum(1 for it in news_cache["data"] if it["sentiment"] == "BEARISH")
        neut_c = len(news_cache["data"]) - bull_c - bear_c
        avg_score = round(sum(it["score"] for it in news_cache["data"]) / max(len(news_cache["data"]), 1))
        label = "BULLISH" if avg_score >= 10 else ("BEARISH" if avg_score <= -10 else "NEUTRAL")
        sector_counts: Dict[str, int] = {}
        for it in news_cache["data"]:
            sec = it.get("impactSector", "MARKET")
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        top_sectors = [k for k, _ in sorted(sector_counts.items(), key=lambda kv: -kv[1])[:3]]

        news_intelligence.update({
            "sentimentScore": avg_score,
            "sentimentLabel": label,
            "bullishCount": bull_c,
            "bearishCount": bear_c,
            "neutralCount": neut_c,
            "totalHeadlines": len(news_cache["data"]),
            "topHeadline": news_cache["data"][0]["title"],
            "impactSectors": top_sectors,
            "lastUpdated": datetime.now(IST).strftime("%H:%M:%S IST")
        })
        logger.info(f"[NAIROBI News AI] Loaded {len(news_cache['data'])} headlines | Sentiment: {label} ({avg_score:+d}) | Sectors: {top_sectors}")
    elif not news_cache["data"]:
        news_cache["data"] = [
            {
                "title": "NSE Nifty and BSE Sensex hold steady amid positive global cues",
                "link": "https://economictimes.indiatimes.com",
                "pubDate": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
                "description": "Indian benchmark indices consolidate as institutional flows stay selective.",
                "source": "Market Intelligence",
                "sentiment": "BULLISH",
                "score": 35,
                "impactSector": "INDEX",
                "relatedSymbols": []
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
            resp = requests.post(url, headers=headers, json=body, timeout=8)
        else:
            resp = requests.get(url, headers=headers, timeout=8)
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


# ----------------------------------------------------
# 🧠 INSTITUTIONAL OPTION CHAIN & MOMENTUM ANALYTICS
# ----------------------------------------------------
option_analytics: Dict[str, Dict[str, Any]] = {
    "NIFTY": {"pcr": 1.0, "coi_pcr": 1.0, "call_wall": 25400.0, "put_wall": 25000.0, "max_pain": 25250.0, "atm_straddle": 0.0, "expected_move_pct": 0.0, "hot_strike": "25250 CE", "hot_ce": "25250 CE", "hot_pe": "25250 PE", "bias": "NEUTRAL", "spot": 25255.05, "expiry": None},
    "SENSEX": {"pcr": 1.0, "coi_pcr": 1.0, "call_wall": 82500.0, "put_wall": 81000.0, "max_pain": 81900.0, "atm_straddle": 0.0, "expected_move_pct": 0.0, "hot_strike": "81900 CE", "hot_ce": "81900 CE", "hot_pe": "81900 PE", "bias": "NEUTRAL", "spot": 81879.40, "expiry": None},
    "BANKNIFTY": {"pcr": 1.0, "coi_pcr": 1.0, "call_wall": 61500.0, "put_wall": 60000.0, "max_pain": 60800.0, "atm_straddle": 0.0, "expected_move_pct": 0.0, "hot_strike": "60800 CE", "hot_ce": "60800 CE", "hot_pe": "60800 PE", "bias": "NEUTRAL", "spot": 60792.40, "expiry": None},
    "FINNIFTY": {"pcr": 1.0, "coi_pcr": 1.0, "call_wall": 28000.0, "put_wall": 27400.0, "max_pain": 27700.0, "atm_straddle": 0.0, "expected_move_pct": 0.0, "hot_strike": "27650 CE", "hot_ce": "27650 CE", "hot_pe": "27650 PE", "bias": "NEUTRAL", "spot": 27663.40, "expiry": None}
}

def _classify_oi_buildup(price_chg: float, oi_chg: float) -> str:
    if price_chg > 0 and oi_chg > 0:
        return "LONG BUILDUP"
    elif price_chg > 0 and oi_chg <= 0:
        return "SHORT COVERING"
    elif price_chg < 0 and oi_chg > 0:
        return "SHORT BUILDUP"
    elif price_chg < 0 and oi_chg <= 0:
        return "LONG UNWINDING"
    return "NEUTRAL"

def analyze_option_chain(index_symbol: str, oc_data: Dict[str, Any], spot: float, expiry: Optional[str] = None) -> Dict[str, Any]:
    global hot_strikes_board
    if not oc_data:
        return option_analytics.get(index_symbol, {"pcr": 1.0, "coi_pcr": 1.0, "call_wall": spot, "put_wall": spot, "max_pain": spot, "hot_strike": "ATM", "bias": "NEUTRAL"})

    total_ce_oi = 0
    total_pe_oi = 0
    total_ce_coi = 0
    total_pe_coi = 0
    max_ce_oi = -1
    max_pe_oi = -1
    call_wall = spot
    put_wall = spot
    strike_rows: List[Dict[str, Any]] = []

    for strike_str, data in oc_data.items():
        try:
            strike = float(strike_str)
        except (ValueError, TypeError):
            continue

        ce = data.get("ce") or {}
        pe = data.get("pe") or {}
        ce_oi = float(ce.get("oi") or 0)
        pe_oi = float(pe.get("oi") or 0)
        ce_prev_oi = float(ce.get("previous_oi") or ce_oi)
        pe_prev_oi = float(pe.get("previous_oi") or pe_oi)

        total_ce_oi += ce_oi
        total_pe_oi += pe_oi
        total_ce_coi += (ce_oi - ce_prev_oi)
        total_pe_coi += (pe_oi - pe_prev_oi)

        if ce_oi > max_ce_oi:
            max_ce_oi = ce_oi
            call_wall = strike
        if pe_oi > max_pe_oi:
            max_pe_oi = pe_oi
            put_wall = strike

        strike_rows.append({"strike": strike, "ce": ce, "pe": pe, "ce_oi": ce_oi, "pe_oi": pe_oi})

    strike_rows.sort(key=lambda r: r["strike"])
    strikes = [r["strike"] for r in strike_rows]
    atm_strike = min(strikes, key=lambda s: abs(s - spot)) if strikes else spot

    # Exact Max Pain calculation across active strikes near spot
    max_pain = atm_strike
    near_rows = [r for r in strike_rows if abs(r["strike"] - spot) <= max(spot * 0.06, 500)] or strike_rows
    if near_rows:
        min_payout = None
        for cand in near_rows:
            s_exp = cand["strike"]
            payout = 0.0
            for r in near_rows:
                k = r["strike"]
                if s_exp > k:
                    payout += (s_exp - k) * r["ce_oi"]
                elif s_exp < k:
                    payout += (k - s_exp) * r["pe_oi"]
            if min_payout is None or payout < min_payout:
                min_payout = payout
                max_pain = s_exp

    # ATM Straddle & Expected Move
    atm_row = min(strike_rows, key=lambda r: abs(r["strike"] - spot)) if strike_rows else None
    atm_ce_ltp = float((atm_row["ce"].get("last_price") or 0)) if atm_row else 0.0
    atm_pe_ltp = float((atm_row["pe"].get("last_price") or 0)) if atm_row else 0.0
    atm_straddle = round(atm_ce_ltp + atm_pe_ltp, 2)
    expected_move_pct = round((atm_straddle / spot) * 100, 2) if spot > 0 and atm_straddle > 0 else 0.0

    # PCR & Change-in-OI PCR
    pcr = round(total_pe_oi / max(total_ce_oi, 1), 2)
    if total_ce_coi > 0 and total_pe_coi > 0:
        coi_pcr = round(total_pe_coi / max(total_ce_coi, 1), 2)
    elif total_pe_coi > 0 and total_ce_coi <= 0:
        coi_pcr = 2.0
    elif total_ce_coi > 0 and total_pe_coi <= 0:
        coi_pcr = 0.5
    else:
        coi_pcr = pcr

    composite_pcr = (pcr * 0.6) + (coi_pcr * 0.4)
    bias = "BULLISH" if composite_pcr >= 1.05 else ("BEARISH" if composite_pcr <= 0.85 else "NEUTRAL")

    # Strike Momentum Hunter: score strikes within +/- 5 steps around ATM
    step = STRIKE_STEPS.get(index_symbol, 50)
    scored_contracts: List[Dict[str, Any]] = []
    for r in strike_rows:
        strike = r["strike"]
        if abs(strike - atm_strike) > step * 5:
            continue
        for opt_type in ["CE", "PE"]:
            side = r[opt_type.lower()]
            ltp = float(side.get("last_price") or 0)
            if ltp <= 1.0:
                continue
            prev_close = float(side.get("previous_close_price") or ltp)
            avg_price = float(side.get("average_price") or ltp)
            oi = float(side.get("oi") or 0)
            prev_oi = float(side.get("previous_oi") or oi)
            vol = float(side.get("volume") or 0)
            prev_vol = float(side.get("previous_volume") or vol or 1)
            greeks = side.get("greeks") or {}
            delta = abs(float(greeks.get("delta") or 0))
            gamma = float(greeks.get("gamma") or 0)
            iv = float(side.get("implied_volatility") or 0)
            bid = float(side.get("top_bid_price") or 0)
            ask = float(side.get("top_ask_price") or 0)
            spread_pct = round(((ask - bid) / ltp) * 100, 2) if (ask > bid > 0 and ltp > 0) else 0.0

            pct_chg = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0
            oi_chg = oi - prev_oi
            oi_chg_pct = round((oi_chg / max(prev_oi, 1)) * 100, 1) if prev_oi > 0 else 0.0
            vol_ratio = round(vol / max(prev_vol, 1), 2) if prev_vol > 0 else 1.0
            above_vwap = ltp >= avg_price if avg_price > 0 else False
            buildup = _classify_oi_buildup(pct_chg, oi_chg)

            # Institutional Strike Momentum Score
            mom_score = 0.0
            if buildup in ["LONG BUILDUP", "SHORT COVERING"]:
                mom_score += 25.0
            if above_vwap:
                mom_score += 18.0
            mom_score += min(30.0, max(-25.0, pct_chg * 0.8))
            if pct_chg < -10.0:
                mom_score -= 15.0
            mom_score += min(20.0, vol_ratio * 8.0)
            if 0.30 <= delta <= 0.65:
                mom_score += 15.0
            if spread_pct > 3.5:
                mom_score -= 15.0

            scored_contracts.append({
                "index": index_symbol,
                "contract": f"{index_symbol} {strike:.0f} {opt_type}",
                "strike": strike,
                "optionType": opt_type,
                "securityId": side.get("security_id"),
                "ltp": round(ltp, 2),
                "vwap": round(avg_price, 2),
                "aboveVwap": above_vwap,
                "pctChange": pct_chg,
                "oi": int(oi),
                "oiChange": int(oi_chg),
                "oiChangePct": oi_chg_pct,
                "volume": int(vol),
                "volRatio": vol_ratio,
                "delta": round(delta, 3),
                "gamma": round(gamma, 5),
                "iv": round(iv, 2),
                "spreadPct": spread_pct,
                "buildup": buildup,
                "momentumScore": round(mom_score, 1)
            })

    scored_contracts.sort(key=lambda c: -c["momentumScore"])
    ce_sorted = [c for c in scored_contracts if c["optionType"] == "CE"]
    pe_sorted = [c for c in scored_contracts if c["optionType"] == "PE"]

    best_ce = next((c for c in ce_sorted if c["pctChange"] > 0), ce_sorted[0] if ce_sorted else None)
    best_pe = next((c for c in pe_sorted if c["pctChange"] > 0), pe_sorted[0] if pe_sorted else None)

    if bias == "BEARISH" and best_pe and best_pe["pctChange"] > 0:
        hot_strike = f"{best_pe['strike']:.0f} PE ({best_pe['pctChange']:+.1f}%)"
    elif bias == "BULLISH" and best_ce and best_ce["pctChange"] > 0:
        hot_strike = f"{best_ce['strike']:.0f} CE ({best_ce['pctChange']:+.1f}%)"
    elif scored_contracts:
        top_c = scored_contracts[0]
        hot_strike = f"{top_c['strike']:.0f} {top_c['optionType']} ({top_c['pctChange']:+.1f}%)"
    else:
        hot_strike = f"{atm_strike:.0f} {'CE' if bias == 'BULLISH' else 'PE'}"

    # Update global ranked strike board (top 3 per index kept)
    other_board = [c for c in hot_strikes_board if c.get("index") != index_symbol]
    hot_strikes_board = sorted(other_board + scored_contracts[:4], key=lambda c: -c["momentumScore"])[:16]

    analytics = {
        "pcr": pcr,
        "coi_pcr": coi_pcr,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "max_pain": max_pain,
        "atm_strike": atm_strike,
        "atm_straddle": atm_straddle,
        "expected_move_pct": expected_move_pct,
        "hot_strike": hot_strike,
        "hot_ce": f"{best_ce['strike']:.0f} CE ({best_ce['pctChange']:+.1f}%)" if best_ce else f"{atm_strike:.0f} CE",
        "hot_pe": f"{best_pe['strike']:.0f} PE ({best_pe['pctChange']:+.1f}%)" if best_pe else f"{atm_strike:.0f} PE",
        "hot_ce_data": best_ce,
        "hot_pe_data": best_pe,
        "bias": bias,
        "spot": round(spot, 2),
        "expiry": expiry or resolved_expiry_dates.get(index_symbol),
        "total_ce_oi": int(total_ce_oi),
        "total_pe_oi": int(total_pe_oi),
        "updatedAt": datetime.now(IST).strftime("%H:%M:%S IST")
    }
    option_analytics[index_symbol] = analytics
    return analytics

def fetch_active_option_chain(index_symbol: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # 1. Use Dhan official Expiry List API directly (zero blind guessing)
    try:
        exp_payload = {
            "UnderlyingScrip": config["scrip"],
            "UnderlyingSeg": config["seg"]
        }
        exp_res = dhan_api_call("/v2/optionchain/expirylist", "POST", exp_payload)
        if exp_res.get("status") == 200 and exp_res.get("data") and exp_res["data"].get("data"):
            exp_list = exp_res["data"]["data"]
            if isinstance(exp_list, list) and len(exp_list) > 0:
                nearest_expiry = exp_list[0]
                resolved_expiry_dates[index_symbol] = nearest_expiry
                oc_payload = {
                    "UnderlyingScrip": config["scrip"],
                    "UnderlyingSeg": config["seg"],
                    "Expiry": nearest_expiry
                }
                oc_res = dhan_api_call("/v2/optionchain", "POST", oc_payload)
                if oc_res.get("status") == 200 and oc_res.get("data") and oc_res["data"].get("data") and oc_res["data"]["data"].get("oc"):
                    oc_payload_data = oc_res["data"]["data"]
                    oc = oc_payload_data["oc"]
                    live_spot = float(oc_payload_data.get("last_price") or index_spots.get(index_symbol, {}).get("spot", 0))
                    if live_spot > 0:
                        index_spots.setdefault(index_symbol, {})["spot"] = round(live_spot, 2)
                    analyze_option_chain(index_symbol, oc, live_spot, nearest_expiry)
                    logger.info(f"[STOCKHOLM Option Chain] {index_symbol} expiry {nearest_expiry} ({len(oc)} strikes) | PCR: {option_analytics[index_symbol]['pcr']} | MaxPain: {option_analytics[index_symbol]['max_pain']}")
                    return {"oc": oc, "expiry": nearest_expiry}
    except Exception as e:
        logger.warning(f"[Option Chain ExpiryList Notice] {index_symbol}: {e}")

    # Fallback candidate scan if expirylist rate-limited
    candidates = get_expiry_candidates()
    for candidate in candidates[:6]:
        try:
            payload = {
                "UnderlyingScrip": config["scrip"],
                "UnderlyingSeg": config["seg"],
                "Expiry": candidate
            }
            res = dhan_api_call("/v2/optionchain", "POST", payload)
            if res["status"] == 200 and res.get("data") and res["data"].get("data") and res["data"]["data"].get("oc"):
                oc_payload_data = res["data"]["data"]
                oc = oc_payload_data["oc"]
                if len(oc) > 0:
                    resolved_expiry_dates[index_symbol] = candidate
                    live_spot = float(oc_payload_data.get("last_price") or index_spots.get(index_symbol, {}).get("spot", 0))
                    analyze_option_chain(index_symbol, oc, live_spot, candidate)
                    return {"oc": oc, "expiry": candidate}
        except Exception:
            pass
        time.sleep(0.15)

    return None

PAPER_DB_PATH = BASE_DIR / "paper_trades_db.json"

def load_paper_trades_db():
    global paper_trades, virtual_portfolio, options_signals, trade_decision_log
    if PAPER_DB_PATH.exists():
        try:
            with open(PAPER_DB_PATH, "r", encoding="utf-8") as f:
                db = json.load(f)
                if "paperTrades" in db:
                    paper_trades = db["paperTrades"]
                if "virtualPortfolio" in db:
                    virtual_portfolio = db["virtualPortfolio"]
                    if virtual_portfolio.get("initialCash") != 2000000.0:
                        virtual_portfolio["cash"] = 2000000.0
                        virtual_portfolio["initialCash"] = 2000000.0
                        virtual_portfolio["totalTrades"] = 0
                        virtual_portfolio["winningTrades"] = 0
                        virtual_portfolio["netProfit"] = 0.0
                        paper_trades = []
                if "optionsSignals" in db:
                    options_signals = db["optionsSignals"]
                if isinstance(db.get("decisionAuditLog"), list):
                    with _decision_log_lock:
                        trade_decision_log = db["decisionAuditLog"][:80]
            logger.info("[Paper Trade DB] Loaded virtual portfolio successfully with ₹20,00,000 capital.")
        except Exception as e:
            logger.error(f"[Paper Trade DB] Error loading DB: {e}")

def save_paper_trades_db():
    try:
        with _decision_log_lock:
            audit_snapshot = list(trade_decision_log[:80])
        db = {
            "paperTrades": paper_trades,
            "virtualPortfolio": virtual_portfolio,
            "optionsSignals": options_signals,
            "decisionAuditLog": audit_snapshot
        }
        with open(PAPER_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        logger.error(f"[Paper Trade DB] Error saving DB: {e}")

def _enrich_trade_telemetry(trade: Dict[str, Any], equity_base: float = 2000000.0) -> Dict[str, Any]:
    item = dict(trade)
    entry_p = float(item.get("entryPrice") or 0.0)
    qty_v = int(item.get("qty") or 0)
    allocated = float(item.get("allocatedCapital") or round(entry_p * qty_v, 2))
    item["allocatedCapital"] = round(allocated, 2)
    if item.get("equityPct") is None:
        item["equityPct"] = round((allocated / equity_base) * 100.0, 1) if equity_base > 0 else 0.0
    if item.get("slippagePct") is None:
        item["slippagePct"] = 0.0
    if item.get("maxSlippageCap") is None:
        item["maxSlippageCap"] = float(risk_settings.get("maxSlippagePct", 2.0))
    if not item.get("entryReason"):
        item["entryReason"] = "Engulfing / Momentum verified setup"
    curr_p = float(item.get("exitPrice") or item.get("currentPrice") or entry_p)
    if entry_p > 0:
        pct_move = ((curr_p - entry_p) / entry_p) * 100.0
        item["livePnLPct"] = round(pct_move if item.get("type", "BULLISH") == "BULLISH" else -pct_move, 2)
    else:
        item["livePnLPct"] = 0.0
    return item

def build_pipeline_summary() -> Dict[str, Any]:
    start_cap = round(float(virtual_portfolio.get("initialCash", 2000000.0)), 2)
    realized_equity = round(start_cap + float(virtual_portfolio.get("netProfit", 0.0)), 2)
    open_pnl = round(sum(float(t.get("pnl") or 0.0) for t in paper_trades if t.get("status") == "ACTIVE"), 2)
    live_equity = round(realized_equity + open_pnl, 2)
    active_list = [_enrich_trade_telemetry(t, realized_equity) for t in paper_trades if t.get("status") == "ACTIVE"]
    closed_list = [_enrich_trade_telemetry(t, realized_equity) for t in paper_trades if t.get("status") != "ACTIVE"]
    win_cnt = sum(1 for t in closed_list if float(t.get("pnl") or 0.0) > 0)
    loss_cnt = sum(1 for t in closed_list if float(t.get("pnl") or 0.0) < 0)
    be_cnt = sum(1 for t in closed_list if float(t.get("pnl") or 0.0) == 0)
    with _decision_log_lock:
        recent_filtered = [d for d in trade_decision_log if d.get("action") == "FILTERED"][:6]
        total_decisions = len(trade_decision_log)
    return {
        "startCapital": start_cap,
        "currentEquity": realized_equity,
        "liveEquity": live_equity,
        "openPnl": open_pnl,
        "netProfit": round(float(virtual_portfolio.get("netProfit", 0.0)), 2),
        "scannedCount": int(risk_audit_log.get("totalChecked", 0)),
        "filteredCount": int(trap_stats.get("trapsBlocked", 0)) + int(risk_audit_log.get("vetoCount", 0)),
        "slippageVetoes": int(risk_audit_log.get("slippageVetoes", 0)),
        "activeTradesCount": len(active_list),
        "closedTradesCount": len(closed_list),
        "winCount": win_cnt,
        "lossCount": loss_cnt,
        "beCount": be_cnt,
        "slippageCapPct": float(risk_settings.get("maxSlippagePct", 2.0)),
        "activeTrade": active_list[0] if active_list else None,
        "lastClosedTrade": closed_list[0] if closed_list else None,
        "recentFiltered": recent_filtered,
        "totalAuditEvents": total_decisions
    }

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
        "FINNIFTY": "NIFTY_FIN_SERVICE.NS",
        "SENSEX": "^BSESN",
        "INDIA_VIX": "^INDIAVIX"
    }
    for key, ticker in tickers.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
            resp = _yf_session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [{}])[0]
                meta = result.get("meta", {})
                quotes = result.get("indicators", {}).get("quote", [{}])[0]
                raw_closes = [float(c) for c in (quotes.get("close") or []) if c is not None]
                raw_opens = [float(c) for c in (quotes.get("open") or []) if c is not None]
                raw_highs = [float(c) for c in (quotes.get("high") or []) if c is not None]
                raw_lows = [float(c) for c in (quotes.get("low") or []) if c is not None]

                ltp = meta.get("regularMarketPrice") or (raw_closes[-1] if raw_closes else None)
                # Use prior trading session close (closes[-2]) rather than 5d range start
                if len(raw_closes) >= 2:
                    prev_close = raw_closes[-2]
                else:
                    prev_close = meta.get("previousClose") or meta.get("chartPreviousClose") or ltp
                if ltp:
                    ltp_f = round(float(ltp), 2)
                    prev_f = round(float(prev_close), 2) if prev_close else ltp_f
                    pts = round(ltp_f - prev_f, 2)
                    chg = round(((ltp_f - prev_f) / prev_f) * 100, 2) if prev_f else 0.0
                    index_spots[key] = {
                        "spot": ltp_f,
                        "change": chg,
                        "pointsChange": pts,
                        "prevClose": prev_f,
                        "open": round(float(meta.get("regularMarketOpen") or (raw_opens[-1] if raw_opens else ltp_f)), 2),
                        "high": round(float(meta.get("regularMarketDayHigh") or (raw_highs[-1] if raw_highs else ltp_f)), 2),
                        "low": round(float(meta.get("regularMarketDayLow") or (raw_lows[-1] if raw_lows else ltp_f)), 2)
                    }
        except Exception as e:
            logger.debug(f"[Yahoo Index Spot Notice] {key}: {e}")

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
    signal_price = round(float(signal.get("price") or 0.0), 2)
    if signal_price <= 0:
        return None

    active_trades = [t for t in paper_trades if t.get("status") == "ACTIVE"]
    if len(active_trades) >= 5:
        logger.info(f"[Paper Trade Engine] Max active concurrent positions (5) reached. Active count: {len(active_trades)}")
        risk_audit_log["vetoCount"] += 1
        risk_audit_log["lastVetoReason"] = f"Max 5 open trades reached; skipped {signal.get('symbol')}"
        record_trade_decision("FILTERED", signal.get("symbol", "--"), "PALERMO", "Max 5 concurrent paper positions reached", price=signal_price, direction=signal.get("type"))
        return None

    # 🛡️ PROFESSOR Slippage Cap Guard (Compare signal trigger price vs live fill / half-spread)
    max_slip_cap = float(risk_settings.get("maxSlippagePct", 2.0))
    if signal.get("execPrice") is not None:
        exec_price = round(float(signal["execPrice"]), 2)
    elif float(signal.get("spreadPct") or 0.0) > 0:
        half_spread_pct = float(signal["spreadPct"]) * 0.5
        exec_price = round(signal_price * (1.0 + (half_spread_pct / 100.0)), 2)
    else:
        cached_hist = options_candle_cache.get(signal.get("symbol", ""))
        exec_price = round(float(cached_hist[-1]["close"]), 2) if cached_hist else signal_price

    slippage_pct = round(abs(exec_price - signal_price) / signal_price * 100.0, 2) if signal_price > 0 else 0.0
    if slippage_pct > max_slip_cap:
        risk_audit_log["vetoCount"] += 1
        risk_audit_log["slippageVetoes"] += 1
        risk_audit_log["lastVetoReason"] = f"Slippage {slippage_pct:.2f}% > {max_slip_cap:.1f}% cap on {signal['symbol']}"
        logger.info(f"[PROFESSOR Slippage Guard] {risk_audit_log['lastVetoReason']}")
        record_trade_decision("FILTERED", signal["symbol"], "PROFESSOR", risk_audit_log["lastVetoReason"], price=exec_price, slippage_pct=slippage_pct, direction=signal.get("type"))
        return None

    lot_sizes = {"NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60, "MIDCPNIFTY": 120, "SENSEX": 10}
    lot_size = lot_sizes.get(signal.get("index"), 50)

    # Scale position size sensibly for 20 Lakh capital (allocate up to ~5% per trade)
    current_equity = float(virtual_portfolio.get("initialCash", 2000000.0)) + float(virtual_portfolio.get("netProfit", 0.0))
    max_trade_capital = virtual_portfolio["cash"] * 0.05
    lots_to_buy = max(1, min(10, int(max_trade_capital / (exec_price * lot_size or 1.0))))
    total_cost = exec_price * (lots_to_buy * lot_size)

    while lots_to_buy > 0 and virtual_portfolio["cash"] < total_cost:
        lots_to_buy -= 1
        total_cost = exec_price * (lots_to_buy * lot_size)

    if lots_to_buy <= 0:
        logger.info(f"[Paper Trade Engine] Insufficient virtual funds (₹{virtual_portfolio['cash']:.2f}) to buy even 1 lot of {signal['symbol']} at ₹{exec_price:.2f}.")
        record_trade_decision("FILTERED", signal["symbol"], "PALERMO", f"Insufficient margin for 1 lot @ ₹{exec_price:.2f}", price=exec_price, slippage_pct=slippage_pct, direction=signal.get("type"))
        return None

    qty = lots_to_buy * lot_size
    equity_pct = round((total_cost / current_equity) * 100.0, 1) if current_equity > 0 else 0.0
    sl_percent = 0.12
    target_percent = 0.25

    stop_loss = round(exec_price * (1 - sl_percent) if signal["type"] == "BULLISH" else exec_price * (1 + sl_percent), 2)
    target = round(exec_price * (1 + target_percent) if signal["type"] == "BULLISH" else exec_price * (1 - target_percent), 2)
    initial_risk = round(abs(exec_price - stop_loss), 2)
    paper_id = f"PT-{str(int(time.time() * 1000))[-6:]}"
    entry_reason = signal.get("reason") or signal.get("trendContext") or signal.get("signalType") or "Verified Multi-Factor Trigger"

    new_trade = {
        "id": paper_id,
        "symbol": signal["symbol"],
        "index": signal["index"],
        "strike": signal["strike"],
        "optionType": signal["optionType"],
        "type": signal["type"],
        "signalPrice": signal_price,
        "entryPrice": round(exec_price, 2),
        "slippagePct": slippage_pct,
        "maxSlippageCap": max_slip_cap,
        "qty": qty,
        "lots": lots_to_buy,
        "lotSize": lot_size,
        "allocatedCapital": round(total_cost, 2),
        "equityPct": equity_pct,
        "entryReason": entry_reason,
        "stopLoss": stop_loss,
        "initialRisk": initial_risk,
        "trailStage": "INITIAL",
        "trailingActive": False,
        "target": target,
        "status": "ACTIVE",
        "entryTime": datetime.utcnow().isoformat(),
        "exitTime": None,
        "exitPrice": None,
        "exitReason": None,
        "pnl": 0.0,
        "currentPrice": round(exec_price, 2)
    }

    virtual_portfolio["cash"] -= total_cost
    paper_trades.insert(0, new_trade)
    virtual_portfolio["totalTrades"] += 1

    record_trade_decision(
        "ENTER",
        new_trade["symbol"],
        "PROFESSOR",
        entry_reason,
        price=new_trade["entryPrice"],
        equity_pct=equity_pct,
        slippage_pct=slippage_pct,
        allocated_capital=round(total_cost, 2),
        direction=new_trade["type"]
    )

    logger.info(f"[Paper Trade] Executed virtual BUY order for {signal['symbol']}. Qty: {qty} ({lots_to_buy} lots, {equity_pct}% eq), Fill: ₹{exec_price:.2f} (Slip {slippage_pct}%), Cost: ₹{total_cost:.2f}")

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
    return new_trade

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

                latest_price = round(float(history[-1]["close"]), 2)
                trade["currentPrice"] = latest_price
                diff = latest_price - trade["entryPrice"]
                trade["pnl"] = round(diff * trade["qty"] if trade["type"] == "BULLISH" else -diff * trade["qty"], 2)

                # 🛡️ Two-Stage Institutional Trailing Stop-Loss (1R Breakeven -> 1.5R Profit Lock)
                risk_amt = float(trade.get("initialRisk") or abs(trade["entryPrice"] - trade["stopLoss"]) or (trade["entryPrice"] * 0.12))
                if risk_amt > 0:
                    if trade["type"] == "BULLISH":
                        if latest_price >= (trade["entryPrice"] + risk_amt * 1.5) and trade["stopLoss"] < (trade["entryPrice"] + risk_amt * 0.5):
                            trade["stopLoss"] = round(trade["entryPrice"] + risk_amt * 0.5, 2)
                            trade["trailingActive"] = True
                            trade["trailStage"] = "PROFIT_LOCK_0.5R"
                            risk_audit_log["profitLockedCount"] += 1
                            db_changed = True
                            logger.info(f"[Trailing SL Stage-2] {trade['symbol']} locked +0.5R profit at ₹{trade['stopLoss']}!")
                        elif latest_price >= (trade["entryPrice"] + risk_amt) and trade["stopLoss"] < trade["entryPrice"]:
                            trade["stopLoss"] = trade["entryPrice"]
                            trade["trailingActive"] = True
                            trade["trailStage"] = "BREAKEVEN"
                            risk_audit_log["beTrailedCount"] += 1
                            db_changed = True
                            logger.info(f"[Trailing SL Stage-1] {trade['symbol']} trailed to BREAKEVEN (₹{trade['entryPrice']})!")
                    else:
                        if latest_price <= (trade["entryPrice"] - risk_amt * 1.5) and trade["stopLoss"] > (trade["entryPrice"] - risk_amt * 0.5):
                            trade["stopLoss"] = round(trade["entryPrice"] - risk_amt * 0.5, 2)
                            trade["trailingActive"] = True
                            trade["trailStage"] = "PROFIT_LOCK_0.5R"
                            risk_audit_log["profitLockedCount"] += 1
                            db_changed = True
                            logger.info(f"[Trailing SL Stage-2] {trade['symbol']} locked +0.5R profit at ₹{trade['stopLoss']}!")
                        elif latest_price <= (trade["entryPrice"] - risk_amt) and trade["stopLoss"] > trade["entryPrice"]:
                            trade["stopLoss"] = trade["entryPrice"]
                            trade["trailingActive"] = True
                            trade["trailStage"] = "BREAKEVEN"
                            risk_audit_log["beTrailedCount"] += 1
                            db_changed = True
                            logger.info(f"[Trailing SL Stage-1] {trade['symbol']} trailed to BREAKEVEN (₹{trade['entryPrice']})!")

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
                        exit_reason = "TRAILING PROFIT LOCK 🔒" if exit_price > trade["entryPrice"] else ("BREAKEVEN EXIT 🛡️" if exit_price == trade["entryPrice"] else "STOP LOSS TRIGGERED 🛑")
                else:
                    if latest_price <= trade["target"]:
                        exit_triggered = True
                        exit_price = trade["target"]
                        exit_reason = "TARGET MET 🎯"
                    elif latest_price >= trade["stopLoss"]:
                        exit_triggered = True
                        exit_price = trade["stopLoss"]
                        exit_reason = "TRAILING PROFIT LOCK 🔒" if exit_price < trade["entryPrice"] else ("BREAKEVEN EXIT 🛡️" if exit_price == trade["entryPrice"] else "STOP LOSS TRIGGERED 🛑")

                if exit_triggered:
                    trade_diff = exit_price - trade["entryPrice"]
                    final_pnl = round(trade_diff * trade["qty"] if trade["type"] == "BULLISH" else -trade_diff * trade["qty"], 2)
                    trade["status"] = "PROFIT" if final_pnl > 0 else ("BREAKEVEN" if final_pnl == 0 else "LOSS")
                    trade["exitPrice"] = exit_price
                    trade["exitTime"] = datetime.utcnow().isoformat()
                    trade["exitReason"] = exit_reason
                    trade["pnl"] = final_pnl

                    total_cost = trade["entryPrice"] * trade["qty"]
                    virtual_portfolio["cash"] += (total_cost + final_pnl)
                    virtual_portfolio["netProfit"] += final_pnl
                    equity_history.append(round(virtual_portfolio["initialCash"] + virtual_portfolio["netProfit"], 2))
                    if len(equity_history) > 60:
                        equity_history.pop(0)

                    if final_pnl > 0:
                        virtual_portfolio["winningTrades"] += 1

                    pnl_pct_exit = round((final_pnl / total_cost) * 100.0, 2) if total_cost > 0 else 0.0
                    record_trade_decision(
                        "EXIT",
                        trade["symbol"],
                        "HELSINKI",
                        f"{exit_reason} ({pnl_pct_exit:+.2f}%)",
                        price=exit_price,
                        equity_pct=trade.get("equityPct"),
                        slippage_pct=trade.get("slippagePct"),
                        pnl=final_pnl,
                        direction=trade.get("type")
                    )

                    logger.info(f"[Paper Trade Exit] Closed position for {trade['symbol']}. Reason: {exit_reason}, P&L: ₹{final_pnl:.2f}")

                    emoji = "🟢💰" if final_pnl >= 0 else "🔴🛑"
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
                        "instrument": parts[6],
                        "lotSize": parts[7] if len(parts) > 7 else ""
                    })
            logger.info(f"[F&O Sync Service] Loaded {len(local_fno_list)} F&O stocks from local CSV database.")
        except Exception as e:
            logger.error(f"[F&O Sync Service] Error parsing CSV: {e}")

    if not local_fno_list:
        local_fno_list = DEFAULT_FNO_STOCKS

    # Restrict strictly to F&O equity stocks (NSE_EQ segment only)
    active_scanning_list = [s for s in local_fno_list if s.get("status") == "ACTIVE" and s.get("exchangeSegment") == "NSE_EQ"]
    logger.info(f"[F&O Sync Service] Active scanning list size: {len(active_scanning_list)} F&O equity stocks.")

# Pre-load all F&O stocks immediately
sync_fno_stocks_list()


# ----------------------------------------------------
# 🔍 MCX DYNAMIC CONTRACT RESOLVER
# ----------------------------------------------------
def resolve_mcx_instruments():
    mcx_tokens["GOLD"] = mcx_tokens.get("GOLD") or "55395"
    mcx_tokens["SILVER"] = mcx_tokens.get("SILVER") or "55403"
    mcx_tokens["CRUDEOIL"] = mcx_tokens.get("CRUDEOIL") or "55515"
    logger.info(f"[Commodity Resolver] Active MCX contracts: GOLD -> {mcx_tokens['GOLD']}, SILVER -> {mcx_tokens['SILVER']}, CRUDEOIL -> {mcx_tokens['CRUDEOIL']}")

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
# 📈 TECHNICAL INDICATORS, PATTERN ENGINE & ANTI-TRAP (RIO / DENVER / LISBON)
# ----------------------------------------------------
def compute_ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return round(sum(values) / len(values), 2)
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1.0 - k)
    return round(ema, 2)

def compute_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 1)

def compute_atr(candles: List[Dict[str, float]], period: int = 14) -> float:
    if len(candles) < 2:
        return abs(candles[-1]["high"] - candles[-1]["low"]) if candles else 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    subset = trs[-period:]
    return round(sum(subset) / max(len(subset), 1), 2)

def compute_technical_indicators(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles:
        return {"rsi": 50.0, "atr": 0.0, "vwap": 0.0, "rvol": 1.0, "ema9": 0.0, "ema21": 0.0, "pdh": 0.0, "pdl": 0.0, "prevClose": 0.0}

    closes = [float(c["close"]) for c in candles]
    rsi = compute_rsi(closes, 14)
    atr = compute_atr(candles, 14)
    ema9 = compute_ema(closes, 9)
    ema21 = compute_ema(closes, 21)

    # RVOL (current candle vs previous 15 candles average volume)
    curr_vol = float(candles[-1].get("volume") or 0)
    prior_vols = [float(c.get("volume") or 0) for c in candles[-16:-1] if float(c.get("volume") or 0) > 0]
    avg_vol = (sum(prior_vols) / len(prior_vols)) if prior_vols else 0.0
    rvol = round(curr_vol / avg_vol, 2) if avg_vol > 0 else 1.0

    # Session VWAP + Previous Day High/Low using timestamps (fallback to rolling blocks)
    has_ts = all(c.get("time") for c in candles[-min(len(candles), 5):])
    pdh, pdl, prev_close = 0.0, 0.0, closes[0]
    vwap_bars = candles[-15:]

    if has_ts:
        by_day: Dict[str, List[Dict[str, Any]]] = {}
        for c in candles:
            ts = int(c["time"])
            day_key = datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d")
            by_day.setdefault(day_key, []).append(c)
        days_sorted = sorted(by_day.keys())
        if days_sorted:
            vwap_bars = by_day[days_sorted[-1]]
        if len(days_sorted) >= 2:
            prev_day_bars = by_day[days_sorted[-2]]
            pdh = round(max(b["high"] for b in prev_day_bars), 2)
            pdl = round(min(b["low"] for b in prev_day_bars), 2)
            prev_close = round(prev_day_bars[-1]["close"], 2)
    elif len(candles) >= 20:
        prev_day_bars = candles[-30:-15] if len(candles) >= 30 else candles[:-10]
        pdh = round(max(b["high"] for b in prev_day_bars), 2)
        pdl = round(min(b["low"] for b in prev_day_bars), 2)
        prev_close = round(prev_day_bars[-1]["close"], 2)

    cum_pv, cum_v = 0.0, 0.0
    for b in vwap_bars:
        tp = (float(b["high"]) + float(b["low"]) + float(b["close"])) / 3.0
        vol = float(b.get("volume") or 1.0)
        cum_pv += tp * vol
        cum_v += vol
    vwap = round(cum_pv / cum_v, 2) if cum_v > 0 else round(closes[-1], 2)

    day_chg_pct = round(((closes[-1] - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0

    return {
        "rsi": rsi,
        "atr": atr,
        "vwap": vwap,
        "rvol": rvol,
        "ema9": ema9,
        "ema21": ema21,
        "pdh": pdh,
        "pdl": pdl,
        "prevClose": prev_close,
        "dayChangePct": day_chg_pct
    }

def check_anti_trap(symbol: str, sig_type: str, current: Dict[str, float], indicators: Optional[Dict[str, Any]], record_stats: bool = True) -> Optional[str]:
    if not indicators:
        return None
    rsi = float(indicators.get("rsi", 50.0))
    atr = float(indicators.get("atr", 0.0))
    pdh = float(indicators.get("pdh", 0.0))
    pdl = float(indicators.get("pdl", 0.0))
    c_range = abs(float(current["high"]) - float(current["low"]))
    c_close = float(current["close"])

    reason = None
    # 1. Exhaustion / Low-Volatility Compression Chop Guard
    if atr > 0 and c_range < (atr * 0.35):
        reason = f"Low-ATR chop trap (Range ₹{c_range:.2f} < 0.35x ATR ₹{atr:.2f})"
        if record_stats:
            trap_stats["dojiBlocked"] += 1
    # 2. Extreme Overbought / Oversold Trap Guard
    elif sig_type == "BULLISH" and rsi > 72.0:
        reason = f"Overbought top-buy trap (RSI {rsi:.1f} > 72)"
        if record_stats:
            trap_stats["sweepsDetected"] += 1
    elif sig_type == "BEARISH" and rsi < 28.0:
        reason = f"Oversold capitulation-short trap (RSI {rsi:.1f} < 28)"
        if record_stats:
            trap_stats["sweepsDetected"] += 1
    # 3. PDH/PDL False Sweep Rejection Guard
    elif sig_type == "BULLISH" and pdh > 0 and current["high"] > pdh and c_close < pdh and (current["high"] - c_close) > c_range * 0.25:
        reason = f"PDH liquidity sweep rejection above ₹{pdh:.2f}"
        if record_stats:
            trap_stats["sweepsDetected"] += 1
    elif sig_type == "BEARISH" and pdl > 0 and current["low"] < pdl and c_close > pdl and (c_close - current["low"]) > c_range * 0.25:
        reason = f"PDL liquidity sweep rejection below ₹{pdl:.2f}"
        if record_stats:
            trap_stats["sweepsDetected"] += 1

    if reason and record_stats:
        trap_stats["trapsBlocked"] += 1
        trap_stats["lastTrapSymbol"] = symbol
        trap_stats["lastTrapReason"] = f"{symbol}: {reason}"
    return reason

def calculate_strength(body: float, total_wick: float, volume: float) -> int:
    wick_ratio = total_wick / (body if body != 0 else 1.0)
    volume_score = min(volume / 1000000.0, 10.0)
    strength = round((1 - wick_ratio) * 50 + volume_score * 5)
    return max(1, min(strength, 100))

ENGULF_MIN_RATIO = 0.80
ENGULF_MAX_RATIO = 1.20

def _evaluate_engulf_geometry(current: Dict[str, float], previous: Dict[str, float]) -> Dict[str, Any]:
    """
    Validates strict 80%-120% Perfect Engulfing body ratio with >= 80% body overlap coverage.
    Rejects weak <80% bars and extreme >120% blowout spikes.
    """
    c_open = float(current["open"])
    c_close = float(current["close"])
    p_open = float(previous["open"])
    p_close = float(previous["close"])

    c_body = abs(c_close - c_open)
    p_body = abs(p_close - p_open)
    if p_body <= 0 or c_body <= 0:
        return {"valid": False, "direction": None, "ratio": 0.0, "coverage": 0.0, "reason": "Zero body size"}

    ratio = c_body / p_body
    if ratio < (ENGULF_MIN_RATIO - 1e-6) or ratio > (ENGULF_MAX_RATIO + 1e-6):
        return {
            "valid": False,
            "direction": None,
            "ratio": ratio,
            "coverage": 0.0,
            "reason": f"Body ratio {ratio * 100:.1f}% outside 80%-120%"
        }

    curr_top = max(c_open, c_close)
    curr_bottom = min(c_open, c_close)
    prev_top = max(p_open, p_close)
    prev_bottom = min(p_open, p_close)

    overlap = max(0.0, min(curr_top, prev_top) - max(curr_bottom, prev_bottom))
    coverage = overlap / p_body
    if coverage < (ENGULF_MIN_RATIO - 1e-6):
        return {
            "valid": False,
            "direction": None,
            "ratio": ratio,
            "coverage": coverage,
            "reason": f"Body coverage {coverage * 100:.1f}% < 80%"
        }

    is_bull = (p_close < p_open and c_close > c_open and c_close >= (prev_bottom + 0.80 * p_body - 1e-6))
    is_bear = (p_close > p_open and c_close < c_open and c_close <= (prev_top - 0.80 * p_body + 1e-6))
    if not (is_bull or is_bear):
        return {"valid": False, "direction": None, "ratio": ratio, "coverage": coverage, "reason": "Opposite close < 80%"}

    return {
        "valid": True,
        "direction": "BULLISH" if is_bull else "BEARISH",
        "ratio": ratio,
        "coverage": coverage,
        "reason": "OK"
    }

def check_perfect_engulfing(current: Dict[str, float], previous: Dict[str, float], avg_body: float) -> Dict[str, Any]:
    """
    Perfect Engulfing check restricted strictly to 80% - 120% body ratio with >= 80% body overlap.
    """
    prev_body = abs(previous["close"] - previous["open"])
    curr_body = abs(current["close"] - current["open"])

    if prev_body <= 0 or curr_body <= 0:
        return {"isPerfect": False, "direction": None, "reason": "Zero body size", "category": "Incomplete", "requiredRange": "80% - 120%", "pctOfAvg": "0.0%", "ratio": "0.0%"}

    min_opt_body = max(float(previous["close"]) * 0.001, (avg_body or 1.0) * 0.25, 0.25)
    geom = _evaluate_engulf_geometry(current, previous)
    ratio = geom["ratio"]
    is_perfect = bool(geom["valid"] and prev_body >= min_opt_body and curr_body >= min_opt_body)

    category = f"Perfect Engulf ({ratio*100:.0f}%)" if is_perfect else "Incomplete"
    required_range = "80% - 120%"

    return {
        "isPerfect": is_perfect,
        "direction": geom["direction"] if is_perfect else None,
        "category": category,
        "requiredRange": required_range,
        "pctOfAvg": f"{(prev_body / (avg_body or 1.0) * 100):.1f}%",
        "ratio": f"{(ratio * 100):.1f}%"
    }

def detect_prior_trend(history: Optional[List[Dict[str, float]]] = None, previous: Optional[Dict[str, float]] = None) -> str:
    """
    Evaluates prior 3-8 candles to check if pattern follows a valid Decline or Rise.
    - Bullish Engulfing MUST follow a DECLINE (reversal from bottom).
    - Bearish Engulfing MUST follow a RISE (reversal from top).
    """
    if not history or len(history) < 2:
        return "UNKNOWN"

    subset = history[-6:]
    net_change = subset[-1]["close"] - subset[0]["open"]
    highs = [c["high"] for c in subset]
    lows = [c["low"] for c in subset]
    recent_high = max(highs)
    recent_low = min(lows)

    prev_l = previous["low"] if previous else subset[-1]["low"]
    prev_h = previous["high"] if previous else subset[-1]["high"]

    # If recent net change is negative or previous candle is at the recent swing low:
    if net_change < 0 or prev_l <= (recent_low * 1.002):
        return "DECLINE"
    # If recent net change is positive or previous candle is at the recent swing high:
    elif net_change > 0 or prev_h >= (recent_high * 0.998):
        return "RISE"

    return "SIDEWAYS"

def analyze_engulfing_pattern(
    symbol: str,
    name: str,
    current: Dict[str, float],
    previous: Dict[str, float],
    avg_body: float,
    history: Optional[List[Dict[str, float]]] = None,
    indicators: Optional[Dict[str, Any]] = None,
    record_traps: bool = True
) -> Optional[Dict[str, Any]]:
    c_open = round(float(current["open"]), 2)
    c_close = round(float(current["close"]), 2)
    c_high = round(float(current["high"]), 2)
    c_low = round(float(current["low"]), 2)

    p_open = round(float(previous["open"]), 2)
    p_close = round(float(previous["close"]), 2)

    c_body = round(abs(c_close - c_open), 2)
    p_body = round(abs(p_close - p_open), 2)
    overall_size = round(c_high - c_low, 2)

    if overall_size <= 0 or c_body <= 0 or p_body <= 0:
        return None

    # FILTER 1: Minimum Candle Body Size (Filter out micro-dojis & flat lunchtime noise)
    min_body_threshold = max(p_close * 0.0008, (avg_body or 1.0) * 0.25)
    if p_body < min_body_threshold or c_body < min_body_threshold:
        return None

    # FILTER 2: Wick size ≤ 20% of total candle size (Exact Dhan criteria)
    upper_wick = round(c_high - max(c_open, c_close), 2)
    lower_wick = round(min(c_open, c_close) - c_low, 2)
    total_wick = round(upper_wick + lower_wick, 2)
    wick_pct = round((total_wick / overall_size) * 100, 1)

    if wick_pct > 20.0:
        return None

    # Perfect Engulfing restricted strictly to 80% - 120% body ratio with >= 80% overlap coverage
    geom = _evaluate_engulf_geometry(current, previous)
    if not geom["valid"]:
        return None
    ratio = round(geom["ratio"], 2)

    # FILTER 3: STRICT PRIOR TREND & LOCATION REVERSAL CHECK
    prior_trend = detect_prior_trend(history, previous)
    sig_type = geom["direction"]

    if sig_type == "BULLISH" and prior_trend == "RISE":
        return None
    if sig_type == "BEARISH" and prior_trend == "DECLINE":
        return None

    # FILTER 4: LISBON ANTI-TRAP VERIFICATION (ATR Compression, Overbought/Oversold & PDH/PDL Sweeps)
    trap_reason = check_anti_trap(symbol, sig_type, current, indicators, record_stats=record_traps)
    if trap_reason:
        if record_traps:
            record_trade_decision("FILTERED", symbol, "LISBON", trap_reason, price=c_close, direction=sig_type)
        logger.debug(f"[LISBON Anti-Trap] Filtered {sig_type} in {symbol}: {trap_reason}")
        return None

    # FILTER 5: DENVER MULTI-FACTOR CONFLUENCE SCORE (VWAP + RVOL + RSI + ATR)
    ind = indicators or {}
    rvol = float(ind.get("rvol", 1.0))
    rsi = float(ind.get("rsi", 50.0))
    vwap = float(ind.get("vwap", c_close))
    atr = float(ind.get("atr", overall_size))
    vwap_aligned = (c_close >= vwap) if sig_type == "BULLISH" else (c_close <= vwap)

    base_strength = calculate_strength(c_body, total_wick, current.get("volume", 0))
    bonus = 0
    tags = []
    if (sig_type == "BULLISH" and prior_trend == "DECLINE") or (sig_type == "BEARISH" and prior_trend == "RISE"):
        bonus += 12
        tags.append("Trend Reversal")
    if vwap_aligned:
        bonus += 10
        tags.append("VWAP Aligned")
    if rvol >= 1.5:
        bonus += 12
        tags.append(f"RVOL {rvol:.1f}x")
    elif rvol >= 1.2:
        bonus += 6
        tags.append(f"Vol {rvol:.1f}x")
    if (sig_type == "BULLISH" and rsi <= 48) or (sig_type == "BEARISH" and rsi >= 52):
        bonus += 8
        tags.append(f"RSI {rsi:.0f}")

    strength = max(45, min(100, base_strength + bonus))
    grade = "A+" if strength >= 85 else ("A" if strength >= 70 else "B")

    if sig_type == "BULLISH":
        stop_loss = c_low
        target = round(c_close + (c_close - c_low) * 2, 2)
        trend_text = f"Grade {grade} Reversal ({', '.join(tags) or 'Support Engulfing'})"
    else:
        stop_loss = c_high
        target = round(c_close - (c_high - c_close) * 2, 2)
        trend_text = f"Grade {grade} Reversal ({', '.join(tags) or 'Resistance Engulfing'})"

    return {
        "id": f"{symbol}-{int(time.time() * 1000)}-{'bull' if sig_type == 'BULLISH' else 'bear'}",
        "symbol": symbol,
        "name": name,
        "type": sig_type,
        "strength": strength,
        "tradeGrade": grade,
        "rvol": rvol,
        "rsi": rsi,
        "atr": atr,
        "vwap": vwap,
        "vwapAligned": vwap_aligned,
        "entryPrice": c_close,
        "stopLoss": stop_loss,
        "target": target,
        "perfectCategory": f"Perfect Engulf ({ratio * 100:.0f}%)",
        "pctOfAvg": f"{(p_body / (avg_body or 1.0) * 100):.1f}%",
        "ratio": f"{(ratio * 100):.1f}%",
        "wickPct": f"{wick_pct}%",
        "requiredRange": "80% - 120% (Wick ≤ 20%)",
        "trendContext": trend_text,
        "timestamp": datetime.utcnow().isoformat(),
        "candleData": {"current": current, "previous": previous}
    }

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

_yf_session = requests.Session()
_yf_adapter = requests.adapters.HTTPAdapter(pool_connections=40, pool_maxsize=40, max_retries=1)
_yf_session.mount("https://", _yf_adapter)
_yf_session.mount("http://", _yf_adapter)

_candle_cache: Dict[str, Any] = {}
_candle_cache_lock = threading.Lock()

def _resolve_dhan_instrument(symbol: str) -> Optional[Dict[str, str]]:
    sym = symbol.upper().strip()
    if sym in INDEX_MAPPINGS:
        return {
            "securityId": str(INDEX_MAPPINGS[sym]["scrip"]),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX"
        }
    if sym in ["INDIA_VIX", "INDIAVIX", "VIX"]:
        return {
            "securityId": str(INDIA_VIX_SCRIP),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX"
        }
    stock_config = next((s for s in active_scanning_list if s["symbol"].upper() == sym), None)
    if stock_config and stock_config.get("securityId"):
        sec_id = stock_config["securityId"]
        if sec_id == "GOLD_ACTIVE": sec_id = mcx_tokens.get("GOLD", "")
        if sec_id == "SILVER_ACTIVE": sec_id = mcx_tokens.get("SILVER", "")
        if sec_id == "CRUDEOIL_ACTIVE": sec_id = mcx_tokens.get("CRUDEOIL", "")
        return {
            "securityId": str(sec_id),
            "exchangeSegment": stock_config.get("exchangeSegment", "NSE_EQ"),
            "instrument": stock_config.get("instrument", "EQUITY")
        }
    default_match = next((s for s in DEFAULT_FNO_STOCKS if s["symbol"].upper() == sym), None)
    if default_match:
        return {
            "securityId": str(default_match["securityId"]),
            "exchangeSegment": default_match.get("exchangeSegment", "NSE_EQ"),
            "instrument": default_match.get("instrument", "EQUITY")
        }
    return None

def fetch_yahoo_finance_stock_candles(symbol: str, exchange_segment: str, interval: str = "5m", yf_range: str = "5d") -> Optional[Dict[str, Any]]:
    try:
        ticker = f"{symbol}.NS"
        if exchange_segment == "IDX_I" or symbol in INDEX_MAPPINGS:
            if symbol == "NIFTY": ticker = "^NSEI"
            elif symbol == "BANKNIFTY": ticker = "^NSEBANK"
            elif symbol == "FINNIFTY": ticker = "NIFTY_FIN_SERVICE.NS"
            elif symbol == "SENSEX": ticker = "^BSESN"
            elif symbol in ["INDIA_VIX", "INDIAVIX", "VIX"]: ticker = "^INDIAVIX"
        elif exchange_segment == "MCX_COMM":
            if symbol.startswith("GOLD"): ticker = "GC=F"
            elif symbol.startswith("SILVER"): ticker = "SI=F"
            elif symbol.startswith("CRUDEOIL"): ticker = "CL=F"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={yf_range}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9"
        }
        resp = _yf_session.get(url, headers=headers, timeout=4.0)
        if resp.status_code != 200:
            return None

        json_data = resp.json()
        chart_result = json_data.get("chart", {}).get("result")
        if not chart_result or not isinstance(chart_result, list) or len(chart_result) == 0:
            return None
        result = chart_result[0]
        meta = result.get("meta", {})
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        timestamps = result.get("timestamp", [])

        opens, closes, highs, lows, volumes, valid_ts = [], [], [], [], [], []
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
                valid_ts.append(int(timestamps[i]))

        if len(opens) < 10:
            return None

        return {
            "open": opens,
            "close": closes,
            "high": highs,
            "low": lows,
            "volume": volumes,
            "timestamp": valid_ts,
            "previousClose": meta.get("chartPreviousClose") or meta.get("previousClose"),
            "source": "YAHOO"
        }
    except Exception as e:
        logger.debug(f"[Yahoo Stock Fallback Notice] {symbol}: {e}")
        return None

def fetch_candles_series(symbol: str, interval: str = "25m", lookback_days: int = 10) -> Dict[str, Any]:
    clean_sym = symbol.upper().replace(".NS", "").replace("NSE:", "").replace("BSE:", "").strip()
    int_req = interval.lower().strip()
    is_25m = int_req in ["25m", "25", "25min"]
    is_daily = int_req in ["1d", "d", "day"]

    dhan_interval_map = {"1m": "1", "5m": "5", "15m": "15", "25m": "25", "25": "25", "30m": "15", "60m": "60", "1h": "60"}
    dhan_interval = dhan_interval_map.get(int_req, "25")

    inst_cfg = _resolve_dhan_instrument(clean_sym)
    bars: List[Dict[str, Any]] = []
    source = "YAHOO"
    prev_close = None

    # 1. Primary: Official DhanHQ Historical/Intraday Data API
    if dhan_status.get("isValid", False) and inst_cfg and inst_cfg.get("securityId"):
        to_date = (datetime.now(IST).date() + timedelta(days=1)).isoformat()
        from_date = (datetime.now(IST).date() - timedelta(days=max(lookback_days, 7 if not is_daily else 180))).isoformat()
        endpoint = "/v2/charts/historical" if is_daily else "/v2/charts/intraday"
        payload = {
            "securityId": inst_cfg["securityId"],
            "exchangeSegment": inst_cfg["exchangeSegment"],
            "instrument": inst_cfg["instrument"],
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date,
            "toDate": to_date
        }
        if not is_daily:
            payload["interval"] = dhan_interval
        try:
            res = dhan_api_call(endpoint, "POST", payload)
            if res["status"] == 200 and res.get("data") and res["data"].get("close") and len(res["data"]["close"]) >= 10:
                raw = res["data"]
                opens = raw["open"]
                highs = raw["high"]
                lows = raw["low"]
                closes = raw["close"]
                vols = raw.get("volume", [0] * len(closes))
                tss = raw.get("timestamp", [])
                for i in range(len(closes)):
                    bars.append({
                        "time": int(tss[i]) if i < len(tss) else int(time.time()),
                        "open": round(float(opens[i]), 2),
                        "high": round(float(highs[i]), 2),
                        "low": round(float(lows[i]), 2),
                        "close": round(float(closes[i]), 2),
                        "volume": float(vols[i]) if i < len(vols) else 0.0
                    })
                source = "DHANHQ"
        except Exception as e:
            logger.debug(f"[Dhan Candle Notice] {clean_sym}: {e}")

    # 2. High-Speed NSE/BSE Fallback (Yahoo Finance)
    if not bars:
        seg = inst_cfg["exchangeSegment"] if inst_cfg else ("IDX_I" if clean_sym in INDEX_MAPPINGS else "NSE_EQ")
        if is_25m:
            yf_int, yf_rng = ("5m", "1mo" if lookback_days > 5 else "5d")
        else:
            yf_map = {"1m": ("1m", "1d"), "5m": ("5m", "1mo" if lookback_days > 5 else "5d"), "15m": ("15m", "1mo"), "30m": ("30m", "1mo"), "60m": ("60m", "1mo"), "1h": ("60m", "1mo"), "1d": ("1d", "6mo"), "d": ("1d", "6mo")}
            yf_int, yf_rng = yf_map.get(int_req, ("15m", "1mo"))
        yf_raw = fetch_yahoo_finance_stock_candles(clean_sym, seg, interval=yf_int, yf_range=yf_rng)
        if yf_raw:
            prev_close = yf_raw.get("previousClose")
            opens, highs, lows, closes, vols, tss = yf_raw["open"], yf_raw["high"], yf_raw["low"], yf_raw["close"], yf_raw["volume"], yf_raw["timestamp"]
            if is_25m and len(closes) >= 10:
                for i in range(0, len(closes) - 4, 5):
                    chunk_o = opens[i]
                    chunk_c = closes[i + 4]
                    chunk_h = max(highs[i:i + 5])
                    chunk_l = min(lows[i:i + 5])
                    chunk_v = sum(vols[i:i + 5])
                    bars.append({
                        "time": int(tss[i + 4]),
                        "open": round(chunk_o, 2),
                        "high": round(chunk_h, 2),
                        "low": round(chunk_l, 2),
                        "close": round(chunk_c, 2),
                        "volume": float(chunk_v)
                    })
            else:
                for i in range(len(closes)):
                    bars.append({
                        "time": int(tss[i]),
                        "open": round(opens[i], 2),
                        "high": round(highs[i], 2),
                        "low": round(lows[i], 2),
                        "close": round(closes[i], 2),
                        "volume": float(vols[i])
                    })
            source = "NSE/YAHOO"

    indicators = compute_technical_indicators(bars) if bars else {}
    if not prev_close and indicators.get("prevClose"):
        prev_close = indicators["prevClose"]

    return {
        "bars": bars,
        "indicators": indicators,
        "source": source,
        "previousClose": prev_close
    }

def fetch_and_aggregate_candles(symbol: str) -> Optional[Dict[str, Any]]:
    # 1. Fast Cache Check (90-second TTL prevents duplicate network hits during sweep)
    now_ts = time.time()
    with _candle_cache_lock:
        if symbol in _candle_cache:
            c_time, c_data = _candle_cache[symbol]
            if now_ts - c_time < 90:
                return c_data

    try:
        series = fetch_candles_series(symbol, interval="25m", lookback_days=7)
        bars = series.get("bars") or []
        if len(bars) < 6:
            return None

        current_candle = bars[-1]
        previous_candle = bars[-2]
        history_candles = bars[-15:-1]

        if history_candles:
            avg_body = sum(abs(c["close"] - c["open"]) for c in history_candles) / len(history_candles)
        else:
            avg_body = abs(previous_candle["close"] - previous_candle["open"]) or 1.0

        aggregated_result = {
            "current": current_candle,
            "previous": previous_candle,
            "avgBody": avg_body or 1.0,
            "history": history_candles,
            "indicators": series.get("indicators", {}),
            "source": series.get("source", "DHANHQ")
        }

        with _candle_cache_lock:
            _candle_cache[symbol] = (now_ts, aggregated_result)

        return aggregated_result
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

    available_balance = virtual_portfolio.get("cash", 2000000.0)
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

def _resolve_index_prev_close(idx_name: str, scrip_id: str, last_price: float) -> float:
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    cache_key = f"{idx_name}_{today_str}"
    if cache_key in daily_prev_close_cache:
        return daily_prev_close_cache[cache_key]

    try:
        from_date = (datetime.now(IST).date() - timedelta(days=10)).isoformat()
        to_date = (datetime.now(IST).date() + timedelta(days=1)).isoformat()
        res = dhan_api_call("/v2/charts/historical", "POST", {
            "securityId": str(scrip_id),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date,
            "toDate": to_date
        })
        if res.get("status") == 200 and res.get("data") and res["data"].get("close"):
            closes = res["data"]["close"]
            tss = res["data"].get("timestamp", [])
            if len(closes) >= 2:
                last_bar_date = datetime.fromtimestamp(int(tss[-1]), IST).strftime("%Y-%m-%d") if tss else ""
                if last_bar_date == today_str:
                    prev_c = float(closes[-2])
                else:
                    prev_c = float(closes[-2] if abs(float(closes[-1]) - last_price) < 0.05 else closes[-1])
                daily_prev_close_cache[cache_key] = round(prev_c, 2)
                return daily_prev_close_cache[cache_key]
    except Exception as e:
        logger.debug(f"[PrevClose Resolver] {idx_name}: {e}")

    return round(float(index_spots.get(idx_name, {}).get("prevClose") or last_price), 2)

def update_index_spots_and_regime():
    """
    BERLIN (Macro & Index Radar): Syncs NIFTY, BANKNIFTY, FINNIFTY, SENSEX + INDIA VIX
    via DhanHQ Quote API (or Yahoo fallback), computes NIFTY 25m VWAP/RSI levels,
    and updates macro regime classification.
    """
    global index_spots
    is_dhan_ok = dhan_status.get("isValid", False)
    dhan_success = False

    if is_dhan_ok:
        try:
            payload = {"IDX_I": [13, INDIA_VIX_SCRIP, 25, 27, 51]}
            res = dhan_api_call("/v2/marketfeed/quote", "POST", payload)
            if res.get("status") == 200 and res.get("data") and res["data"].get("data") and "IDX_I" in res["data"]["data"]:
                idx_data = res["data"]["data"]["IDX_I"]
                id_map = {"13": "NIFTY", str(INDIA_VIX_SCRIP): "INDIA_VIX", "25": "BANKNIFTY", "27": "FINNIFTY", "51": "SENSEX"}
                for sid, idx_name in id_map.items():
                    if sid not in idx_data:
                        continue
                    q = idx_data[sid]
                    ohlc = q.get("ohlc") or {}
                    last_p = float(q.get("last_price") or ohlc.get("close") or 0)
                    if last_p <= 0:
                        continue
                    net_chg = float(q.get("net_change") or 0)
                    dhan_close = float(ohlc.get("close") or 0)

                    if net_chg != 0:
                        prev_c = last_p - net_chg
                    elif dhan_close > 0 and abs(dhan_close - last_p) > 0.01:
                        prev_c = dhan_close
                    else:
                        prev_c = _resolve_index_prev_close(idx_name, sid, last_p)

                    pts = round(last_p - prev_c, 2) if prev_c > 0 else 0.0
                    pct = round((pts / prev_c) * 100, 2) if prev_c > 0 else 0.0
                    index_spots[idx_name] = {
                        "spot": round(last_p, 2),
                        "change": pct,
                        "pointsChange": pts,
                        "prevClose": round(prev_c, 2),
                        "open": round(float(ohlc.get("open") or last_p), 2),
                        "high": round(float(ohlc.get("high") or last_p), 2),
                        "low": round(float(ohlc.get("low") or last_p), 2)
                    }
                dhan_success = True
        except Exception as e:
            logger.debug(f"[BERLIN Index Sync Notice]: {e}")

    if not dhan_success:
        fetch_yahoo_finance_index_spots()

    # Compute NIFTY 25m VWAP/RSI levels (throttled to every 45s to stay ultra-fast)
    try:
        cached_ind = index_technical_levels.get("NIFTY") or {}
        if not cached_ind or (time.time() - float(cached_ind.get("_ts", 0))) > 45:
            nifty_series = fetch_candles_series("NIFTY", "25m", lookback_days=5)
            ind = dict(nifty_series.get("indicators") or {})
            ind["_ts"] = time.time()
            index_technical_levels["NIFTY"] = ind
    except Exception as e:
        logger.debug(f"[BERLIN NIFTY Levels Notice]: {e}")

    nifty_spot = float(index_spots.get("NIFTY", {}).get("spot", 0.0))
    nifty_chg = float(index_spots.get("NIFTY", {}).get("change", 0.0))
    vix = float(index_spots.get("INDIA_VIX", {}).get("spot", 17.56))
    vix_chg = float(index_spots.get("INDIA_VIX", {}).get("change", 0.0))

    ind = index_technical_levels.get("NIFTY") or {}
    nifty_vwap = float(ind.get("vwap") or nifty_spot)
    if nifty_spot > nifty_vwap * 1.0005:
        nifty_vs_vwap = "ABOVE VWAP"
    elif nifty_spot < nifty_vwap * 0.9995:
        nifty_vs_vwap = "BELOW VWAP"
    else:
        nifty_vs_vwap = "AT VWAP"

    pcr_vals = [
        float(option_analytics[k]["pcr"])
        for k in ["NIFTY", "SENSEX", "BANKNIFTY"]
        if option_analytics.get(k, {}).get("total_ce_oi", 0) > 0
    ]
    pcr_cons = round(sum(pcr_vals) / len(pcr_vals), 2) if pcr_vals else float(option_analytics.get("NIFTY", {}).get("pcr", 1.0))

    vix_zone = "HIGH FEAR (>20)" if vix >= 20.0 else ("ELEVATED VOL (16-20)" if vix >= 16.0 else ("CALM (<13)" if vix < 13.0 else "NORMAL VOL"))

    if nifty_chg >= 0.4 and nifty_spot >= nifty_vwap and pcr_cons >= 1.05:
        regime_label = "TRENDING BULLISH"
    elif nifty_chg <= -0.4 and nifty_spot <= nifty_vwap and pcr_cons <= 0.85:
        regime_label = "TRENDING BEARISH"
    elif vix >= 20.0:
        regime_label = "HIGH VOLATILITY CHOP"
    elif nifty_chg >= 0 and nifty_spot >= nifty_vwap:
        regime_label = "INSTITUTIONAL BULLISH"
    elif nifty_chg < 0 and nifty_spot < nifty_vwap:
        regime_label = "CAUTIOUS BEARISH"
    else:
        regime_label = "RANGE-BOUND"

    exp_move_n = float(option_analytics.get("NIFTY", {}).get("expected_move_pct", 0.0))
    index_regime.update({
        "regimeLabel": regime_label,
        "vix": round(vix, 2),
        "vixChange": round(vix_chg, 2),
        "vixZone": vix_zone,
        "niftyVsVwap": nifty_vs_vwap,
        "niftyVwap": round(nifty_vwap, 2),
        "pcrConsensus": pcr_cons,
        "expectedMove": f"±{exp_move_n:.2f}%",
        "lastUpdated": datetime.now(IST).strftime("%H:%M:%S IST")
    })

def update_momentum_snapshot_from_quotes():
    """
    TOKYO (Momentum Sniper) & PALERMO (Circuit Guard): Fast batched DhanHQ quote sweep
    across active NSE_EQ F&O scrips to refresh Top Gainers, Top Losers, VWAP Breakouts, and Circuit Warnings.
    """
    if not dhan_status.get("isValid", False):
        return

    # Preserve existing RVOL / RSI readings gathered by the 25m candle scanner
    prev_metrics: Dict[str, Dict[str, Any]] = {}
    for bucket in ["topGainers", "topLosers", "rvolBreakouts"]:
        for row in momentum_snapshot.get(bucket, []):
            prev_metrics[row["symbol"]] = row

    sec_map: Dict[str, Dict[str, Any]] = {}
    for st in active_scanning_list:
        sid = str(st.get("securityId") or "").strip()
        if sid.isdigit() and sid not in sec_map:
            sec_map[sid] = st

    if not sec_map:
        return

    sec_ids = [int(k) for k in sec_map.keys()]
    rows: List[Dict[str, Any]] = []
    circuit_near: List[Dict[str, Any]] = []

    for idx in range(0, len(sec_ids), 80):
        chunk = sec_ids[idx:idx + 80]
        try:
            res = dhan_api_call("/v2/marketfeed/quote", "POST", {"NSE_EQ": chunk})
            if res.get("status") == 200 and res.get("data") and res["data"].get("data"):
                eq_dict = res["data"]["data"].get("NSE_EQ") or {}
                for sid_str, q in eq_dict.items():
                    meta = sec_map.get(str(sid_str))
                    if not meta:
                        continue
                    ltp = float(q.get("last_price") or 0)
                    if ltp <= 0:
                        continue
                    ohlc = q.get("ohlc") or {}
                    open_p = float(ohlc.get("open") or ltp)
                    close_p = float(ohlc.get("close") or 0)
                    net_chg = float(q.get("net_change") or 0)
                    vwap = float(q.get("average_price") or ltp)
                    upper_ckt = float(q.get("upper_circuit_limit") or 0)
                    lower_ckt = float(q.get("lower_circuit_limit") or 0)
                    vol = float(q.get("volume") or 0)

                    if net_chg != 0 and (ltp - net_chg) > 0:
                        chg_pct = round((net_chg / (ltp - net_chg)) * 100, 2)
                    elif close_p > 0 and abs(close_p - ltp) > 0.01:
                        chg_pct = round(((ltp - close_p) / close_p) * 100, 2)
                    elif open_p > 0:
                        chg_pct = round(((ltp - open_p) / open_p) * 100, 2)
                    else:
                        chg_pct = 0.0

                    sym = meta["symbol"]
                    prev_m = prev_metrics.get(sym, {})
                    rec = {
                        "symbol": sym,
                        "name": meta.get("name", sym),
                        "sector": meta.get("sector", ""),
                        "ltp": round(ltp, 2),
                        "vwap": round(vwap, 2),
                        "vwapAligned": ltp >= vwap if vwap > 0 else True,
                        "dayChangePct": chg_pct,
                        "rvol": float(prev_m.get("rvol", 1.0)),
                        "rsi": float(prev_m.get("rsi", 50.0)),
                        "volume": int(vol)
                    }
                    rows.append(rec)

                    if (upper_ckt > 0 and ((upper_ckt - ltp) / upper_ckt) <= 0.015) or (lower_ckt > 0 and ((ltp - lower_ckt) / lower_ckt) <= 0.015):
                        circuit_near.append(sym)
        except Exception as e:
            logger.debug(f"[TOKYO Batch Quote Notice]: {e}")
        time.sleep(0.15)

    if rows:
        gainers = sorted(rows, key=lambda r: -r["dayChangePct"])[:10]
        losers = sorted(rows, key=lambda r: r["dayChangePct"])[:10]
        existing_rvol = [r for r in momentum_snapshot.get("rvolBreakouts", []) if r.get("rvol", 1.0) >= 1.4]
        if not existing_rvol:
            existing_rvol = sorted(rows, key=lambda r: -abs(r["dayChangePct"]))[:10]

        risk_audit_log["circuitAlerts"] = len(circuit_near)
        top_g = gainers[0]
        momentum_snapshot.update({
            "topGainers": gainers,
            "topLosers": losers,
            "rvolBreakouts": existing_rvol[:12],
            "circuitNear": circuit_near[:10],
            "scannedAt": datetime.now(IST).strftime("%H:%M:%S IST"),
            "topLeaderText": f"{top_g['symbol']} ({top_g['dayChangePct']:+.2f}%) | ₹{top_g['ltp']:.1f}"
        })
        logger.info(f"[TOKYO Momentum Pulse] Refreshed {len(rows)} live F&O quotes | Top Mover: {momentum_snapshot['topLeaderText']}")

def run_automatic_scan(manual_trigger: bool = False):
    global scan_progress, scan_start_time
    now_ts = time.time()
    if scanner_status["isRunning"] and not manual_trigger:
        if now_ts - scan_start_time < 25:
            return
        logger.warning("[Scanning Engine] Detected stalled scan cycle (>25s). Auto-recovering...")

    scan_start_time = now_ts
    logger.info(f"[Scanning Engine] Triggered high-speed scan cycle. Active F&O count: {len(active_scanning_list)}")
    sync_fno_stocks_list()
    try:
        update_index_spots_and_regime()
    except Exception:
        pass

    scanner_status["isRunning"] = True
    scan_progress = 10  # Immediate initial progress so UI displays progress
    new_signals_count = 0
    scanned_count = 0
    scan_metrics: List[Dict[str, Any]] = []
    scan_lock = threading.Lock()

    def process_stock(stock):
        nonlocal scanned_count, new_signals_count
        try:
            candle_data = fetch_and_aggregate_candles(stock["symbol"])
            if candle_data:
                ind = candle_data.get("indicators", {})
                curr = candle_data["current"]
                with scan_lock:
                    risk_audit_log["totalChecked"] += 1
                    scan_metrics.append({
                        "symbol": stock["symbol"],
                        "name": stock.get("name", stock["symbol"]),
                        "sector": stock.get("sector", ""),
                        "ltp": round(float(curr["close"]), 2),
                        "vwap": round(float(ind.get("vwap", curr["close"])), 2),
                        "vwapAligned": float(curr["close"]) >= float(ind.get("vwap", curr["close"])),
                        "dayChangePct": float(ind.get("dayChangePct", 0.0)),
                        "rvol": float(ind.get("rvol", 1.0)),
                        "rsi": float(ind.get("rsi", 50.0)),
                        "volume": int(float(curr.get("volume") or 0))
                    })

                signal = analyze_engulfing_pattern(
                    stock["symbol"],
                    stock["name"],
                    candle_data["current"],
                    candle_data["previous"],
                    candle_data["avgBody"],
                    candle_data.get("history"),
                    indicators=ind,
                    record_traps=True
                )
                if signal:
                    should_alert = False
                    with scan_lock:
                        if not is_duplicate_alert(signal["symbol"], signal["type"]):
                            new_signals_count += 1
                            active_signals.insert(0, signal)
                            if len(active_signals) > 50:
                                active_signals.pop()
                            logger.info(f"[Signal Detected] {signal['type']} ({signal.get('tradeGrade', 'A')}) in {signal['symbol']} @ ₹{signal['entryPrice']}")
                            should_alert = True
                    if should_alert:
                        threading.Thread(target=dispatch_signal_alerts, args=(signal, candle_data), daemon=True).start()
        except Exception as err:
            logger.debug(f"[Process Stock Notice] {stock.get('symbol')}: {err}")
        finally:
            with scan_lock:
                scanned_count += 1
                global scan_progress
                pct = round((scanned_count / max(len(active_scanning_list), 1)) * 100)
                scan_progress = max(10, min(99, pct))

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(process_stock, active_scanning_list))
    except Exception as e:
        logger.error(f"[Scan ThreadPool Error]: {e}")
    finally:
        if scan_metrics:
            gainers = sorted(scan_metrics, key=lambda r: -r["dayChangePct"])[:10]
            losers = sorted(scan_metrics, key=lambda r: r["dayChangePct"])[:10]
            rvol_leaders = sorted([r for r in scan_metrics if r["rvol"] >= 1.4], key=lambda r: -r["rvol"])
            if not rvol_leaders:
                rvol_leaders = sorted(scan_metrics, key=lambda r: -r["rvol"])[:10]
            top_leader = rvol_leaders[0] if rvol_leaders else gainers[0]
            momentum_snapshot.update({
                "topGainers": gainers,
                "topLosers": losers,
                "rvolBreakouts": rvol_leaders[:12],
                "scannedAt": datetime.now(IST).strftime("%H:%M:%S IST"),
                "topLeaderText": f"{top_leader['symbol']} ({top_leader['rvol']:.1f}x RVOL, {top_leader['dayChangePct']:+.2f}%)"
            })

        scanner_status["lastScan"] = datetime.utcnow().isoformat()
        scanner_status["nextScan"] = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
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
        f"Scan complete. {scanner_status['stocksScanned']} F&O stocks scanned. Found {new_signals_count} signals."
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

    update_index_spots_and_regime()

    if not is_mock and dhan_status.get("isValid", False):
        for index_sym, cfg in INDEX_MAPPINGS.items():
            result = fetch_active_option_chain(index_sym, cfg)
            if result:
                active_option_chain_data[index_sym] = result["oc"]
            time.sleep(0.35)
        try:
            update_index_spots_and_regime()
        except Exception:
            pass

    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]
    for idx_name in indices:
        spot = index_spots[idx_name]["spot"]
        strikes = get_atm_strikes(idx_name, spot)
        oc_data = active_option_chain_data.get(idx_name)
        idx_ana = option_analytics.get(idx_name, {})
        call_wall = float(idx_ana.get("call_wall") or 0)
        put_wall = float(idx_ana.get("put_wall") or 0)
        idx_step = STRIKE_STEPS.get(idx_name, 50)

        # Keep any already-active paper trade contracts synced directly to live OC feed
        if oc_data:
            for tr in paper_trades:
                if tr.get("status") == "ACTIVE" and tr.get("index") == idx_name:
                    m_key = next((k for k in oc_data.keys() if abs(float(k) - float(tr.get("strike", 0))) < 1), None)
                    if m_key:
                        side = oc_data[m_key].get(str(tr.get("optionType", "CE")).lower(), {})
                        if side.get("last_price"):
                            push_real_option_price_to_candles(tr["symbol"], float(side["last_price"]), side.get("volume"))

        for strike in strikes:
            for option_type in ["CE", "PE"]:
                scrips_scanned += 1
                contract_key = f"{idx_name}-{strike:.0f}-{option_type}"

                candles = []
                real_ltp = None
                real_vol = None
                spread_pct = 0.0
                opt_delta = 0.0
                opt_iv = 0.0
                oi_buildup = "NEUTRAL"
                opt_vwap = 0.0

                if oc_data:
                    matching_key = next((k for k in oc_data.keys() if abs(float(k) - strike) < 1), None)
                    if matching_key:
                        chain_item = oc_data[matching_key]
                        type_key = option_type.lower()
                        if type_key in chain_item:
                            side = chain_item[type_key]
                            real_ltp = side.get("last_price")
                            real_vol = side.get("volume")
                            bid = float(side.get("top_bid_price") or 0)
                            ask = float(side.get("top_ask_price") or 0)
                            if real_ltp and ask > bid > 0:
                                spread_pct = round(((ask - bid) / float(real_ltp)) * 100, 2)
                            opt_delta = round(abs(float((side.get("greeks") or {}).get("delta") or 0)), 3)
                            opt_iv = round(float(side.get("implied_volatility") or 0), 2)
                            opt_vwap = round(float(side.get("average_price") or real_ltp or 0), 2)
                            prev_c = float(side.get("previous_close_price") or real_ltp or 0)
                            prev_oi = float(side.get("previous_oi") or side.get("oi") or 0)
                            oi_chg = float(side.get("oi") or 0) - prev_oi
                            oi_buildup = _classify_oi_buildup((float(real_ltp or 0) - prev_c), oi_chg)

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
                is_wick_acceptable = overall_size > 0 and (curr_tot_wick <= 0.20 * overall_size)

                is_bull_engulf = (
                    perfect_engulf_info.get("isPerfect") and
                    perfect_engulf_info.get("direction") == "BULLISH" and
                    body_size > 0 and
                    is_wick_acceptable
                )

                is_bear_engulf = (
                    perfect_engulf_info.get("isPerfect") and
                    perfect_engulf_info.get("direction") == "BEARISH" and
                    body_size > 0 and
                    is_wick_acceptable
                )

                if is_bull_engulf:
                    signal_triggered = True
                    signal_type = "BULLISH_ENGULFING"
                    reason = f"Perfect Bullish Engulfing: Green body (₹{current['open']:.2f} - ₹{current['close']:.2f}) engulfed previous body (₹{previous['open']:.2f} - ₹{previous['close']:.2f}) by {perfect_engulf_info['ratio']} | {oi_buildup}"
                elif is_bear_engulf:
                    signal_triggered = True
                    signal_type = "BEARISH_ENGULFING"
                    reason = f"Perfect Bearish Engulfing: Red body (₹{current['open']:.2f} - ₹{current['close']:.2f}) engulfed previous body (₹{previous['open']:.2f} - ₹{previous['close']:.2f}) by {perfect_engulf_info['ratio']} | {oi_buildup}"
                elif is_resistance_break and is_volume_spike:
                    signal_triggered = True
                    signal_type = "RESISTANCE_BREAKOUT"
                    reason = f"Option broke resistance ₹{resistance:.2f} with {(current['volume']/avg_volume):.1f}x volume spike | {oi_buildup}"
                elif is_support_break:
                    signal_triggered = True
                    signal_type = "SUPPORT_BREAKDOWN"
                    reason = f"Option broke below support floor ₹{support:.2f} | {oi_buildup}"
                elif is_momentum_spike:
                    signal_triggered = True
                    signal_type = "MOMENTUM_SPIKE"
                    reason = f"Strike velocity surge: {point_movement:+.1f} pts ({oi_buildup})"

                if signal_triggered and not is_duplicate_alert(contract_key, signal_type):
                    risk_audit_log["totalChecked"] += 1
                    is_bull_sig = (signal_type == "BULLISH_ENGULFING" or signal_type == "RESISTANCE_BREAKOUT" or (signal_type == "MOMENTUM_SPIKE" and point_movement > 0))

                    # 🛡️ PALERMO Liquidity & Bid-Ask Spread Gate
                    if spread_pct > 4.0:
                        risk_audit_log["vetoCount"] += 1
                        risk_audit_log["spreadVetoes"] += 1
                        risk_audit_log["lastVetoReason"] = f"Wide bid-ask spread ({spread_pct:.1f}%) on {contract_key}"
                        record_trade_decision("FILTERED", contract_key, "PALERMO", f"Wide bid-ask spread ({spread_pct:.1f}% > 4.0%)", price=current["close"], slippage_pct=round(spread_pct, 2), direction="BULLISH" if is_bull_sig else "BEARISH")
                        logger.info(f"[PALERMO Spread Veto] {risk_audit_log['lastVetoReason']}")
                        continue

                    # 🛡️ LISBON OI Wall Trap Check
                    if is_bull_sig and option_type == "CE" and call_wall > 0 and 0 <= (call_wall - spot) <= (idx_step * 0.5):
                        trap_stats["trapsBlocked"] += 1
                        trap_stats["oiWallBlocks"] += 1
                        trap_stats["lastTrapSymbol"] = contract_key
                        trap_stats["lastTrapReason"] = f"{contract_key}: Spot trapped under Call OI Wall {call_wall:.0f}"
                        record_trade_decision("FILTERED", contract_key, "LISBON", trap_stats["lastTrapReason"], price=current["close"], direction="BULLISH")
                        logger.info(f"[LISBON OI Wall Block] {trap_stats['lastTrapReason']}")
                        continue
                    if is_bull_sig and option_type == "PE" and put_wall > 0 and 0 <= (spot - put_wall) <= (idx_step * 0.5):
                        trap_stats["trapsBlocked"] += 1
                        trap_stats["oiWallBlocks"] += 1
                        trap_stats["lastTrapSymbol"] = contract_key
                        trap_stats["lastTrapReason"] = f"{contract_key}: Spot resting on Put Support Wall {put_wall:.0f}"
                        record_trade_decision("FILTERED", contract_key, "LISBON", trap_stats["lastTrapReason"], price=current["close"], direction="BEARISH")
                        logger.info(f"[LISBON OI Wall Block] {trap_stats['lastTrapReason']}")
                        continue

                    new_signals_count += 1
                    opt_signal = {
                        "id": f"{contract_key}-{int(time.time() * 1000)}",
                        "symbol": contract_key,
                        "index": idx_name,
                        "strike": strike,
                        "optionType": option_type,
                        "type": "BULLISH" if is_bull_sig else "BEARISH",
                        "signalType": signal_type,
                        "price": current["close"],
                        "pointsMoved": round(point_movement, 2),
                        "volume": current["volume"],
                        "avgVolume": round(avg_volume),
                        "reason": reason,
                        "oiBuildup": oi_buildup,
                        "delta": opt_delta,
                        "iv": opt_iv,
                        "vwap": opt_vwap,
                        "spreadPct": spread_pct,
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
                        f"{icon} <b>OPTIONS MOMENTUM DETECTED</b> {icon}\n\n"
                        f"<b>Contract:</b> {opt_signal['symbol']}\n"
                        f"<b>LTP:</b> ₹{opt_signal['price']:.2f} ({oi_buildup})\n"
                        f"<b>Trigger:</b> {opt_signal['signalType']}\n"
                    )
                    if is_engulf:
                        alert_msg += (
                            f"<b>Pattern:</b> {'Perfect Bullish Engulfing' if opt_signal['type'] == 'BULLISH' else 'Perfect Bearish Engulfing'}\n"
                            f"<b>Engulfing Ratio:</b> {opt_signal['ratio']} | <b>Wick:</b> {opt_signal['wickPct']}\n"
                        )
                    alert_msg += (
                        f"<b>Details:</b> {opt_signal['reason']}\n\n"
                        f"<i>🤖 Action: Paper trade & risk check executed.</i>"
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
def scheduler_market_intelligence():
    """
    Keeps NAIROBI (News AI), BERLIN (Index Spots & Regime), STOCKHOLM (Option Chains),
    and TOKYO (Quote Momentum Pulse) fresh continuously without blocking request threads.
    """
    time.sleep(90)  # Let refresh_startup_intelligence() complete initial prime cleanly
    while True:
        try:
            now_ist = datetime.now(IST)
            current_minutes = now_ist.hour * 60 + now_ist.minute
            is_market_open = (0 <= now_ist.weekday() <= 4 and (9 * 60 + 15) <= current_minutes <= (15 * 60 + 30))

            fetch_live_financial_news(force=is_market_open)
            update_index_spots_and_regime()
            if dhan_status.get("isValid", False):
                for idx_sym, cfg in INDEX_MAPPINGS.items():
                    res = fetch_active_option_chain(idx_sym, cfg)
                    if res:
                        active_option_chain_data[idx_sym] = res["oc"]
                    time.sleep(0.5)
                update_index_spots_and_regime()
                update_momentum_snapshot_from_quotes()

            time.sleep(120 if is_market_open else 600)
        except Exception as e:
            logger.debug(f"[Market Intelligence Scheduler Notice]: {e}")
            time.sleep(120)

def scheduler_stock_scanner():
    time.sleep(10)
    while True:
        try:
            now_ist = datetime.now(IST)
            current_minutes = now_ist.hour * 60 + now_ist.minute
            market_start = 9 * 60 + 15
            market_end = 15 * 60 + 30
            day = now_ist.weekday()  # Mon=0, Sun=6

            is_market_open = (0 <= day <= 4 and market_start <= current_minutes <= market_end)
            if is_market_open:
                logger.info(f"[Scheduler] Stock Scan Triggered (Live Trading Session). Active F&O: {len(active_scanning_list)}")
                run_automatic_scan(False)
            else:
                logger.debug("[Scheduler] Market is closed. Automatic sweeps paused (use manual button).")
        except Exception as e:
            logger.error(f"[Scheduler Stock Error]: {e}")

        time.sleep(300)  # Check every 5 minutes

def scheduler_options_scanner():
    time.sleep(20)
    while True:
        try:
            now_ist = datetime.now(IST)
            current_minutes = now_ist.hour * 60 + now_ist.minute
            market_start = 9 * 60 + 15
            market_end = 15 * 60 + 30
            day = now_ist.weekday()

            is_market_open = (0 <= day <= 4 and market_start <= current_minutes <= market_end)
            if is_market_open:
                run_options_scan()
        except Exception as e:
            logger.error(f"[Scheduler Options Error]: {e}")

        time.sleep(180)

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
        if "maxSlippagePct" in body:
            risk_settings["maxSlippagePct"] = round(max(0.1, min(10.0, float(body["maxSlippagePct"]))), 2)
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
        threading.Thread(target=refresh_startup_intelligence, daemon=True).start()
        return jsonify({"success": True, "message": "DhanHQ API Connected Successfully!", "data": status})
    else:
        return jsonify({
            "success": False,
            "message": f"Dhan authentication notice: {status.get('error')}. Live NSE fallback engine active.",
            "data": status
        })

@app.route("/api/agents/status", methods=["GET"])
def get_agents_status():
    nifty_spot = index_spots.get("NIFTY", {}).get("spot", 25255.05)
    nifty_chg = index_spots.get("NIFTY", {}).get("change", 0.0)
    banknifty_spot = index_spots.get("BANKNIFTY", {}).get("spot", 60792.40)
    sensex_spot = index_spots.get("SENSEX", {}).get("spot", 81879.40)
    vix_spot = index_spots.get("INDIA_VIX", {}).get("spot", index_regime.get("vix", 17.56))
    vix_chg = index_spots.get("INDIA_VIX", {}).get("change", 0.0)

    is_dhan_ok = dhan_status.get("isValid", False)
    provider_name = "DHANHQ DIRECT" if is_dhan_ok else "NSE REAL-TIME STREAM"

    # Pick strongest active option analytics (SENSEX or NIFTY)
    sensex_ana = option_analytics.get("SENSEX", {})
    nifty_ana = option_analytics.get("NIFTY", {})
    active_ana = hot_strikes_board[0] if hot_strikes_board else None
    s_pcr = sensex_ana.get("pcr", 1.0)
    s_coi_pcr = sensex_ana.get("coi_pcr", s_pcr)
    s_hot = active_ana["contract"] if active_ana else sensex_ana.get("hot_strike", "81600 CE")
    s_hot_chg = f" ({active_ana['pctChange']:+.0f}%)" if active_ana else ""
    s_call_wall = sensex_ana.get("call_wall", 82500.0)
    s_put_wall = sensex_ana.get("put_wall", 81000.0)
    s_max_pain = sensex_ana.get("max_pain", 81900.0)
    s_bias = sensex_ana.get("bias", nifty_ana.get("bias", "NEUTRAL"))

    rvol_list = momentum_snapshot.get("rvolBreakouts", [])
    rvol_high = [r for r in rvol_list if float(r.get("rvol", 1.0)) >= 1.5]
    top_leader_raw = momentum_snapshot.get("topLeaderText") or ""
    top_leader = top_leader_raw if momentum_snapshot.get("scannedAt") else f"{len(active_scanning_list)} F&O READY"

    grade_a_cnt = sum(1 for s in active_signals if s.get("tradeGrade") in ["A+", "A"])
    active_paper = [t for t in paper_trades if t.get("status") == "ACTIVE"]
    open_pnl = round(sum(float(t.get("pnl") or 0) for t in active_paper), 2)
    total_pnl = round(float(virtual_portfolio.get("netProfit", 0.0)) + open_pnl, 2)
    win_rate = round((virtual_portfolio["winningTrades"] / max(virtual_portfolio["totalTrades"], 1)) * 100)

    news_lbl = news_intelligence.get("sentimentLabel", "NEUTRAL")
    news_score = news_intelligence.get("sentimentScore", 0)
    news_cnt = news_intelligence.get("totalHeadlines", 0)
    news_sectors = ", ".join(news_intelligence.get("impactSectors", [])[:2]) or "INDEX"
    top_headline = news_intelligence.get("topHeadline", "Monitoring ET / Moneycontrol / LiveMint feeds")

    veto_cnt = int(risk_audit_log.get("vetoCount", 0))
    be_cnt = int(risk_audit_log.get("beTrailedCount", 0)) + int(risk_audit_log.get("profitLockedCount", 0))
    traps_blocked = int(trap_stats.get("trapsBlocked", 0))
    regime_lbl = index_regime.get("regimeLabel", "CALIBRATING")
    exp_move = index_regime.get("expectedMove", "±0.0%")

    agents = [
        {
            "id": "TOKYO",
            "name": "TOKYO",
            "number": "01",
            "role": "MOMENTUM",
            "emoji": "🧑‍🚀",
            "badgeColor": "amber",
            "duty": f"F&O Momentum & RVOL Sniper ({len(active_scanning_list)} Scrips)",
            "metric": f"{len(rvol_high)} RVOL | {len(active_scanning_list)} F&O" if rvol_high else top_leader,
            "status": "SCANNING LIVE" if scanner_status["isRunning"] else f"PULSE: {momentum_snapshot.get('scannedAt') or 'PRIMED'}",
            "statusClass": "text-amber-700 bg-amber-50 border-amber-200",
            "dotClass": "bg-amber-500 animate-ping" if scanner_status["isRunning"] else "bg-amber-500",
            "details": f"Top Mover: {top_leader} | Tracks RVOL >= 1.5x & VWAP breakouts across all F&O stocks"
        },
        {
            "id": "PALERMO",
            "name": "PALERMO",
            "number": "02",
            "role": "RISK GATE",
            "emoji": "👮",
            "badgeColor": "emerald",
            "duty": "Capital Risk, Margin, Spread & Circuit Gate",
            "metric": f"{risk_settings.get('currentDailyTrades', 0)}/{risk_settings.get('maxDailyTrades', 10)} | {veto_cnt} VETO",
            "status": f"SHIELD ({risk_audit_log.get('circuitAlerts', 0)} CKT)",
            "statusClass": "text-emerald-700 bg-emerald-50 border-emerald-200",
            "dotClass": "bg-emerald-500",
            "details": f"Checked: {risk_audit_log.get('totalChecked', 0)} | Spread Vetoes: {risk_audit_log.get('spreadVetoes', 0)} | {risk_audit_log.get('lastVetoReason', 'Nominal')}"
        },
        {
            "id": "DENVER",
            "name": "DENVER",
            "number": "03",
            "role": "SIGNALS",
            "emoji": "🕵️",
            "badgeColor": "yellow",
            "duty": "25m Engulfing + VWAP/RSI/RVOL Confluence Engine",
            "metric": f"{len(active_signals)} SIG ({grade_a_cnt} A+)",
            "status": "CONFLUENCE PRO",
            "statusClass": "text-yellow-700 bg-yellow-50 border-yellow-200",
            "dotClass": "bg-yellow-500",
            "details": "Strict 25m Full-Body Engulfing (Wick <= 20%) scored 0-100 with VWAP, RVOL & RSI confluence"
        },
        {
            "id": "STOCKHOLM",
            "name": "STOCKHOLM",
            "number": "04",
            "role": "OPT CHAIN",
            "emoji": "🎯",
            "badgeColor": "blue",
            "duty": "Index Option Chain OI & Strike Momentum Hunter",
            "metric": f"{s_hot}{s_hot_chg}",
            "status": f"PCR {s_pcr:.2f} | {s_bias}",
            "statusClass": "text-blue-700 bg-blue-50 border-blue-200",
            "dotClass": "bg-blue-500",
            "details": f"SENSEX PCR {s_pcr:.2f} (COI {s_coi_pcr:.2f}) | MaxPain {s_max_pain:,.0f} | Call Wall {s_call_wall:,.0f} | Put Wall {s_put_wall:,.0f}"
        },
        {
            "id": "PROFESSOR",
            "name": "PROFESSOR",
            "number": "05",
            "role": "ROUTER",
            "emoji": "🧔",
            "badgeColor": "teal",
            "duty": "Order Router & 2-Stage Trailing SL Manager",
            "metric": f"{'DHAN' if is_dhan_ok else 'NSE'} | {be_cnt} SL+",
            "status": "AUTO LIVE" if risk_settings["autoTrading"] else "PAPER TRADING",
            "statusClass": "text-teal-700 bg-teal-50 border-teal-200",
            "dotClass": "bg-teal-500",
            "details": f"Stage-1 1R->BE & Stage-2 1.5R->+0.5R Lock ({risk_audit_log.get('beTrailedCount', 0)} BE, {risk_audit_log.get('profitLockedCount', 0)} Locked)"
        },
        {
            "id": "RIO",
            "name": "RIO",
            "number": "06",
            "role": "CHARTS",
            "emoji": "🎧",
            "badgeColor": "purple",
            "duty": "Native 25m DhanHQ/NSE Candle & VWAP/ATR Engine",
            "metric": f"25m | {len(_candle_cache)} CACHED",
            "status": "DHAN NATIVE 25M" if is_dhan_ok else "5m->25m AGG",
            "statusClass": "text-purple-700 bg-purple-50 border-purple-200",
            "dotClass": "bg-purple-500",
            "details": "Streams native 25m Dhan candles + live Session VWAP, RSI(14), ATR(14), EMA(9/21) & PDH/PDL"
        },
        {
            "id": "HELSINKI",
            "name": "HELSINKI",
            "number": "07",
            "role": "LEDGER",
            "emoji": "🧔‍♂️",
            "badgeColor": "fuchsia",
            "duty": "INR Capital, Open MTM & Realized P&L Ledger",
            "metric": f"₹{virtual_portfolio['cash']/100000:.2f}L | {len(active_paper)} OPEN",
            "status": f"₹{total_pnl:+,.0f} P&L",
            "statusClass": "text-fuchsia-700 bg-fuchsia-50 border-fuchsia-200",
            "dotClass": "bg-fuchsia-500",
            "details": f"Realized ₹{virtual_portfolio['netProfit']:+,.0f} | Open MTM ₹{open_pnl:+,.0f} | {virtual_portfolio['totalTrades']} trades ({win_rate}% WR)"
        },
        {
            "id": "NAIROBI",
            "name": "NAIROBI",
            "number": "08",
            "role": "NEWS AI",
            "emoji": "👩",
            "badgeColor": "rose",
            "duty": "Indian Market News Sentiment AI & Alerts",
            "metric": f"{news_lbl} ({news_score:+d}) | {news_cnt}",
            "status": f"SECT: {(news_intelligence.get('impactSectors') or ['INDEX'])[0]}",
            "statusClass": "text-rose-700 bg-rose-50 border-rose-200",
            "dotClass": "bg-rose-500",
            "details": f"Top News ({news_lbl}): {top_headline[:95]}"
        },
        {
            "id": "BERLIN",
            "name": "BERLIN",
            "number": "09",
            "role": "INDEX RADAR",
            "emoji": "👨‍💼",
            "badgeColor": "red",
            "duty": "Multi-Index, India VIX & VWAP Macro Regime",
            "metric": f"VIX {vix_spot:.2f} | {index_regime.get('niftyVsVwap', 'VWAP')}"[:24],
            "status": regime_lbl[:20],
            "statusClass": "text-red-700 bg-red-50 border-red-200",
            "dotClass": "bg-red-500",
            "details": f"NIFTY {nifty_spot:,.1f} ({nifty_chg:+.2f}%) | SENSEX {sensex_spot:,.0f} | BANKNIFTY {banknifty_spot:,.0f} | VIX {vix_spot:.2f} ({vix_chg:+.2f}%) | ExpMove {exp_move}"
        },
        {
            "id": "LISBON",
            "name": "LISBON",
            "number": "10",
            "role": "ANTI-TRAP",
            "emoji": "👩‍💼",
            "badgeColor": "amber",
            "duty": "PDH/PDL Liquidity Sweep & OI Wall Trap Shield",
            "metric": f"{traps_blocked} TRAPS BLOCKED",
            "status": "ACTIVE SHIELD",
            "statusClass": "text-amber-800 bg-amber-50 border-amber-200",
            "dotClass": "bg-amber-600",
            "details": f"Blocked: {traps_blocked} (Sweeps: {trap_stats.get('sweepsDetected', 0)}, OI Walls: {trap_stats.get('oiWallBlocks', 0)}, Chop: {trap_stats.get('dojiBlocked', 0)}) | {trap_stats.get('lastTrapReason', '')}"
        }
    ]
    return jsonify({
        "success": True,
        "agents": agents,
        "provider": provider_name,
        "dhanStatus": dhan_status,
        "marketRegime": index_regime
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
    global scan_start_time, scan_progress
    now_ts = time.time()
    if scanner_status["isRunning"]:
        if now_ts - scan_start_time > 20:
            logger.info("[Scanner Trigger] Stalled scan detected (>20s). Auto-recovering...")
            scanner_status["isRunning"] = False
        else:
            return jsonify({
                "success": True,
                "message": "Scan already underway",
                "inProgress": True,
                "progress": max(10, scan_progress)
            })

    scan_progress = 10
    threading.Thread(target=run_automatic_scan, args=(True,), daemon=True).start()
    return jsonify({"success": True, "message": "Manual scan triggered successfully", "inProgress": True, "progress": 10})

@app.route("/api/scanner/status", methods=["GET"])
def get_scanner_status():
    global scan_progress
    prog = scan_progress if scanner_status["isRunning"] else 100
    return jsonify({
        "progress": prog,
        "status": scanner_status
    })

@app.route("/api/news", methods=["GET"])
def get_news():
    return jsonify(fetch_live_financial_news())

@app.route("/api/options/spots", methods=["GET"])
def get_options_spots():
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    if is_mock or not dhan_status.get("isValid", False):
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
        ce_oi = random.randint(800000, 6500000)
        pe_oi = random.randint(800000, 6500000)
        oc[f"{strike:.1f}"] = {
            "ce": {
                "last_price": round(ce_base + (random.random() * 4 - 2), 2),
                "previous_close_price": round(ce_base, 2),
                "average_price": round(ce_base * 0.98, 2),
                "oi": ce_oi,
                "previous_oi": int(ce_oi * 0.92),
                "volume": random.randint(10000, 410000),
                "previous_volume": random.randint(8000, 200000),
                "price_change": round(random.random() * 5 - 2.5, 2)
            },
            "pe": {
                "last_price": round(pe_base + (random.random() * 4 - 2), 2),
                "previous_close_price": round(pe_base, 2),
                "average_price": round(pe_base * 0.98, 2),
                "oi": pe_oi,
                "previous_oi": int(pe_oi * 0.92),
                "volume": random.randint(10000, 410000),
                "previous_volume": random.randint(8000, 200000),
                "price_change": round(random.random() * 5 - 2.5, 2)
            }
        }
    return oc

@app.route("/api/options/option-chain", methods=["GET"])
def get_option_chain():
    is_mock = not credentials["clientId"] or credentials["clientId"] == "your_client_id_here"
    selected_index = request.args.get("index", "NIFTY").upper().strip()
    indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"]

    if is_mock or not dhan_status.get("isValid", False):
        resp_mock = {}
        for idx in indices:
            moc = active_option_chain_data.get(idx) or get_mock_option_chain(idx, index_spots[idx]["spot"])
            if not active_option_chain_data.get(idx):
                analyze_option_chain(idx, moc, index_spots[idx]["spot"], "DEMO")
            resp_mock[idx] = moc
        return jsonify(resp_mock)

    if selected_index in INDEX_MAPPINGS:
        try:
            res = fetch_active_option_chain(selected_index, INDEX_MAPPINGS[selected_index])
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
    summary = build_pipeline_summary()
    eq_base = summary["currentEquity"] or 2000000.0
    enriched_trades = [_enrich_trade_telemetry(t, eq_base) for t in paper_trades]
    with _decision_log_lock:
        audit_snapshot = list(trade_decision_log[:50])
    return jsonify({
        "paperTrades": enriched_trades,
        "virtualPortfolio": virtual_portfolio,
        "equityHistory": equity_history,
        "openPnl": summary["openPnl"],
        "decisionAuditLog": audit_snapshot,
        "pipelineSummary": summary,
        "riskSettings": risk_settings
    })

@app.route("/api/options/status", methods=["GET"])
def get_options_status():
    return jsonify(options_scanner_status)

@app.route("/api/options/reset-paper", methods=["POST"])
def reset_paper_trades():
    global paper_trades, virtual_portfolio, equity_history
    paper_trades = []
    virtual_portfolio = {
        "cash": 2000000.0,
        "initialCash": 2000000.0,
        "totalTrades": 0,
        "winningTrades": 0,
        "netProfit": 0.0
    }
    equity_history = [2000000.0]
    with _decision_log_lock:
        trade_decision_log.clear()
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
        "stocks": [{"symbol": s["symbol"], "name": s.get("name", s["symbol"]), "sector": s.get("sector", ""), "lotSize": s.get("lotSize", "")} for s in active_scanning_list]
    })

_chart_endpoint_cache: Dict[str, Any] = {}
_chart_endpoint_lock = threading.Lock()

@app.route("/api/chart/candles", methods=["GET"])
def get_chart_candles():
    raw_sym = request.args.get("symbol", "IRFC").upper().strip()
    clean_sym = raw_sym.replace(".NS", "").replace("NSE:", "").replace("BSE:", "").strip()
    interval_req = request.args.get("interval", "25m").lower().strip()
    dhan_ok = dhan_status.get("isValid", False)

    cache_key = f"{clean_sym}_{interval_req}_{dhan_ok}"
    now_ts = time.time()
    with _chart_endpoint_lock:
        if cache_key in _chart_endpoint_cache:
            c_time, cached_payload = _chart_endpoint_cache[cache_key]
            if now_ts - c_time < 15:
                return jsonify(cached_payload)

    try:
        series = fetch_candles_series(clean_sym, interval=interval_req, lookback_days=15)
        bars = series.get("bars") or []
        if not bars:
            with _chart_endpoint_lock:
                if cache_key in _chart_endpoint_cache:
                    return jsonify(_chart_endpoint_cache[cache_key][1])
            return jsonify({"success": False, "message": f"No chart data found for {clean_sym}"}), 404

        candles = [
            {
                "time": int(b["time"]),
                "open": float(b["open"]),
                "high": float(b["high"]),
                "low": float(b["low"]),
                "close": float(b["close"])
            }
            for b in bars
        ]
        volumes = [
            {
                "time": int(b["time"]),
                "value": float(b.get("volume") or 0),
                "color": "rgba(16, 185, 129, 0.45)" if b["close"] >= b["open"] else "rgba(239, 68, 68, 0.45)"
            }
            for b in bars
        ]

        active_sig = next((s for s in active_signals if s["symbol"] == clean_sym), None)
        levels = None
        if active_sig:
            levels = {
                "type": active_sig.get("type"),
                "entry": active_sig.get("entryPrice"),
                "stopLoss": active_sig.get("stopLoss"),
                "target": active_sig.get("target"),
                "ratio": active_sig.get("ratio")
            }

        last_price = candles[-1]["close"]
        prev_close = series.get("previousClose") or last_price

        response_payload = {
            "success": True,
            "symbol": clean_sym,
            "ticker": clean_sym,
            "interval": interval_req,
            "source": series.get("source", "DHANHQ"),
            "currentPrice": last_price,
            "previousClose": prev_close,
            "indicators": series.get("indicators", {}),
            "candles": candles,
            "volumes": volumes,
            "levels": levels
        }

        with _chart_endpoint_lock:
            _chart_endpoint_cache[cache_key] = (now_ts, response_payload)

        return jsonify(response_payload)
    except Exception as e:
        logger.error(f"[Chart API Error] {clean_sym}: {e}")
        with _chart_endpoint_lock:
            if cache_key in _chart_endpoint_cache:
                return jsonify(_chart_endpoint_cache[cache_key][1])
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/market-intelligence", methods=["GET"])
def get_market_intelligence():
    sel_idx = request.args.get("index", "SENSEX").upper().strip()
    if sel_idx not in INDEX_MAPPINGS:
        sel_idx = "SENSEX"

    if request.args.get("refresh") == "1":
        try:
            update_index_spots_and_regime()
            if dhan_status.get("isValid", False) and not active_option_chain_data.get(sel_idx):
                res = fetch_active_option_chain(sel_idx, INDEX_MAPPINGS[sel_idx])
                if res:
                    active_option_chain_data[sel_idx] = res["oc"]
        except Exception:
            pass

    oc_raw = active_option_chain_data.get(sel_idx) or {}
    spot = float(index_spots.get(sel_idx, {}).get("spot", 0))
    step = STRIKE_STEPS.get(sel_idx, 50)
    atm = round(spot / step) * step if spot > 0 else 0
    atm_rows: List[Dict[str, Any]] = []

    if oc_raw:
        parsed = []
        for k, v in oc_raw.items():
            try:
                st = float(k)
                parsed.append((st, v))
            except (ValueError, TypeError):
                continue
        parsed.sort(key=lambda x: x[0])
        near = [p for p in parsed if abs(p[0] - atm) <= step * 6]
        if not near:
            near = parsed[:13]
        for st, item in near:
            ce = item.get("ce") or {}
            pe = item.get("pe") or {}
            ce_ltp = float(ce.get("last_price") or 0)
            pe_ltp = float(pe.get("last_price") or 0)
            ce_prev = float(ce.get("previous_close_price") or ce_ltp)
            pe_prev = float(pe.get("previous_close_price") or pe_ltp)
            ce_oi = float(ce.get("oi") or 0)
            pe_oi = float(pe.get("oi") or 0)
            ce_coi = ce_oi - float(ce.get("previous_oi") or ce_oi)
            pe_coi = pe_oi - float(pe.get("previous_oi") or pe_oi)
            atm_rows.append({
                "strike": st,
                "isAtm": abs(st - atm) < (step * 0.5),
                "ceLtp": round(ce_ltp, 2),
                "ceChgPct": round(((ce_ltp - ce_prev) / ce_prev) * 100, 1) if ce_prev > 0 else 0.0,
                "ceOi": int(ce_oi),
                "ceCoi": int(ce_coi),
                "ceBuildup": _classify_oi_buildup(ce_ltp - ce_prev, ce_coi),
                "ceIv": round(float(ce.get("implied_volatility") or 0), 1),
                "peLtp": round(pe_ltp, 2),
                "peChgPct": round(((pe_ltp - pe_prev) / pe_prev) * 100, 1) if pe_prev > 0 else 0.0,
                "peOi": int(pe_oi),
                "peCoi": int(pe_coi),
                "peBuildup": _classify_oi_buildup(pe_ltp - pe_prev, pe_coi),
                "peIv": round(float(pe.get("implied_volatility") or 0), 1)
            })

    with _decision_log_lock:
        audit_snapshot = list(trade_decision_log[:50])
    return jsonify({
        "success": True,
        "selectedIndex": sel_idx,
        "dhanValid": dhan_status.get("isValid", False),
        "indexSpots": index_spots,
        "indexRegime": index_regime,
        "indexTechnicalLevels": index_technical_levels,
        "optionAnalytics": option_analytics,
        "hotStrikesBoard": hot_strikes_board,
        "selectedChainRows": atm_rows,
        "momentumSnapshot": momentum_snapshot,
        "newsIntelligence": news_intelligence,
        "newsHeadlines": (news_cache.get("data") or [])[:12],
        "trapStats": trap_stats,
        "riskAuditLog": risk_audit_log,
        "decisionAuditLog": audit_snapshot,
        "pipelineSummary": build_pipeline_summary(),
        "riskSettings": risk_settings,
        "equityHistory": equity_history,
        "scannerStatus": scanner_status,
        "optionsScannerStatus": options_scanner_status
    })

@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "mode": "active" if credentials["clientId"] else "simulated"
    })


# ----------------------------------------------------
# ⚡ REAL-TIME DHAN & TRADINGVIEW WEBHOOK ENGINE
# ----------------------------------------------------
webhook_received_signals: List[Dict[str, Any]] = []
webhook_stats = {
    "totalReceived": 0,
    "lastReceivedTime": None,
    "lastSymbol": None,
    "lastType": None,
    "status": "LISTENING"
}

def clean_stock_symbol(raw_sym: str) -> str:
    if not raw_sym:
        return ""
    s = str(raw_sym).strip().upper()
    for prefix in ["NSE:", "BSE:", "DHAN:", "MCX:"]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    for suffix in [".NS", ".BO", "-EQ", "-BE"]:
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    return s.strip()

@app.route("/api/webhook/dhan", methods=["POST", "GET"])
@app.route("/api/webhook/tradingview", methods=["POST", "GET"])
@app.route("/api/webhook/alert", methods=["POST", "GET"])
def handle_incoming_webhook():
    if request.method == "GET":
        return jsonify({
            "status": "ACTIVE",
            "message": "Dhan & TradingView Real-Time Webhook Engine is online and listening for 25m engulfing alerts.",
            "stats": webhook_stats,
            "samplePayload": {
                "symbol": "RELIANCE",
                "timeframe": "25",
                "type": "BULLISH",
                "entry": 2480.0,
                "sl": 2450.0,
                "target": 2540.0,
                "wickPct": "12.5%",
                "ratio": "135%",
                "secret": "dhan_fno_secret"
            }
        })

    try:
        payload = request.get_json(silent=True)
        if not payload:
            raw_text = request.get_data(as_text=True)
            if raw_text:
                try:
                    payload = json.loads(raw_text)
                except Exception:
                    payload = dict(request.form) or {}

        if not payload:
            logger.warning("[Webhook Warning] Received empty or invalid payload.")
            return jsonify({"success": False, "error": "Empty or malformed payload"}), 400

        # Optional secret verification
        configured_secret = os.getenv("WEBHOOK_SECRET")
        if configured_secret:
            incoming_secret = payload.get("secret") or payload.get("passphrase") or payload.get("token")
            if incoming_secret != configured_secret:
                logger.warning("[Webhook Security] Rejected unauthorized webhook: secret key mismatch.")
                return jsonify({"success": False, "error": "Unauthorized secret"}), 401

        raw_sym = payload.get("symbol") or payload.get("ticker") or payload.get("stock") or payload.get("scrip")
        symbol = clean_stock_symbol(raw_sym)
        if not symbol:
            return jsonify({"success": False, "error": "Missing stock symbol"}), 400

        stock_meta = next((s for s in active_scanning_list if s["symbol"] == symbol), None)
        company_name = stock_meta.get("name", symbol) if stock_meta else symbol
        lot_size = stock_meta.get("lotSize", "") if stock_meta else ""
        sector = stock_meta.get("sector", "") if stock_meta else ""

        raw_type = str(payload.get("type") or payload.get("action") or payload.get("signal") or payload.get("pattern") or "").upper()
        if "BULL" in raw_type or raw_type == "BUY":
            sig_type = "BULLISH"
        elif "BEAR" in raw_type or raw_type == "SELL":
            sig_type = "BEARISH"
        else:
            sig_type = "BULLISH" if "BULL" in str(payload).upper() else "BEARISH"

        try:
            entry = float(payload.get("entry") or payload.get("price") or payload.get("close") or 0.0)
        except Exception:
            entry = 0.0

        try:
            sl = float(payload.get("sl") or payload.get("stopLoss") or payload.get("stop_loss") or payload.get("low" if sig_type == "BULLISH" else "high") or 0.0)
        except Exception:
            sl = 0.0

        try:
            target = float(payload.get("target") or payload.get("takeProfit") or payload.get("take_profit") or 0.0)
        except Exception:
            target = 0.0

        if entry > 0 and sl > 0 and target == 0:
            risk = abs(entry - sl)
            target = round(entry + (risk * 2.0) if sig_type == "BULLISH" else entry - (risk * 2.0), 2)

        wick_pct = str(payload.get("wickPct") or payload.get("wick_pct") or "≤ 20%")
        ratio = str(payload.get("ratio") or payload.get("engulfRatio") or "≥ 110%")
        timeframe = str(payload.get("timeframe") or payload.get("interval") or "25m")

        if is_duplicate_alert(symbol, sig_type):
            logger.info(f"[Webhook De-dup] Skipped duplicate alert for {symbol} ({sig_type})")
            return jsonify({"success": True, "message": "Duplicate alert within cooldown period, safely ignored."}), 200

        sig_id = f"{symbol}-{int(time.time() * 1000)}-webhook"
        trend_context = "Real-Time Dhan Webhook Reversal (Photo Validated: Wick ≤ 20%, Body ≥ 110%)"
        signal = {
            "id": sig_id,
            "symbol": symbol,
            "name": company_name,
            "type": sig_type,
            "strength": int(payload.get("strength") or 95),
            "entryPrice": entry,
            "stopLoss": sl,
            "target": target,
            "perfectCategory": f"Dhan Webhook Full Body ({ratio})",
            "pctOfAvg": "100%",
            "ratio": ratio,
            "wickPct": wick_pct,
            "requiredRange": "Full Body (Wick ≤ 20%)",
            "trendContext": trend_context,
            "timestamp": datetime.utcnow().isoformat(),
            "source": "DHAN_WEBHOOK",
            "timeframe": timeframe,
            "lotSize": lot_size,
            "sector": sector
        }

        active_signals.insert(0, signal)
        if len(active_signals) > 50:
            active_signals.pop()

        webhook_stats["totalReceived"] += 1
        webhook_stats["lastReceivedTime"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        webhook_stats["lastSymbol"] = symbol
        webhook_stats["lastType"] = sig_type
        webhook_received_signals.insert(0, signal)
        if len(webhook_received_signals) > 100:
            webhook_received_signals.pop()

        logger.info(f"[Dhan Webhook Alert] 🎯 {sig_type} in {symbol} @ ₹{entry} | SL: ₹{sl} | TGT: ₹{target} | Lot: {lot_size}")

        fake_candle = {
            "current": {"open": entry, "high": max(entry, sl, target), "low": min(entry, sl, target), "close": entry},
            "previous": {"open": sl, "close": entry}
        }
        threading.Thread(target=dispatch_signal_alerts, args=(signal, fake_candle), daemon=True).start()

        return jsonify({
            "success": True,
            "message": f"Real-time {sig_type} alert for {symbol} received and processed successfully!",
            "signalId": sig_id,
            "signal": signal
        }), 200

    except Exception as e:
        logger.error(f"[Webhook Error] Exception processing webhook: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/webhook/dhan-postback", methods=["POST"])
def handle_dhan_postback():
    try:
        data = request.get_json(silent=True) or {}
        logger.info(f"[Dhan Postback Update]: {data}")
        order_id = str(data.get("orderId") or data.get("order_id") or "")
        order_status = str(data.get("orderStatus") or "").upper()
        if order_id:
            for trade in trade_book:
                if str(trade.get("id")) == order_id:
                    trade["status"] = order_status
                    logger.info(f"[Trade Updated via Postback]: {order_id} -> {order_status}")
                    break
        return jsonify({"success": True, "message": "Postback processed"}), 200
    except Exception as e:
        logger.error(f"[Dhan Postback Error]: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/webhook/stats", methods=["GET"])
def get_webhook_stats():
    return jsonify({
        "success": True,
        "stats": webhook_stats,
        "recentSignals": webhook_received_signals[:10]
    })

@app.route("/api/webhook/test-signal", methods=["POST"])
def trigger_test_webhook_signal():
    body = request.get_json(silent=True) or {}
    sample_sym = body.get("symbol") or "RELIANCE"
    sample_type = body.get("type") or "BULLISH"
    stock_meta = next((s for s in active_scanning_list if s["symbol"] == sample_sym), None)
    entry_p = 2540.0
    sl_p = 2500.0 if sample_type == "BULLISH" else 2580.0
    tgt_p = 2620.0 if sample_type == "BULLISH" else 2460.0
    
    test_signal = {
        "id": f"{sample_sym}-{int(time.time() * 1000)}-test",
        "symbol": sample_sym,
        "name": stock_meta.get("name", sample_sym) if stock_meta else sample_sym,
        "type": sample_type,
        "strength": 98,
        "entryPrice": entry_p,
        "stopLoss": sl_p,
        "target": tgt_p,
        "perfectCategory": "Dhan Webhook Full Body (142%)",
        "pctOfAvg": "100%",
        "ratio": "142%",
        "wickPct": "11.2%",
        "requiredRange": "Full Body (Wick ≤ 20%)",
        "trendContext": "Verified 25m Reversal Signal (Photo Validated)",
        "timestamp": datetime.utcnow().isoformat(),
        "source": "DHAN_WEBHOOK_TEST",
        "timeframe": "25m",
        "lotSize": stock_meta.get("lotSize", "250") if stock_meta else "250",
        "sector": stock_meta.get("sector", "ENERGY") if stock_meta else "ENERGY"
    }
    active_signals.insert(0, test_signal)
    if len(active_signals) > 50:
        active_signals.pop()
    webhook_stats["totalReceived"] += 1
    webhook_stats["lastReceivedTime"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    webhook_stats["lastSymbol"] = sample_sym
    webhook_stats["lastType"] = sample_type
    logger.info(f"[Test Webhook Signal] Generated test alert for {sample_sym}")
    return jsonify({"success": True, "message": f"Test alert for {sample_sym} injected into active signals!", "signal": test_signal})

# Serve Animated GPTHEIST DESK Dashboard directly from templates/
TEMPLATES_DIR = BASE_DIR / "templates"
DIST_DIR = ROOT_DIR / "dist"


# ----------------------------------------------------
# 🧪 INSTITUTIONAL HISTORICAL BACKTESTING ENGINE (REAL BAR-BY-BAR SIMULATION)
# ----------------------------------------------------
@app.route("/api/backtest/run", methods=["POST", "GET"])
def run_backtest_simulation():
    try:
        data = request.get_json(silent=True) or {}
        raw_sym = (data.get("symbol") or request.args.get("symbol") or "SENSEX").upper().strip()
        clean_sym = raw_sym.replace(".NS", "").replace("NSE:", "").replace("BSE:", "").strip()
        timeframe = (data.get("timeframe") or request.args.get("timeframe") or "25m").lower().strip()
        rr_ratio = max(1.0, min(5.0, float(data.get("riskReward") or request.args.get("riskReward") or 2.0)))
        days = max(5, min(60, int(data.get("days") or request.args.get("days") or 30)))
        trailing_raw = data.get("trailingSl", request.args.get("trailingSl", True))
        trailing_sl = trailing_raw if isinstance(trailing_raw, bool) else str(trailing_raw).lower() != "false"
        lot_mult = max(1, min(10, int(data.get("lotMultiplier") or request.args.get("lotMultiplier") or 1)))

        series = fetch_candles_series(clean_sym, interval=timeframe, lookback_days=days)
        bars = series.get("bars") or []
        data_source = series.get("source", "DHANHQ")

        if len(bars) < 25:
            return jsonify({
                "success": False,
                "error": f"Insufficient historical bars ({len(bars)}) for {clean_sym} on {timeframe} over {days}d."
            }), 400

        stock_meta = next((s for s in active_scanning_list if s["symbol"].upper() == clean_sym), None)
        if clean_sym in INDEX_MAPPINGS:
            base_lot = int(INDEX_MAPPINGS[clean_sym]["lotSize"])
        elif stock_meta and str(stock_meta.get("lotSize", "")).strip().isdigit():
            base_lot = int(str(stock_meta["lotSize"]).strip())
        else:
            avg_px = float(bars[-1]["close"] or 100.0)
            base_lot = max(1, int(200000.0 / avg_px))
        qty = base_lot * lot_mult

        initial_cap = 2000000.0
        equity = initial_cap
        peak_equity = equity
        max_dd_pct = 0.0
        equity_curve = [equity]

        simulated_trades: List[Dict[str, Any]] = []
        active_pos: Optional[Dict[str, Any]] = None

        for i in range(21, len(bars)):
            bar = bars[i]
            bar_dt = datetime.fromtimestamp(int(bar["time"]), IST).strftime("%Y-%m-%d %H:%M")

            # 1. Manage open position bar-by-bar (conservative: check SL before trailing & target)
            if active_pos is not None:
                entry = active_pos["entry"]
                risk = active_pos["risk"]
                sl = active_pos["sl"]
                target = active_pos["target"]
                direction = active_pos["direction"]
                exit_price = None
                status = None

                if direction == "BULLISH":
                    if bar["low"] <= sl:
                        exit_price = sl
                        status = "PROFIT_LOCK_EXIT" if sl > entry else ("BREAKEVEN_EXIT" if sl == entry else "STOP_LOSS")
                    else:
                        if trailing_sl:
                            if bar["high"] >= (entry + risk * 1.5) and sl < (entry + risk * 0.5):
                                active_pos["sl"] = round(entry + risk * 0.5, 2)
                                sl = active_pos["sl"]
                            elif bar["high"] >= (entry + risk) and sl < entry:
                                active_pos["sl"] = entry
                                sl = active_pos["sl"]
                        if bar["high"] >= target:
                            exit_price = target
                            status = "TARGET_HIT"
                else:
                    if bar["high"] >= sl:
                        exit_price = sl
                        status = "PROFIT_LOCK_EXIT" if sl < entry else ("BREAKEVEN_EXIT" if sl == entry else "STOP_LOSS")
                    else:
                        if trailing_sl:
                            if bar["low"] <= (entry - risk * 1.5) and sl > (entry - risk * 0.5):
                                active_pos["sl"] = round(entry - risk * 0.5, 2)
                                sl = active_pos["sl"]
                            elif bar["low"] <= (entry - risk) and sl > entry:
                                active_pos["sl"] = entry
                                sl = active_pos["sl"]
                        if bar["low"] <= target:
                            exit_price = target
                            status = "TARGET_HIT"

                if exit_price is not None:
                    raw_diff = (exit_price - entry) if direction == "BULLISH" else (entry - exit_price)
                    pnl = round(raw_diff * qty, 2)
                    r_mult = round(raw_diff / risk, 2) if risk > 0 else 0.0
                    equity = round(equity + pnl, 2)
                    peak_equity = max(peak_equity, equity)
                    dd = ((peak_equity - equity) / peak_equity) * 100.0 if peak_equity > 0 else 0.0
                    if dd > max_dd_pct:
                        max_dd_pct = dd
                    equity_curve.append(equity)

                    simulated_trades.append({
                        "date": active_pos["entryDate"],
                        "exitDate": bar_dt,
                        "symbol": clean_sym,
                        "type": "LONG" if direction == "BULLISH" else "SHORT",
                        "grade": active_pos["grade"],
                        "entry": round(entry, 2),
                        "exit": round(exit_price, 2),
                        "status": status,
                        "pnl": pnl,
                        "rMultiple": r_mult,
                        "reason": active_pos["reason"]
                    })
                    active_pos = None
                    continue

            # 2. Check new entry on closed candle bars[i] (if flat and not final bar)
            if active_pos is None and i < len(bars) - 1:
                win_bars = bars[max(0, i - 60):i + 1]
                ind = compute_technical_indicators(win_bars)
                curr = bars[i]
                prev = bars[i - 1]
                hist = bars[max(0, i - 15):i]
                avg_b = (sum(abs(c["close"] - c["open"]) for c in hist) / len(hist)) if hist else 1.0

                sig = analyze_engulfing_pattern(
                    clean_sym, clean_sym, curr, prev, avg_b,
                    history=hist, indicators=ind, record_traps=False
                )
                if sig:
                    e_price = float(sig["entryPrice"])
                    sl_price = float(sig["stopLoss"])
                    risk_pts = abs(e_price - sl_price)
                    if risk_pts > 0:
                        direction = sig["type"]
                        tgt_price = round(e_price + risk_pts * rr_ratio, 2) if direction == "BULLISH" else round(e_price - risk_pts * rr_ratio, 2)
                        active_pos = {
                            "direction": direction,
                            "entry": e_price,
                            "sl": sl_price,
                            "target": tgt_price,
                            "risk": risk_pts,
                            "entryDate": bar_dt,
                            "grade": sig.get("tradeGrade", "A"),
                            "reason": sig.get("trendContext", "Engulfing Pro Confluence")
                        }

        # Include open trade mark-to-market at final bar
        open_trade = None
        if active_pos is not None:
            last_px = float(bars[-1]["close"])
            raw_diff = (last_px - active_pos["entry"]) if active_pos["direction"] == "BULLISH" else (active_pos["entry"] - last_px)
            pnl = round(raw_diff * qty, 2)
            open_trade = {
                "date": active_pos["entryDate"],
                "exitDate": "OPEN",
                "symbol": clean_sym,
                "type": "LONG" if active_pos["direction"] == "BULLISH" else "SHORT",
                "grade": active_pos["grade"],
                "entry": round(active_pos["entry"], 2),
                "exit": round(last_px, 2),
                "status": "RUNNING_PROFIT" if pnl >= 0 else "RUNNING_OPEN",
                "pnl": pnl,
                "rMultiple": round(raw_diff / active_pos["risk"], 2) if active_pos["risk"] > 0 else 0.0,
                "reason": active_pos["reason"]
            }

        total_trades = len(simulated_trades)
        wins = [t for t in simulated_trades if t["pnl"] > 0]
        losses = [t for t in simulated_trades if t["pnl"] < 0]
        be_saves = sum(1 for t in simulated_trades if t["status"] in ["BREAKEVEN_EXIT", "PROFIT_LOCK_EXIT"])

        win_rate = round((len(wins) / total_trades) * 100, 1) if total_trades > 0 else 0.0
        gross_win = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))
        profit_factor = round(gross_win / gross_loss, 2) if gross_loss > 0 else (9.99 if gross_win > 0 else 0.0)
        net_pnl = round(sum(t["pnl"] for t in simulated_trades), 2)
        avg_win_r = round(sum(t["rMultiple"] for t in wins) / len(wins), 1) if wins else 0.0
        expectancy = round(net_pnl / total_trades, 0) if total_trades > 0 else 0.0

        recent = list(reversed(simulated_trades[-25:]))
        if open_trade:
            recent.insert(0, open_trade)

        start_dt = datetime.fromtimestamp(int(bars[0]["time"]), IST).strftime("%Y-%m-%d")
        end_dt = datetime.fromtimestamp(int(bars[-1]["time"]), IST).strftime("%Y-%m-%d")

        return jsonify({
            "success": True,
            "symbol": clean_sym,
            "strategy": "25m Engulfing + VWAP/RSI/RVOL + Anti-Trap" + (" + 2-Stage Trailing SL" if trailing_sl else ""),
            "timeframe": timeframe,
            "dataSource": data_source,
            "barsAnalyzed": len(bars),
            "dateRange": f"{start_dt} to {end_dt}",
            "lotSize": qty,
            "riskReward": f"1:{rr_ratio:.1f}",
            "trailingSl": trailing_sl,
            "metrics": {
                "totalTrades": total_trades,
                "winningTrades": len(wins),
                "losingTrades": len(losses),
                "breakevenSaves": be_saves,
                "winRate": f"{win_rate}%",
                "profitFactor": profit_factor,
                "maxDrawdown": f"{round(max_dd_pct, 2)}%",
                "netPnL": f"₹{net_pnl:+,.0f}",
                "avgRR": f"1:{avg_win_r}",
                "expectancy": f"₹{expectancy:+,.0f}/trade"
            },
            "equityCurve": equity_curve,
            "recentTrades": recent
        })
    except Exception as e:
        logger.error(f"[Backtest Engine Error]: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_frontend(path):
    if path == "perfect_engulfing_pro.pine":
        if (ROOT_DIR / "perfect_engulfing_pro.pine").exists():
            return send_from_directory(str(ROOT_DIR), "perfect_engulfing_pro.pine")
        elif (BASE_DIR / "perfect_engulfing_pro.pine").exists():
            return send_from_directory(str(BASE_DIR), "perfect_engulfing_pro.pine")
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


def keep_alive_worker():
    """
    Automatic anti-sleep daemon to keep cloud deployments (Render, Koyeb)
    continuously active by self-pinging before the 15-min idle spin-down.
    """
    time.sleep(60)  # Wait 1 minute after boot
    target_url = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("KEEP_ALIVE_URL")
    if not target_url:
        logger.info("[KeepAlive] Self-ping idle (No RENDER_EXTERNAL_URL). Recommended: Use free UptimeRobot to ping /api/health")
        return
    health_url = f"{target_url.rstrip('/')}/api/health"
    logger.info(f"[KeepAlive] Automatic cloud anti-sleep daemon active: Pinging {health_url} every 8 minutes.")
    while True:
        try:
            time.sleep(480)  # 8 minutes
            resp = requests.get(health_url, timeout=15)
            logger.info(f"[KeepAlive] Heartbeat ping {resp.status_code} at {datetime.now(IST).strftime('%H:%M:%S')}")
        except Exception as e:
            logger.debug(f"[KeepAlive] Ping notice: {e}")

# ----------------------------------------------------
# 🚀 STARTUP & ENTRY POINT
# ----------------------------------------------------
def refresh_startup_intelligence():
    """Populates News Sentiment, Index Spots/VIX, Option Chains, and F&O Momentum right after boot."""
    try:
        fetch_live_financial_news(force=True)
    except Exception as e:
        logger.debug(f"[Startup News Notice]: {e}")
    try:
        update_index_spots_and_regime()
    except Exception as e:
        logger.debug(f"[Startup Spots Notice]: {e}")
    if dhan_status.get("isValid", False):
        try:
            update_momentum_snapshot_from_quotes()
        except Exception as e:
            logger.debug(f"[Startup Quote Pulse Notice]: {e}")
        try:
            for idx_sym, cfg in INDEX_MAPPINGS.items():
                res = fetch_active_option_chain(idx_sym, cfg)
                if res:
                    active_option_chain_data[idx_sym] = res["oc"]
                time.sleep(0.35)
            update_index_spots_and_regime()
        except Exception as e:
            logger.debug(f"[Startup Dhan Sync Notice]: {e}")

def startup_sequence():
    load_paper_trades_db()
    if not dhan_status.get("lastChecked"):
        check_dhan_connection()

    has_auto_creds = (
        os.getenv("DHAN_PIN") and os.getenv("DHAN_PIN") != "your_6_digit_pin_here" and
        os.getenv("DHAN_TOTP_SECRET") and os.getenv("DHAN_TOTP_SECRET") != "your_totp_secret_key_here"
    )

    if has_auto_creds and not dhan_status.get("isValid"):
        logger.info("[Startup] Detecting automated login credentials. Attempting token generation...")
        generate_dhan_access_token()
        check_dhan_connection()
    elif not dhan_status.get("isValid"):
        logger.info("[Startup] Automated login bypassed: Credentials not configured in .env. Using token mode.")

    sync_fno_stocks_list()
    resolve_mcx_instruments()

    # Launch background daemons & immediate baseline intelligence prime
    threading.Thread(target=refresh_startup_intelligence, daemon=True).start()
    threading.Thread(target=paper_trades_monitor_loop, daemon=True).start()
    threading.Thread(target=scheduler_market_intelligence, daemon=True).start()
    threading.Thread(target=scheduler_stock_scanner, daemon=True).start()
    threading.Thread(target=scheduler_options_scanner, daemon=True).start()
    threading.Thread(target=scheduler_daily_token_renewal, daemon=True).start()
    threading.Thread(target=keep_alive_worker, daemon=True).start()

    logger.info(f"[Startup] ✅ Worker PID {os.getpid()} READY! DhanHQ Valid={dhan_status.get('isValid')} | Daemons launched.")

_startup_lock = threading.Lock()
_startup_pid: Optional[int] = None

def ensure_services_running():
    global _startup_pid
    curr_pid = os.getpid()
    if _startup_pid == curr_pid:
        return
    with _startup_lock:
        if _startup_pid != curr_pid:
            _startup_pid = curr_pid
            if not dhan_status.get("lastChecked"):
                try:
                    check_dhan_connection()
                except Exception:
                    pass
            threading.Thread(target=startup_sequence, daemon=True).start()

@app.before_request
def _ensure_worker_services_hook():
    ensure_services_running()

# Prime once on module load (and auto-rebind after any Gunicorn worker fork via before_request)
ensure_services_running()

def start():
    logger.info("==================================================")
    logger.info("  PRO-AUTOMATION ALGOTRADER PYTHON BACKEND READY! ")
    logger.info(f"  Port: {PORT} | URL: http://localhost:{PORT}")
    logger.info(f"  Services: {len(active_scanning_list)} NSE F&O Equities, 25-Min Rolling Candles")
    logger.info("==================================================")
    ensure_services_running()
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)

if __name__ == "__main__":
    start()
