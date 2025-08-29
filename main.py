#!/usr/bin/env python3
"""
main.py — Binance-based trading alerts (zero pandas/yfinance dependencies)
- 15m MACD (signal crossover + zero-line cross) with bias tracking
- 5m Engulfing pattern (optional filter by last 15m MACD bias)
- Discord webhook notifications
- Health HTTP server bound immediately so Render sees an open port
- Robust Binance REST fetch with retries and closed-candle filtering
"""

import os
import time
import threading
import http.server
import socketserver
import signal
import traceback
import asyncio
import datetime
import json
import math
from typing import List, Dict, Optional, Tuple
import requests

# ================== CONFIG (override via Render env) ==================
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"
)
# Accept either SYMBOL or TICKER; TICKER like "BTC-USD" is auto-mapped to "BTCUSDT"
TICKER = os.environ.get("TICKER", "BTC-USD").upper()
SYMBOL = (os.environ.get("SYMBOL") or "").upper()  # optional explicit Binance symbol
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", 60))  # seconds between checks
BINANCE_BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com")  # switch to https://api.binance.us if needed

# Health server
PORT = int(os.environ.get("PORT", "8000"))
# =====================================================================

# ---------- shared state / globals ----------
START_TIME = time.time()
STOP_EVENT = threading.Event()
HEALTH_SERVER = None

# For deduping alerts across loops
last_alerted = {
    "15m_fast_cross": None,      # "bullish" | "bearish"
    "15m_signal_cross": None,    # "bullish" | "bearish"
    "5m_engulfing_time": None,   # datetime of last engulfing candle alerted
}
shared_state = {
    "last_15m_macd_bias": None   # "bullish" | "bearish"
}

# ---------- helpers ----------
def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def fmt_ts(dt: datetime.datetime) -> str:
    try:
        return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(dt)

def ticker_to_binance_symbol(ticker: str) -> str:
    """
    Convert a Yahoo-style ticker like 'BTC-USD' to Binance symbol 'BTCUSDT'.
    If ticker already looks like a Binance symbol (e.g., BTCUSDT), return as-is.
    """
    t = ticker.upper().replace("-", "")
    if t.endswith("USD"):
        # Binance uses USDT for most USD-quoted pairs
        return t[:-3] + "USDT"
    return t

BINANCE_SYMBOL = SYMBOL or ticker_to_binance_symbol(TICKER)

# Map Binance interval string to seconds
INTERVAL_TO_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000,
}

# ---------- Discord ----------
def send_discord_alert(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not configured; message:", message)
        return
    try:
        payload = {"content": f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] {message}"}
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if resp.status_code >= 400:
            print(f"[Discord] HTTP {resp.status_code} - {resp.text}")
    except Exception as e:
        print("[Discord error]", e)

# ---------- Binance fetch ----------
def binance_klines(
    symbol: str,
    interval: str,
    limit: int = 200,
    session: Optional[requests.Session] = None,
    timeout: int = 10,
    max_retries: int = 3
) -> List[Dict]:
    """
    Fetch klines from Binance public REST with retries & basic rate-limit handling.
    Returns list of dicts with open_time, close_time (datetime), open, high, low, close, volume (floats).
    Only **closed** candles are returned (filters out the current building candle).
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": min(limit, 1000)}
    sess = session or requests.Session()

    for attempt in range(1, max_retries + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            # Handle rate limiting
            if r.status_code == 429 or r.status_code == 418:
                wait_s = int(r.headers.get("Retry-After", "3"))
                print(f"[Binance] rate limited ({r.status_code}), sleeping {wait_s}s...")
                time.sleep(wait_s)
                continue
            if r.status_code != 200:
                print(f"[Binance] HTTP {r.status_code}: {r.text}")
                time.sleep(1 + attempt)  # small backoff
                continue

            raw = r.json()
            if not isinstance(raw, list):
                print(f"[Binance] unexpected JSON: {raw}")
                time.sleep(1 + attempt)
                continue

            out = []
            now_ms = int(time.time() * 1000)
            for row in raw:
                # row format per Binance docs
                # [ openTime, open, high, low, close, volume, closeTime, quote, trades, takerBuyBase, takerBuyQuote, ignore ]
                open_time_ms = int(row[0])
                close_time_ms = int(row[6])
                # Keep only closed candles (close_time <= "now" - small guard)
                if close_time_ms > now_ms - 1000:
                    continue

                o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4]); v = float(row[5])
                out.append({
                    "open_time": datetime.datetime.utcfromtimestamp(open_time_ms / 1000.0).replace(tzinfo=datetime.timezone.utc),
                    "close_time": datetime.datetime.utcfromtimestamp(close_time_ms / 1000.0).replace(tzinfo=datetime.timezone.utc),
                    "open": o, "high": h, "low": l, "close": c, "volume": v
                })

            if out:
                return out

            # If we got here, either empty or all were unclosed
            print("[Binance] no closed candles returned; retrying...")
            time.sleep(1 + attempt)
        except Exception as e:
            print(f"[Binance] error attempt {attempt}: {e}")
            time.sleep(1 + attempt)

    return []

# ---------- Indicators (pure Python) ----------
def ema_series(values: List[float], span: int) -> List[float]:
    """Exponential moving average (same alpha as pandas: alpha = 2/(span+1))."""
    if span <= 0 or not values:
        return [math.nan] * len(values)
    alpha = 2.0 / (span + 1.0)
    out = []
    ema_val = None
    for v in values:
        if ema_val is None:
            ema_val = v
        else:
            ema_val = alpha * v + (1 - alpha) * ema_val
        out.append(ema_val)
    return out

def compute_macd(closes: List[float]) -> Dict[str, List[float]]:
    ema_fast = ema_series(closes, MACD_FAST)
    ema_slow = ema_series(closes, MACD_SLOW)
    macd = [(f - s) if (not math.isnan(f) and not math.isnan(s)) else math.nan for f, s in zip(ema_fast, ema_slow)]
    signal = ema_series([x if not math.isnan(x) else 0.0 for x in macd], MACD_SIGNAL)
    hist = [(m - s) if (not math.isnan(m) and not math.isnan(s)) else math.nan for m, s in zip(macd, signal)]
    return {"ema_fast": ema_fast, "ema_slow": ema_slow, "macd": macd, "signal": signal, "hist": hist}

def detect_zero_cross(series: List[float]) -> Optional[str]:
    """Return 'bullish' if cross from <=0 to >0, 'bearish' if >=0 to <0, else None."""
    if len(series) < 2:
        return None
    prev, curr = series[-2], series[-1]
    if prev <= 0 < curr:
        return "bullish"
    if prev >= 0 > curr:
        return "bearish"
    return None

def detect_signal_cross(macd: List[float], signal: List[float]) -> Optional[str]:
    """Return 'bullish' when MACD crosses above Signal, 'bearish' when below, else None."""
    if len(macd) < 2 or len(signal) < 2:
        return None
    if macd[-2] <= signal[-2] and macd[-1] > signal[-1]:
        return "bullish"
    if macd[-2] >= signal[-2] and macd[-1] < signal[-1]:
        return "bearish"
    return None

def detect_engulfing(candles: List[Dict]) -> Optional[Tuple[datetime.datetime, str]]:
    """
    Detect last bullish/bearish engulfing on the series and return (time, 'bullish'|'bearish'), else None.
    Uses standard body-engulf rules on consecutive closed candles.
    """
    if len(candles) < 2:
        return None
    prev = candles[-2]
    curr = candles[-1]
    # Bullish engulfing: prev red, curr green, and current body engulfs previous body
    prev_body = prev["close"] - prev["open"]
    curr_body = curr["close"] - curr["open"]

    # bullish
    if prev_body < 0 and curr_body > 0 and curr["close"] >= prev["open"] and curr["open"] <= prev["close"]:
        return (curr["close_time"], "bullish")
    # bearish
    if prev_body > 0 and curr_body < 0 and curr["close"] <= prev["open"] and curr["open"] >= prev["close"]:
        return (curr["close_time"], "bearish")
    return None

# ---------- Health HTTP server ----------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            uptime = int(time.time() - START_TIME)
            content = f"OK - {BINANCE_SYMBOL} - uptime {uptime}s\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(content.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return  # silence

class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

def start_health_server_now():
    global HEALTH_SERVER
    server = ThreadingTCPServer(("", PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    HEALTH_SERVER = server
    print(f"[Health] server started on port {PORT}")

# Bind port ASAP so Render detects it
start_health_server_now()

# ---------- Observers ----------
async def macd_observer():
    """
    15m MACD watcher:
      - signal cross alerts (bullish/bearish)
      - zero-line cross alerts (bullish/bearish)
      - updates shared_state['last_15m_macd_bias']
    """
    session = requests.Session()
    while not STOP_EVENT.is_set():
        try:
            candles = binance_klines(BINANCE_SYMBOL, "15m", limit=240, session=session)
            if len(candles) < max(MACD_SLOW, MACD_SIGNAL) + 2:
                await asyncio.sleep(20)
                continue

            closes = [c["close"] for c in candles]
            macd_data = compute_macd(closes)
            macd_series = macd_data["macd"]
            signal_series = macd_data["signal"]

            # Zero-line cross
            zero = detect_zero_cross(macd_series)
            if zero and zero != last_alerted["15m_fast_cross"]:
                last_alerted["15m_fast_cross"] = zero
                shared_state["last_15m_macd_bias"] = zero
                direction = "ABOVE" if zero == "bullish" else "BELOW"
                emoji = "🚀" if zero == "bullish" else "🔻"
                ts = candles[-1]["close_time"]
                send_discord_alert(f"{emoji} 15m MACD FAST crossed {direction} zero → {zero} bias at {fmt_ts(ts)}")

            # Signal cross
            sig = detect_signal_cross(macd_series, signal_series)
            if sig and sig != last_alerted["15m_signal_cross"]:
                last_alerted["15m_signal_cross"] = sig
                direction = "ABOVE" if sig == "bullish" else "BELOW"
                ts = candles[-1]["close_time"]
                send_discord_alert(f"⚡ 15m MACD crossed {direction} signal → {sig} at {fmt_ts(ts)}")

        except Exception as e:
            print("[MACD observer] error:", e)
            traceback.print_exc()

        for _ in range(max(1, FETCH_INTERVAL // 5)):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(5)

async def engulfing_observer():
    """
    5m Engulfing watcher:
      - Detects bullish/bearish engulfing on the last closed candle
      - Optionally filters against last 15m MACD bias (ENGULFING_FILTER)
    """
    session = requests.Session()
    while not STOP_EVENT.is_set():
        try:
            candles = binance_klines(BINANCE_SYMBOL, "5m", limit=300, session=session)
            if len(candles) < 2:
                await asyncio.sleep(20)
                continue

            eng = detect_engulfing(candles)
            if eng:
                t, direction = eng
                if last_alerted["5m_engulfing_time"] != t:
                    bias = shared_state.get("last_15m_macd_bias")
                    if ENGULFING_FILTER and bias and direction != bias:
                        print(f"[Engulfing] {fmt_ts(t)} {direction} skipped due to 15m bias {bias}")
                    else:
                        send_discord_alert(f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {fmt_ts(t)}")
                        last_alerted["5m_engulfing_time"] = t

        except Exception as e:
            print("[Engulfing observer] error:", e)
            traceback.print_exc()

        for _ in range(max(1, FETCH_INTERVAL // 5)):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(5)

# ---------- graceful shutdown ----------
def _signal_handler(signum, frame):
    print(f"[Signal] received {signum}, shutting down...")
    STOP_EVENT.set()
    try:
        if HEALTH_SERVER:
            HEALTH_SERVER.shutdown()
            HEALTH_SERVER.server_close()
    except Exception:
        pass

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ---------- main ----------
async def main():
    send_discord_alert(f"✅ Bot started for {BINANCE_SYMBOL} (from TICKER={TICKER}).")
    print(f"[Init] Monitoring {BINANCE_SYMBOL} | ENGULFING_FILTER={ENGULFING_FILTER} | FETCH_INTERVAL={FETCH_INTERVAL}s")
    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer()),
    ]
    try:
        while not STOP_EVENT.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print("[Main] cancelling observers...")
        STOP_EVENT.set()
        for t in tasks:
            t.cancel()
        try:
            if HEALTH_SERVER:
                HEALTH_SERVER.shutdown()
                HEALTH_SERVER.server_close()
        except Exception:
            pass
        print("[Main] exit.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("[Fatal] unhandled:", e)
        traceback.print_exc()
        raise
