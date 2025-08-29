#!/usr/bin/env python3
"""
Binance-based trading alert bot (zero pandas/numpy/aiohttp).
Features:
- 15m MACD zero-line + signal-line cross alerts (updates bias)
- 5m Engulfing detection (filtered by 15m bias if enabled)
- Discord webhook alerts
- Health HTTP server bound immediately so Render detects a port
- Graceful shutdown on SIGINT/SIGTERM
- Only dependency: requests
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
import math
from typing import List, Dict, Optional, Tuple
import requests

# ---------------- CONFIG ----------------
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"
)
# Accept "BTC-USD" or "BTCUSDT" etc.
TICKER = os.environ.get("TICKER", "BTC-USD").upper()
SYMBOL_OVERRIDE = os.environ.get("SYMBOL", "")  # optional direct Binance symbol
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", 60))  # seconds between checks
BINANCE_BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com")
PORT = int(os.environ.get("PORT", "8000"))
# ----------------------------------------

START_TIME = time.time()
STOP_EVENT = threading.Event()
HEALTH_SERVER = None

# dedupe
last_alerted = {
    "15m_fast_cross": None,
    "15m_signal_cross": None,
    "5m_engulfing_time": None,
}
shared_state = {"last_15m_macd_bias": None}


def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def fmt_ts(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def ticker_to_binance_symbol(ticker: str) -> str:
    t = ticker.upper().replace("/", "").replace("-", "")
    if t.endswith("USD"):
        return t[:-3] + "USDT"
    return t


BINANCE_SYMBOL = SYMBOL_OVERRIDE.upper() if SYMBOL_OVERRIDE else ticker_to_binance_symbol(TICKER)


# ---------------- Discord helper ----------------
def send_discord_alert(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not configured; message:", message)
        return
    try:
        payload = {"content": f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] {message}"}
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code not in (200, 204):
            print(f"[Discord] HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print("[Discord] error sending message:", e)


# ---------------- Binance REST fetch (no libraries) ----------------
def binance_klines(symbol: str, interval: str, limit: int = 500, max_retries: int = 3) -> List[Dict]:
    """
    Returns a list of dicts:
      {open_time, close_time (datetime UTC), open, high, low, close, volume}
    Only returns closed candles (filters out current open candle).
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": min(limit, 1000)}
    sess = requests.Session()
    now_ms = int(time.time() * 1000)
    for attempt in range(1, max_retries + 1):
        try:
            resp = sess.get(url, params=params, timeout=10)
            if resp.status_code in (418, 429):
                wait = int(resp.headers.get("Retry-After", "3"))
                print(f"[Binance] rate-limited ({resp.status_code}), sleeping {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                print(f"[Binance] HTTP {resp.status_code}: {resp.text}")
                time.sleep(1 + attempt)
                continue
            raw = resp.json()
            if not isinstance(raw, list) or len(raw) == 0:
                print("[Binance] empty response; retrying")
                time.sleep(1 + attempt)
                continue
            out = []
            for row in raw:
                open_time_ms = int(row[0])
                close_time_ms = int(row[6])
                # only closed candles
                if close_time_ms > now_ms - 1000:
                    continue
                out.append({
                    "open_time": datetime.datetime.utcfromtimestamp(open_time_ms / 1000.0).replace(tzinfo=datetime.timezone.utc),
                    "close_time": datetime.datetime.utcfromtimestamp(close_time_ms / 1000.0).replace(tzinfo=datetime.timezone.utc),
                    "open": float(row[1]), "high": float(row[2]), "low": float(row[3]),
                    "close": float(row[4]), "volume": float(row[5])
                })
            if out:
                return out
            time.sleep(1 + attempt)
        except Exception as e:
            print(f"[Binance] exception attempt {attempt}: {e}")
            time.sleep(1 + attempt)
    return []


# ---------------- Math helpers (EMA/MACD) ----------------
def ema_list(values: List[float], span: int) -> List[float]:
    if span <= 0 or not values:
        return [math.nan] * len(values)
    alpha = 2.0 / (span + 1.0)
    out = []
    ema = None
    for v in values:
        if ema is None:
            ema = v
        else:
            ema = alpha * v + (1 - alpha) * ema
        out.append(ema)
    return out


def compute_macd(closes: List[float]) -> Dict[str, List[float]]:
    ema_fast = ema_list(closes, MACD_FAST)
    ema_slow = ema_list(closes, MACD_SLOW)
    macd = []
    for f, s in zip(ema_fast, ema_slow):
        macd.append((f - s) if (not math.isnan(f) and not math.isnan(s)) else math.nan)
    # for signal EMA, treat nan as 0 initial to get reasonable smoothing
    macd_for_signal = [0.0 if math.isnan(x) else x for x in macd]
    signal = ema_list(macd_for_signal, MACD_SIGNAL)
    hist = []
    for m, s in zip(macd, signal):
        hist.append((m - s) if (not math.isnan(m) and not math.isnan(s)) else math.nan)
    return {"macd": macd, "signal": signal, "hist": hist}


def detect_zero_cross(macd: List[float]) -> Optional[str]:
    if len(macd) < 2:
        return None
    prev, curr = macd[-2], macd[-1]
    if prev <= 0 < curr:
        return "bullish"
    if prev >= 0 > curr:
        return "bearish"
    return None


def detect_signal_cross(macd: List[float], signal: List[float]) -> Optional[str]:
    if len(macd) < 2 or len(signal) < 2:
        return None
    if macd[-2] <= signal[-2] and macd[-1] > signal[-1]:
        return "bullish"
    if macd[-2] >= signal[-2] and macd[-1] < signal[-1]:
        return "bearish"
    return None


def detect_engulfing(candles: List[Dict]) -> Optional[Tuple[datetime.datetime, str]]:
    if len(candles) < 2:
        return None
    prev = candles[-2]
    curr = candles[-1]
    prev_body = prev["close"] - prev["open"]
    curr_body = curr["close"] - curr["open"]
    # bullish
    if prev_body < 0 and curr_body > 0 and curr["close"] >= prev["open"] and curr["open"] <= prev["close"]:
        return (curr["close_time"], "bullish")
    # bearish
    if prev_body > 0 and curr_body < 0 and curr["close"] <= prev["open"] and curr["open"] >= prev["close"]:
        return (curr["close_time"], "bearish")
    return None


# ---------------- Health server ----------------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            uptime = int(time.time() - START_TIME)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"OK - {BINANCE_SYMBOL} - uptime {uptime}s\n".encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_health_server_now():
    global HEALTH_SERVER
    server = ThreadingTCPServer(("", PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    HEALTH_SERVER = server
    print(f"[Health] server started on port {PORT}")


start_health_server_now()


# ---------------- Observers ----------------
async def macd_observer():
    session = requests.Session()
    while not STOP_EVENT.is_set():
        try:
            candles = binance_klines(BINANCE_SYMBOL, "15m", limit=500)
            if len(candles) < max(MACD_SLOW + 2, 10):
                await asyncio.sleep(10)
                continue
            closes = [c["close"] for c in candles]
            macd_data = compute_macd(closes)
            macd_series = macd_data["macd"]
            signal_series = macd_data["signal"]

            zero = detect_zero_cross(macd_series)
            if zero and zero != last_alerted["15m_fast_cross"]:
                last_alerted["15m_fast_cross"] = zero
                shared_state["last_15m_macd_bias"] = zero
                emoji = "🚀" if zero == "bullish" else "🔻"
                dir_msg = "ABOVE" if zero == "bullish" else "BELOW"
                ts = candles[-1]["close_time"]
                send_discord_alert(f"{emoji} 15m MACD FAST crossed {dir_msg} zero → {zero} bias at {fmt_ts(ts)}")

            sig = detect_signal_cross(macd_series, signal_series)
            if sig and sig != last_alerted["15m_signal_cross"]:
                last_alerted["15m_signal_cross"] = sig
                dir_msg = "ABOVE" if sig == "bullish" else "BELOW"
                ts = candles[-1]["close_time"]
                send_discord_alert(f"⚡ 15m MACD crossed {dir_msg} signal → {sig} at {fmt_ts(ts)}")

        except Exception as e:
            print("[MACD observer] error:", e)
            traceback.print_exc()

        for _ in range(max(1, FETCH_INTERVAL // 5)):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(5)


async def engulfing_observer():
    session = requests.Session()
    while not STOP_EVENT.is_set():
        try:
            candles = binance_klines(BINANCE_SYMBOL, "5m", limit=500)
            if len(candles) < 2:
                await asyncio.sleep(10)
                continue
            eng = detect_engulfing(candles)
            if eng:
                t, direction = eng
                if last_alerted["5m_engulfing_time"] != t:
                    bias = shared_state.get("last_15m_macd_bias")
                    if ENGULFING_FILTER and bias and direction != bias:
                        print(f"[Engulfing] {fmt_ts(t)} {direction} skipped due to bias {bias}")
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


# ---------------- graceful shutdown ----------------
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


# ---------------- main ----------------
async def main():
    print(f"[Init] Monitoring {BINANCE_SYMBOL} | ENGULFING_FILTER={ENGULFING_FILTER} | FETCH_INTERVAL={FETCH_INTERVAL}s")
    send_discord_alert(f"✅ Bot started for {BINANCE_SYMBOL} (from {TICKER}).")
    tasks = [asyncio.create_task(macd_observer()), asyncio.create_task(engulfing_observer())]
    try:
        while not STOP_EVENT.is_set():
            await asyncio.sleep(1)
    finally:
        print("[Main] shutting down...")
        STOP_EVENT.set()
        for t in tasks:
            t.cancel()
        try:
            if HEALTH_SERVER:
                HEALTH_SERVER.shutdown()
                HEALTH_SERVER.server_close()
        except Exception:
            pass
        print("[Main] stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("[Fatal] unhandled:", e)
        traceback.print_exc()
        raise
