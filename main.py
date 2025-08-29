#!/usr/bin/env python3
"""
Yahoo Finance-based trading alert bot (zero Binance dependency).
Sniper-level implementation:
- 15m MACD zero-line cross alerts
- 5m Engulfing detection triggered only after MACD zero-cross
- Exact candle close alignment
- Minimal API calls + retry/backoff
- Discord webhook alerts
- Health HTTP server
- Graceful shutdown
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
import yfinance as yf

# ---------------- CONFIG ----------------
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/EXAMPLE"
)
TICKER = os.environ.get("TICKER", "BTC-USD").upper()
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
PORT = int(os.environ.get("PORT", "8000"))
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
# ----------------------------------------

START_TIME = time.time()
STOP_EVENT = threading.Event()
HEALTH_SERVER = None

# State
last_alerted = {
    "15m_fast_cross": None,
    "5m_engulfing_time": None,
}
shared_state = {"last_15m_macd_bias": None, "engulfing_active": False}


def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def fmt_ts(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


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


# ---------------- Yahoo Finance fetch ----------------
def yf_latest_candle(ticker: str, interval: str) -> Optional[Dict]:
    """Fetch only the last closed candle with retry/backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(
                tickers=ticker,
                period="2d",  # minimal period
                interval=interval,
                progress=False,
                auto_adjust=False,
            )
            if data.empty:
                raise ValueError("No data returned")
            dt = data.index[-2]  # last closed candle
            row = data.iloc[-2]
            candle = {
                "open_time": dt.to_pydatetime().replace(tzinfo=datetime.timezone.utc),
                "close_time": dt.to_pydatetime().replace(tzinfo=datetime.timezone.utc),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
            return candle
        except Exception as e:
            print(f"[YahooFinance] attempt {attempt+1} error: {e}")
            time.sleep(RETRY_DELAY)
    return None


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
    macd_for_signal = [0.0 if math.isnan(x) else x for x in macd]
    signal = ema_list(macd_for_signal, MACD_SIGNAL)
    hist = [m - s for m, s in zip(macd, signal)]
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


def detect_engulfing(prev: Dict, curr: Dict) -> Optional[str]:
    prev_body = prev["close"] - prev["open"]
    curr_body = curr["close"] - curr["open"]
    if prev_body < 0 and curr_body > 0 and curr["close"] >= prev["open"] and curr["open"] <= prev["close"]:
        return "bullish"
    if prev_body > 0 and curr_body < 0 and curr["close"] <= prev["open"] and curr["open"] >= prev["close"]:
        return "bearish"
    return None


# ---------------- Health server ----------------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            uptime = int(time.time() - START_TIME)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            msg = (
                f"OK - {TICKER} - uptime {uptime}s\n"
                f"Last 15m MACD cross: {last_alerted['15m_fast_cross']}\n"
                f"Engulfing active: {shared_state['engulfing_active']}\n"
            )
            self.wfile.write(msg.encode())
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


# ---------------- Helpers ----------------
def sleep_until_next_close(interval_minutes: int):
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    next_minute = (now.minute // interval_minutes + 1) * interval_minutes
    next_hour = now.hour + (next_minute // 60)
    next_minute %= 60
    next_close = now.replace(hour=next_hour % 24, minute=next_minute, second=0, microsecond=0)
    delta = (next_close - now).total_seconds()
    if delta > 0:
        time.sleep(delta)


# ---------------- Observers ----------------
async def macd_observer():
    while not STOP_EVENT.is_set():
        sleep_until_next_close(15)
        candle = yf_latest_candle(TICKER, "15m")
        if not candle:
            continue

        # Maintain last 15m close prices for MACD
        if "closes" not in shared_state:
            shared_state["closes"] = []
        shared_state["closes"].append(candle["close"])
        shared_state["closes"] = shared_state["closes"][-(MACD_SLOW + 5):]

        macd_data = compute_macd(shared_state["closes"])
        macd_series = macd_data["macd"]

        zero = detect_zero_cross(macd_series)
        if zero and zero != last_alerted["15m_fast_cross"]:
            last_alerted["15m_fast_cross"] = zero
            shared_state["last_15m_macd_bias"] = zero
            shared_state["engulfing_active"] = True  # Activate 5m observer
            emoji = "🚀" if zero == "bullish" else "🔻"
            dir_msg = "ABOVE" if zero == "bullish" else "BELOW"
            send_discord_alert(f"{emoji} 15m MACD FAST crossed {dir_msg} zero → {zero} bias at {fmt_ts(candle['close_time'])}")

        await asyncio.sleep(1)


async def engulfing_observer():
    prev_candle = None
    while not STOP_EVENT.is_set():
        if not shared_state.get("engulfing_active"):
            await asyncio.sleep(5)
            continue

        sleep_until_next_close(5)
        curr_candle = yf_latest_candle(TICKER, "5m")
        if not curr_candle:
            continue

        if prev_candle:
            direction = detect_engulfing(prev_candle, curr_candle)
            if direction and last_alerted["5m_engulfing_time"] != curr_candle["close_time"]:
                bias = shared_state.get("last_15m_macd_bias")
                if ENGULFING_FILTER and bias and direction != bias:
                    print(f"[Engulfing] {fmt_ts(curr_candle['close_time'])} {direction} skipped due to bias {bias}")
                else:
                    send_discord_alert(f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {fmt_ts(curr_candle['close_time'])}")
                    last_alerted["5m_engulfing_time"] = curr_candle["close_time"]
                    # Reset engulfing state after first detection
                    shared_state["engulfing_active"] = False

        prev_candle = curr_candle
        await asyncio.sleep(1)


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
    print(f"[Init] Monitoring {TICKER} | ENGULFING_FILTER={ENGULFING_FILTER}")
    send_discord_alert(f"✅ Bot started for {TICKER}.")
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
