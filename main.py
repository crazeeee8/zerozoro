#!/usr/bin/env python3
"""
Yahoo Finance-based trading alert bot (no Binance dependency).
Sniper implementation:
- Exact candle-close alignment (15m & 5m)
- Backfill-on-start: detect recent 15m MACD(fast) zero-cross even if bot restarts
- Minimal API calls: only fetch on candle closes
- Zero-cross detection uses <=/>= safety (won't miss exact zero touches)
- Timestamp-based dedupe/state (no double alerts)
- 5m Engulfing detection only after a 15m MACD(fast) zero-cross; auto-deactivates after first engulfing
- Retry/backoff for Yahoo
- Discord alerts, Health server, graceful shutdown
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
from typing import Optional, Dict

import requests
import yfinance as yf
import pandas as pd

# ---------------- CONFIG ----------------
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TICKER = os.environ.get("TICKER", "BTC-USD").upper()

# MACD settings (fast/slow/signal)
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))

# Filter: engulfing must align with last 15m MACD bias?
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")

# Server port (Render expects the process to bind quickly)
PORT = int(os.environ.get("PORT", "8000"))

# Yahoo fetch retry/backoff
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_DELAY = float(os.environ.get("RETRY_DELAY", "5"))  # seconds

# How much history to fetch
HIST_15M_PERIOD = os.environ.get("HIST_15M_PERIOD", "1mo")
HIST_5M_PERIOD = os.environ.get("HIST_5M_PERIOD", "2d")

# Extra safety buffer (seconds) after close before fetching (ensure data is posted)
CLOSE_BUFFER_SEC = float(os.environ.get("CLOSE_BUFFER_SEC", "2.5"))

# ----------------------------------------

START_TIME = time.time()
STOP_EVENT = threading.Event()
HEALTH_SERVER = None

# State / dedupe
state = {
    # 15m MACD state
    "last_15m_macd_cross_time": None,      # datetime of candle close when cross detected
    "last_15m_macd_bias": None,            # "bullish" or "bearish"
    "last_15m_cross_str": "None",          # pretty string for health endpoint

    # 5m engulfing activation state
    "engulfing_active": False,

    # 5m dedupe
    "last_5m_engulfing_time": None,        # datetime of engulfing candle close
}

# ------------- Utilities -------------
def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def fmt_ts(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def safe_name_to_dt(name) -> datetime.datetime:
    """Index value (Timestamp) -> timezone-aware UTC datetime."""
    if isinstance(name, pd.Timestamp):
        # Assume yfinance timestamps are UTC or naive UTC
        if name.tzinfo is None:
            return name.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
        return name.to_pydatetime().astimezone(datetime.timezone.utc)
    # Fallback: parse/assume UTC
    return pd.Timestamp(name).to_pydatetime().replace(tzinfo=datetime.timezone.utc)

# ------------- Discord -------------
def send_discord_alert(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        print(f"[Discord] Not configured. Message: {message}")
        return
    try:
        payload = {"content": f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] {message}"}
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code not in (200, 204):
            print(f"[Discord] HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[Discord] Error sending message: {e}")

# ------------- Yahoo fetch -------------
def yf_fetch_data(ticker: str, interval: str, period: str) -> Optional[pd.DataFrame]:
    """Fetch data with retry/backoff; returns DataFrame with ['Open','High','Low','Close','Volume']."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(
                tickers=ticker,
                interval=interval,
                period=period,
                progress=False,
                auto_adjust=False,  # use raw OHLC (BTC-USD unaffected by splits/divs)
            )
            if df is None or df.empty:
                raise ValueError("Empty data")
            # Ensure columns exist
            for col in ("Open", "High", "Low", "Close", "Volume"):
                if col not in df.columns:
                    raise ValueError(f"Missing column: {col}")
            return df
        except Exception as e:
            print(f"[Yahoo] {interval} fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)  # linear backoff
    return None

# ------------- MACD -------------
def compute_macd(close: pd.Series) -> pd.DataFrame:
    """
    Standard MACD using pandas EWM (adjust=False):
    macd = EMA_fast - EMA_slow
    signal = EMA(macd, signal_span)
    hist = macd - signal
    """
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": hist})

def detect_zero_cross(macd_series: pd.Series) -> Optional[str]:
    """
    Detects zero-line cross using the last two CLOSED values.
    Uses <=/>= to avoid missing exact-zero touches.
    """
    if macd_series is None or len(macd_series) < 2:
        return None
    prev_val = macd_series.iloc[-2]
    curr_val = macd_series.iloc[-1]
    if prev_val <= 0 and curr_val > 0:
        return "bullish"
    if prev_val >= 0 and curr_val < 0:
        return "bearish"
    return None

# ------------- Patterns -------------
def detect_engulfing(prev: Dict, curr: Dict) -> Optional[str]:
    """
    Plain engulfing:
      - Bullish: prev red, curr green, curr.high >= prev.open and curr.low <= prev.close
                 (tightened to body containment on open/close)
      - Bearish: prev green, curr red, curr.low <= prev.open and curr.high >= prev.close
    Using body containment aligned with your earlier logic.
    """
    prev_open, prev_close = float(prev["Open"]), float(prev["Close"])
    curr_open, curr_close = float(curr["Open"]), float(curr["Close"])

    # Bullish engulfing
    if prev_close < prev_open and curr_close > curr_open:
        if (curr_close >= prev_open) and (curr_open <= prev_close):
            return "bullish"

    # Bearish engulfing
    if prev_close > prev_open and curr_close < curr_open:
        if (curr_close <= prev_open) and (curr_open >= prev_close):
            return "bearish"

    return None

# ------------- Candle alignment -------------
async def sleep_until_next_close(interval_minutes: int, buffer_sec: float = CLOSE_BUFFER_SEC):
    """
    Sleep exactly until the next candle CLOSE for the given interval.
    Adds a small buffer so the API has posted the new candle.
    """
    now = now_utc()
    interval_sec = interval_minutes * 60
    # Seconds since the start of this interval
    seconds_into = (now.minute % interval_minutes) * 60 + now.second + now.microsecond / 1_000_000
    # How many seconds to the end of this interval
    sleep_s = interval_sec - seconds_into
    # Normalize wrap-around
    while sleep_s <= 0:
        sleep_s += interval_sec
    sleep_s += max(0.0, buffer_sec)
    print(f"[Sleep] Waiting {sleep_s:.2f}s for next {interval_minutes}m close...")
    await asyncio.sleep(sleep_s)

# ------------- Health server -------------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            uptime = int(time.time() - START_TIME)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            msg = (
                f"OK - {TICKER} - uptime {uptime}s\n"
                f"MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL})\n"
                f"Last 15m MACD cross: {state['last_15m_cross_str']}\n"
                f"Engulfing active: {state['engulfing_active']}\n"
                f"Last engulfing: {fmt_ts(state['last_5m_engulfing_time']) if state['last_5m_engulfing_time'] else 'None'}\n"
            )
            self.wfile.write(msg.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        return

class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

def start_health_server_now():
    global HEALTH_SERVER
    server = ThreadingTCPServer(("", PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    HEALTH_SERVER = server
    print(f"[Health] Server started on port {PORT}")

# ------------- Observers -------------
async def macd_observer():
    """
    Runs forever:
      - Align to each 15m close
      - Fetch 15m data
      - Compute MACD and check for zero-cross on *last closed candle*
      - If cross, set bias, activate engulfing, alert once per candle timestamp
    Also does a backfill check at startup so we don't miss a cross if the bot restarts.
    """
    print("[MACD] Observer starting...")

    # --- Backfill-on-start: check the most recent closed candle immediately ---
    df = yf_fetch_data(TICKER, "15m", HIST_15M_PERIOD)
    if df is not None and len(df) >= max(MACD_SLOW + 2, 50):
        macd_df = compute_macd(df["Close"])
        cross = detect_zero_cross(macd_df["macd"])
        if cross:
            # Use last closed candle timestamp
            closed_time = safe_name_to_dt(df.index[-1])
            if state["last_15m_macd_cross_time"] != closed_time:
                state["last_15m_macd_cross_time"] = closed_time
                state["last_15m_macd_bias"] = cross
                state["engulfing_active"] = True
                state["last_15m_cross_str"] = f"{cross.upper()} at {fmt_ts(closed_time)} [BACKFILL]"
                emoji = "🚀" if cross == "bullish" else "🔻"
                dir_text = "ABOVE" if cross == "bullish" else "BELOW"
                msg = f"{emoji} 15m MACD FAST crossed {dir_text} zero → **{cross.upper()}** bias (backfill)"
                print(f"[MACD] {msg}")
                send_discord_alert(msg)
    else:
        print("[MACD] Backfill skipped (insufficient data).")

    # --- Main loop (aligned to candle closes) ---
    while not STOP_EVENT.is_set():
        try:
            await sleep_until_next_close(15)
            if STOP_EVENT.is_set():
                break

            df = yf_fetch_data(TICKER, "15m", HIST_15M_PERIOD)
            if df is None or len(df) < max(MACD_SLOW + 2, 50):
                print("[MACD] Insufficient data on 15m fetch.")
                continue

            macd_df = compute_macd(df["Close"])
            last_closed_time = safe_name_to_dt(df.index[-1])

            # Debug values
            prev_macd = macd_df["macd"].iloc[-2]
            curr_macd = macd_df["macd"].iloc[-1]
            print(f"[MACD] {fmt_ts(last_closed_time)} | MACD prev={prev_macd:.6f}, curr={curr_macd:.6f}")

            cross = detect_zero_cross(macd_df["macd"])
            if cross and state["last_15m_macd_cross_time"] != last_closed_time:
                state["last_15m_macd_cross_time"] = last_closed_time
                state["last_15m_macd_bias"] = cross
                state["engulfing_active"] = True
                state["last_15m_cross_str"] = f"{cross.upper()} at {fmt_ts(last_closed_time)}"
                emoji = "🚀" if cross == "bullish" else "🔻"
                dir_text = "ABOVE" if cross == "bullish" else "BELOW"
                msg = f"{emoji} 15m MACD FAST crossed {dir_text} zero → **{cross.upper()}** bias"
                print(f"[MACD] ALERT: {msg}")
                send_discord_alert(msg)

        except Exception as e:
            print(f"[MACD] Error: {e}")
            traceback.print_exc()
            await asyncio.sleep(10)  # brief pause before retry

        await asyncio.sleep(1)

async def engulfing_observer():
    """
    Runs forever:
      - Only acts when engulfing_active == True (set by MACD cross)
      - Align to 5m close, fetch only then
      - Detect single engulfing; after first alert, deactivate until next 15m cross
      - Optional ENGULFING_FILTER: direction must match last_15m_macd_bias
    """
    print("[ENGULFING] Observer starting...")
    while not STOP_EVENT.is_set():
        try:
            if not state["engulfing_active"]:
                await asyncio.sleep(2)
                continue

            await sleep_until_next_close(5)
            if STOP_EVENT.is_set():
                break

            df = yf_fetch_data(TICKER, "5m", HIST_5M_PERIOD)
            if df is None or len(df) < 2:
                print("[ENGULFING] Insufficient 5m data.")
                continue

            prev_row = df.iloc[-2]
            curr_row = df.iloc[-1]
            curr_close_time = safe_name_to_dt(curr_row.name)

            print(f"[ENGULFING] Checking 5m candle at {fmt_ts(curr_close_time)}")

            # Dedupe
            if state["last_5m_engulfing_time"] == curr_close_time:
                print("[ENGULFING] Already processed this candle.")
                continue

            direction = detect_engulfing(prev_row, curr_row)
            if direction:
                bias = state.get("last_15m_macd_bias")
                if ENGULFING_FILTER and bias and (direction != bias):
                    print(f"[ENGULFING] {direction} ignored due to {bias} bias filter.")
                else:
                    msg = f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()})"
                    print(f"[ENGULFING] ALERT: {msg}")
                    send_discord_alert(msg)
                    state["last_5m_engulfing_time"] = curr_close_time
                    # Reset activation until next MACD cross
                    state["engulfing_active"] = False
                    print("[ENGULFING] Deactivated until next 15m MACD cross.")
            else:
                print("[ENGULFING] No engulfing on this candle.")

        except Exception as e:
            print(f"[ENGULFING] Error: {e}")
            traceback.print_exc()
            await asyncio.sleep(10)

        await asyncio.sleep(1)

# ------------- Shutdown -------------
def _signal_handler(signum, _frame):
    print(f"[Signal] Received {signal.Signals(signum).name}, shutting down...")
    STOP_EVENT.set()
    try:
        if HEALTH_SERVER:
            HEALTH_SERVER.shutdown()
            HEALTH_SERVER.server_close()
    except Exception:
        pass

# ------------- Main -------------
async def main():
    start_health_server_now()
    print(
        f"[Init] Monitoring {TICKER} | MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}) | "
        f"ENGULFING_FILTER={ENGULFING_FILTER}"
    )
    send_discord_alert(f"✅ Bot started for {TICKER} (sniper mode).")

    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer()),
    ]
    try:
        while not STOP_EVENT.is_set():
            await asyncio.sleep(1)
    finally:
        print("[Main] Shutting down tasks...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("[Main] Stopped.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("[Main] Exited cleanly.")
    except Exception as e:
        print(f"[Fatal] Unhandled exception: {e}")
        traceback.print_exc()
