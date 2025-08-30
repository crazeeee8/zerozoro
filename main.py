#!/usr/bin/env python3
"""
main.py - Yahoo Finance sniper bot (optimized)

Features:
- 15m MACD (fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL) zero-line cross detection (single-sided with tolerance)
- Once MACD(fast) zero-cross occurs, activate 5m real-time engulfing detection
  * 5m observer polls only on exact 5m candle close times while active
  * deactivates after the FIRST valid engulfing (or can be re-activated by a later 15m zero-cross)
- Exact candle-close alignment (waits until the next candle fully closed + small buffer)
- Minimal API calls: 15m fetches only every 15m; 5m fetches every 5m only while active
- Persists state to disk to survive restarts (JSON)
- Retry/backoff for Yahoo fetches (exponential)
- Discord webhook alerts
- Health server (immediate bind)
- Graceful shutdown (SIGINT/SIGTERM)

Requirements:
- requests
- yfinance
- pandas

Config via ENV:
- DISCORD_WEBHOOK_URL
- TICKER (default: BTC-USD)
- MACD_FAST, MACD_SLOW, MACD_SIGNAL
- ENGULFING_FILTER (True/False)
- PORT (health server)
- CANDLE_BUFFER_SEC (default: 6) - buffer added after candle close before fetching
- ZERO_TOL (default: 1e-9) - tolerance around zero for MACD cross detection
- STATE_FILE (default: .zerozoro_state.json)
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
import json
from typing import List, Dict, Optional, Tuple, Any

import requests
import yfinance as yf
import pandas as pd

# ---------------- CONFIG ----------------
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
TICKER = os.environ.get("TICKER", "BTC-USD").upper()
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
PORT = int(os.environ.get("PORT", "8000"))
CANDLE_BUFFER_SEC = float(os.environ.get("CANDLE_BUFFER_SEC", "6"))  # seconds to wait after candle close
ZERO_TOL = float(os.environ.get("ZERO_TOL", "1e-9"))  # tolerance for zero-cross detection
STATE_FILE = os.environ.get("STATE_FILE", ".zerozoro_state.json")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
INITIAL_RETRY_DELAY = float(os.environ.get("INITIAL_RETRY_DELAY", "2"))  # seconds
# ----------------------------------------

START_TIME = time.time()
STOP_EVENT = threading.Event()
HEALTH_SERVER = None

# persisted + runtime state
last_alerted = {
    # ISO8601 strings or None
    "15m_fast_cross_time": None,
    "5m_engulfing_time": None,
}
shared_state = {
    "last_15m_macd_bias": None,  # "bullish" / "bearish" / None
    "engulfing_active": False,
    "last_15m_cross_str": "None"
}


# ---------------- Utilities ----------------
def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)


def fmt_ts(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def isoformat(dt: Optional[datetime.datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(datetime.timezone.utc).isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime.datetime]:
    if s is None:
        return None
    try:
        return datetime.datetime.fromisoformat(s).astimezone(datetime.timezone.utc)
    except Exception:
        return None


# ---------------- Persistence ----------------
def load_state() -> None:
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        # restore last_alerted times and shared_state keys safely
        for k in ("15m_fast_cross_time", "5m_engulfing_time"):
            last_alerted[k] = obj.get(k)
        shared_state["last_15m_macd_bias"] = obj.get("last_15m_macd_bias")
        shared_state["engulfing_active"] = bool(obj.get("engulfing_active", False))
        shared_state["last_15m_cross_str"] = obj.get("last_15m_cross_str", "None")
        print(f"[State] Loaded state from {STATE_FILE}: {shared_state}")
    except Exception as e:
        print(f"[State] Error loading state: {e}")


def save_state() -> None:
    try:
        obj: Dict[str, Any] = {
            "15m_fast_cross_time": last_alerted.get("15m_fast_cross_time"),
            "5m_engulfing_time": last_alerted.get("5m_engulfing_time"),
            "last_15m_macd_bias": shared_state.get("last_15m_macd_bias"),
            "engulfing_active": bool(shared_state.get("engulfing_active", False)),
            "last_15m_cross_str": shared_state.get("last_15m_cross_str", "None"),
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(obj, f)
        # quick debug print
        # print(f"[State] Saved state: {obj}")
    except Exception as e:
        print(f"[State] Error saving state: {e}")


# ---------------- Discord helper ----------------
def send_discord_alert(message: str) -> None:
    # print/log always locally so we can see behavior even if webhook missing
    print(f"[Discord] {message}")
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] No webhook configured; skipping remote POST.")
        return
    try:
        payload = {"content": f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')} UTC] {message}"}
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if r.status_code not in (200, 204):
            print(f"[Discord] HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print(f"[Discord] error sending message: {e}")


# ---------------- Yahoo Finance fetch (robust retry + validation) ----------------
def yf_fetch_data(ticker: str, interval: str, period: str = "7d", required_rows: int = 10) -> Optional[pd.DataFrame]:
    """
    Fetch historical OHLCV with exponential backoff. Ensures non-empty result and at least `required_rows`.
    Returns df indexed by timestamp (DatetimeIndex). Timestamps are returned as tz-aware UTC if possible.
    """
    delay = INITIAL_RETRY_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(tickers=ticker, period=period, interval=interval, progress=False, auto_adjust=False)
            if df is None or df.empty:
                raise ValueError("empty dataframe")
            # ensure index timezone awareness: yfinance often returns tz-aware in UTC, but handle naive
            if df.index.tz is None:
                # treat as UTC
                df.index = df.index.tz_localize(datetime.timezone.utc)
            else:
                # convert to UTC
                df.index = df.index.tz_convert(datetime.timezone.utc)
            if len(df) < required_rows:
                raise ValueError(f"not enough rows: {len(df)} (need {required_rows})")
            return df
        except Exception as e:
            print(f"[Yahoo] attempt {attempt}/{MAX_RETRIES} failed for {interval}: {e}")
            if attempt == MAX_RETRIES:
                break
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return None


# ---------------- Math helpers (pandas MACD) ----------------
def compute_macd(closes: pd.Series) -> pd.DataFrame:
    ema_fast = closes.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = closes.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": hist})


def detect_zero_cross(macd_series: pd.Series) -> Optional[str]:
    """
    Detect zero-line crossing on the last two values with tolerance ZERO_TOL.
    returns "bullish" or "bearish" or None
    """
    if len(macd_series) < 2:
        return None
    prev = float(macd_series.iloc[-2])
    curr = float(macd_series.iloc[-1])
    # bullish: prev <= tol_neg and curr > tol_pos
    tol = ZERO_TOL
    if (prev <= tol) and (curr > tol):
        return "bullish"
    if (prev >= -tol) and (curr < -tol):
        return "bearish"
    return None


def detect_engulfing(prev_row: pd.Series, curr_row: pd.Series) -> Optional[str]:
    """
    prev_row and curr_row are Series with Open/High/Low/Close, etc.
    returns 'bullish'/'bearish'/None
    """
    prev_open = float(prev_row["Open"])
    prev_close = float(prev_row["Close"])
    curr_open = float(curr_row["Open"])
    curr_close = float(curr_row["Close"])

    # Bullish engulfing: prev red, curr green, curr close > prev open AND curr open < prev close
    if prev_close < prev_open and curr_close > curr_open and (curr_close > prev_open) and (curr_open < prev_close):
        return "bullish"
    # Bearish engulfing: prev green, curr red, curr close < prev_open AND curr open > prev_close
    if prev_close > prev_open and curr_close < curr_open and (curr_close < prev_open) and (curr_open > prev_close):
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
                f"Last 15m MACD cross: {shared_state.get('last_15m_cross_str')}\n"
                f"Engulfing active: {shared_state.get('engulfing_active')}\n"
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
    try:
        server = ThreadingTCPServer(("", PORT), HealthHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        HEALTH_SERVER = server
        print(f"[Health] server started on port {PORT}")
    except Exception as e:
        print(f"[Health] could not start server: {e}")


# ---------------- timing helper ----------------
async def sleep_until_next_close(interval_minutes: int):
    """
    Sleep until the next candle close for the given interval (in minutes).
    This function adds CANDLE_BUFFER_SEC to avoid reading incomplete candles.
    """
    now = now_utc()
    # seconds into current interval
    seconds_into_interval = (now.minute % interval_minutes) * 60 + now.second + now.microsecond / 1_000_000
    interval_seconds = interval_minutes * 60
    sleep_duration = interval_seconds - seconds_into_interval
    # add a small buffer to let Yahoo populate closed candle
    sleep_duration += CANDLE_BUFFER_SEC
    if sleep_duration <= 0:
        # if negative (rare), wait one full interval plus buffer
        sleep_duration = interval_seconds + CANDLE_BUFFER_SEC
    print(f"[Sleep] waiting {sleep_duration:.1f}s until next {interval_minutes}m candle close + buffer")
    await asyncio.sleep(sleep_duration)


# ---------------- Observers ----------------
async def macd_observer():
    """
    Runs once per 15m candle close. Activates engulfing_observer by setting shared_state['engulfing_active']=True
    when the MACD fast-line crosses ZERO (with tolerance).
    """
    print("[MACD] observer starting")
    # ensure we have starting state loaded
    while not STOP_EVENT.is_set():
        try:
            await sleep_until_next_close(15)
            if STOP_EVENT.is_set():
                break

            # fetch 15m data; period should be long enough to seed EMA
            df = yf_fetch_data(TICKER, "15m", period="14d", required_rows=MACD_SLOW * 10)
            if df is None:
                print("[MACD] failed to fetch 15m data; will retry next interval")
                continue

            # get close series
            closes = df["Close"]
            macd_df = compute_macd(closes)

            # confirm the last closed candle's timestamp
            last_idx = df.index[-1]  # tz-aware UTC
            last_closed_time = last_idx.to_pydatetime().astimezone(datetime.timezone.utc)

            # debug prints
            try:
                prev_val = float(macd_df["macd"].iloc[-2])
                curr_val = float(macd_df["macd"].iloc[-1])
                print(f"[MACD] 15m closed at {fmt_ts(last_closed_time)} | macd last two: {prev_val:.8f}, {curr_val:.8f}")
            except Exception:
                print("[MACD] insufficient macd values to print debug")

            # detect zero cross with tolerance
            cross = detect_zero_cross(macd_df["macd"])
            # compare last alerted cross time to avoid duplicate alerts (persisted ISO string)
            last_cross_iso = last_alerted.get("15m_fast_cross_time")
            last_cross_dt = parse_iso(last_cross_iso)

            if cross:
                # if last alerted time equals this candle, skip alert
                if last_cross_dt is None or last_cross_dt != last_closed_time:
                    print(f"[MACD] detected zero cross: {cross} at {fmt_ts(last_closed_time)}")
                    # set state
                    shared_state["last_15m_macd_bias"] = cross
                    shared_state["engulfing_active"] = True
                    shared_state["last_15m_cross_str"] = f"{cross.upper()} at {fmt_ts(last_closed_time)}"
                    last_alerted["15m_fast_cross_time"] = isoformat(last_closed_time)
                    save_state()
                    # send discord
                    emoji = "🚀" if cross == "bullish" else "🔻"
                    dir_msg = "ABOVE" if cross == "bullish" else "BELOW"
                    send_discord_alert(f"{emoji} 15m MACD FAST crossed {dir_msg} zero → **{cross.upper()}** bias at {fmt_ts(last_closed_time)}")
                else:
                    print("[MACD] cross detected but already alerted for this candle -> skipping")
            else:
                # no cross; continue
                pass

        except Exception as e:
            print(f"[MACD] unexpected error: {e}")
            traceback.print_exc()
            # small sleep to avoid tight-loop on persistent error
            await asyncio.sleep(10)
    print("[MACD] observer stopped")


async def engulfing_observer():
    """
    When engaged (shared_state['engulfing_active']=True), checks every 5m candle close for engulfing
    using only the last closed candle and the one before it. After the first confirmed engulfing alert,
    it deactivates until the next 15m MACD zero-cross.
    """
    print("[Engulfing] observer starting")
    while not STOP_EVENT.is_set():
        try:
            if not shared_state.get("engulfing_active"):
                # poll sleep (cheap)
                await asyncio.sleep(3)
                continue

            # wait until 5m candle close + buffer
            await sleep_until_next_close(5)
            if STOP_EVENT.is_set():
                break

            df = yf_fetch_data(TICKER, "5m", period="2d", required_rows=3)
            if df is None or len(df) < 2:
                print("[Engulfing] insufficient 5m data; skipping")
                continue

            # last two closed candles
            prev_row = df.iloc[-2]
            curr_row = df.iloc[-1]
            last_closed_time = df.index[-1].to_pydatetime().astimezone(datetime.timezone.utc)

            # detect engulfing
            direction = detect_engulfing(prev_row, curr_row)
            print(f"[Engulfing] checked 5m closed at {fmt_ts(last_closed_time)} -> {direction if direction else 'no pattern'}")

            # check dedupe
            last_eng_iso = last_alerted.get("5m_engulfing_time")
            last_eng_dt = parse_iso(last_eng_iso)
            if direction and (last_eng_dt is None or last_eng_dt != last_closed_time):
                bias = shared_state.get("last_15m_macd_bias")
                if ENGULFING_FILTER and bias and (direction != bias):
                    print(f"[Engulfing] detected {direction} but skipped due to bias {bias}")
                else:
                    send_discord_alert(f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {fmt_ts(last_closed_time)}")
                    last_alerted["5m_engulfing_time"] = isoformat(last_closed_time)
                    save_state()
                    # Reset/Deactivate until next 15m zero-cross
                    shared_state["engulfing_active"] = False
                    print("[Engulfing] deactivated until next 15m MACD zero-cross")
            else:
                # either no pattern or already alerted for this candle
                pass

        except Exception as e:
            print(f"[Engulfing] unexpected error: {e}")
            traceback.print_exc()
            await asyncio.sleep(8)
    print("[Engulfing] observer stopped")


# ---------------- graceful shutdown ----------------
def _signal_handler(signum, frame):
    name = None
    try:
        name = signal.Signals(signum).name
    except Exception:
        name = str(signum)
    print(f"[Signal] received {name}, shutting down...")
    STOP_EVENT.set()
    try:
        if HEALTH_SERVER:
            HEALTH_SERVER.shutdown()
            HEALTH_SERVER.server_close()
    except Exception:
        pass


# ---------------- main ----------------
async def main():
    # load persisted state if available
    load_state()

    start_health_server_now()
    print(f"[Init] Monitoring {TICKER} | MACD({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}) | ENGULFING_FILTER={ENGULFING_FILTER}")
    # announce start (non-blocking)
    send_discord_alert(f"✅ Bot started for {TICKER} (MACD {MACD_FAST}/{MACD_SLOW}/{MACD_SIGNAL}).")

    # start observers
    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer())
    ]
    try:
        while not STOP_EVENT.is_set():
            await asyncio.sleep(1)
    finally:
        print("[Main] shutting down tasks...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # save before exit
        save_state()
        print("[Main] stopped.")


if __name__ == "__main__":
    # hooks
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("[Main] Exited cleanly")
    except Exception as e:
        print(f"[Fatal] Unhandled: {e}")
        traceback.print_exc()
        raise
