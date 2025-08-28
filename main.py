# main.py
import os
import time
import math
import json
import random
import threading
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import aiohttp
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from flask import Flask

# -----------------------
# CONFIG / ENV
# -----------------------
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
PORT = int(os.environ.get("PORT", "10000"))
# Yahoo symbol for BTC/USDT spot-equivalent
SYMBOL_5M = os.environ.get("SYMBOL_5M", "BTC-USD")   # yahoo uses BTC-USD
SYMBOL_15M = os.environ.get("SYMBOL_15M", "BTC-USD")
USE_BIAS_FILTER = os.environ.get("USE_BIAS_FILTER", "true").lower() in ("1", "true", "yes")
ENGULF_WATCH_PCT = float(os.environ.get("ENGULF_WATCH_PCT", "1.05"))  # 1.05 = 105% (early watch)
MACD_FAST = int(os.environ.get("MACD_FAST", "8"))
MACD_SLOW = int(os.environ.get("MACD_SLOW", "15"))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", "9"))

# Polling frequencies (seconds)
POLL_1M = int(os.environ.get("POLL_1M", "15"))   # used to build intrabar 5m
POLL_5M = int(os.environ.get("POLL_5M", "20"))   # check 5m closed bars
POLL_15M = int(os.environ.get("POLL_15M", "25"))  # check 15m closed bars

# Keep last N candles
MAX_15M_LOOKBACK = 500
MAX_5M_LOOKBACK = 500

# timezone for display (IST)
IST = timezone(timedelta(hours=5, minutes=30))

# -----------------------
# STATE
# -----------------------
class State:
    def __init__(self):
        # 15m candles (closed) lists: timestamps(ms) & closes
        self.closes_15m: List[float] = []
        self.ts_15m: List[int] = []
        # last fast macd direction (bullish/bearish) from 15m fast line zero-cross
        self.last_fast_direction: Optional[str] = None
        # 5m previous closed candle and current intrabar candle built from 1m ticks
        self.prev_5m: Optional[Dict[str, Any]] = None
        self.live_5m: Optional[Dict[str, Any]] = None
        # idempotency set
        self.sent_event_keys: set = set()
        # aiohttp session
        self.session: Optional[aiohttp.ClientSession] = None

STATE = State()

# -----------------------
# UTIL
# -----------------------
def now_ist_str() -> str:
    return datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

async def ensure_session() -> aiohttp.ClientSession:
    if STATE.session and not STATE.session.closed:
        return STATE.session
    STATE.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
    return STATE.session

async def send_discord_message(payload_text: str):
    """
    Send a plain-text message to Discord webhook.
    Handles 429 Retry-After politely.
    """
    if not DISCORD_WEBHOOK:
        # print locally if webhook not provided
        print(f"[{now_ist_str()}] {payload_text}")
        return

    session = await ensure_session()
    headers = {"Content-Type": "application/json"}
    body = {"content": payload_text}
    for attempt in range(6):
        try:
            async with session.post(DISCORD_WEBHOOK, json=body, headers=headers) as resp:
                if resp.status in (200, 204):
                    return
                text = await resp.text()
                if resp.status == 429:
                    # Respect Retry-After if provided
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else (2 ** attempt) + random.random()
                    print(f"[{now_ist_str()}] Discord rate-limited; waiting {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                # non-retryable or other errors: log & break
                print(f"[{now_ist_str()}] Discord returned {resp.status}: {text}")
                return
        except Exception as e:
            # network error: exponential backoff small jitter
            wait = min(30.0, (2 ** attempt) + random.random())
            print(f"[{now_ist_str()}] Discord send error ({e}); retrying in {wait:.1f}s")
            await asyncio.sleep(wait)
    print(f"[{now_ist_str()}] Discord message failed after retries: {payload_text}")

def event_key(kind: str, tf: str, bar_ts: int, extra: Optional[str] = None) -> str:
    if extra:
        return f"{kind}|{tf}|{bar_ts}|{extra}"
    return f"{kind}|{tf}|{bar_ts}"

def idempotent_alert(kind: str, tf: str, bar_ts: int, msg: str, extra: Optional[str] = None):
    key = event_key(kind, tf, bar_ts, extra)
    if key in STATE.sent_event_keys:
        return
    STATE.sent_event_keys.add(key)
    # fire-and-forget
    asyncio.create_task(send_discord_message(f"[{now_ist_str()}] {msg}"))

# -----------------------
# YAHOO DATA HELPERS (sync calls wrapped in threads)
# -----------------------
def _yf_download(symbol: str, period: str, interval: str) -> pd.DataFrame:
    # wrapper for yfinance.download (synchronous)
    # period e.g., "2d", interval e.g., "1m","5m","15m"
    df = yf.download(tickers=symbol, period=period, interval=interval, progress=False, threads=False)
    return df

async def fetch_yahoo_df(symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
    try:
        df = await asyncio.to_thread(_yf_download, symbol, period, interval)
        if df is None or df.empty:
            return None
        # clean
        df = df.dropna()
        return df
    except Exception as e:
        print(f"[{now_ist_str()}] Yahoo fetch error for {symbol} {interval}: {e}")
        return None

# Helpers to convert DataFrame index to ms epoch
def df_index_to_ms(df: pd.DataFrame) -> List[int]:
    # yfinance returns timezone-aware DatetimeIndex; cast to UTC then ms
    return [int(int(ts.timestamp()) * 1000) for ts in pd.to_datetime(df.index).tz_convert(timezone.utc)]

# -----------------------
# INDICATORS
# -----------------------
def compute_macd_series(closes: List[float]) -> Optional[pd.DataFrame]:
    if len(closes) < max(MACD_SLOW, MACD_SIGNAL) + 3:
        return None
    s = pd.Series(closes)
    macd = ta.macd(s, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
    return macd

# -----------------------
# OBSERVERS
# -----------------------
async def observer_15m_macd_fast():
    """
    Observes 15m closed candles and sends alerts when MACD (fast line) crosses zero.
    Runs independently and continuously.
    """
    while True:
        try:
            df = await fetch_yahoo_df(SYMBOL_15M, period="3d", interval="15m")
            if df is None:
                print(f"[{now_ist_str()}] observer_15m_macd_fast: no data returned")
                await asyncio.sleep(POLL_15M)
                continue

            closes = list(df["Close"].astype(float).tolist())
            timestamps = df_index_to_ms(df)
            # keep history
            STATE.closes_15m = closes[-MAX_15M_LOOKBACK:]
            STATE.ts_15m = timestamps[-MAX_15M_LOOKBACK:]

            macd = compute_macd_series(STATE.closes_15m)
            if macd is None:
                await asyncio.sleep(POLL_15M)
                continue
            macd = macd.dropna()
            if len(macd) < 2:
                await asyncio.sleep(POLL_15M)
                continue

            # Determine column names (pandas_ta can produce names like MACD_8_15_9)
            macd_cols = [c for c in macd.columns if c.upper().startswith("MACD_") and "MACDs" not in c]
            signal_cols = [c for c in macd.columns if "MACDs" in c or c.upper().startswith("MACDS")]
            if not macd_cols or not signal_cols:
                # try heuristics
                macd_cols = [c for c in macd.columns if "MACD" in c and "MACDs" not in c]
                signal_cols = [c for c in macd.columns if "MACDs" in c]
            if not macd_cols or not signal_cols:
                print(f"[{now_ist_str()}] observer_15m_macd_fast: couldn't find MACD cols: {macd.columns}")
                await asyncio.sleep(POLL_15M)
                continue

            macd_line = float(macd.iloc[-1][macd_cols[0]])
            macd_prev = float(macd.iloc[-2][macd_cols[0]])
            bar_ts = STATE.ts_15m[-1] if STATE.ts_15m else int(time.time() * 1000)

            # zero-cross detection
            if macd_prev < 0 <= macd_line:
                # crossed above
                idempotent_alert("MACD_FAST_CROSS", "15m", bar_ts,
                                 f"MACD FAST crossed ABOVE 0 | {SYMBOL_15M} | 15m | macd={macd_line:.8f}",
                                 extra="bull")
                STATE.last_fast_direction = "bullish"
            elif macd_prev > 0 >= macd_line:
                idempotent_alert("MACD_FAST_CROSS", "15m", bar_ts,
                                 f"MACD FAST crossed BELOW 0 | {SYMBOL_15M} | 15m | macd={macd_line:.8f}",
                                 extra="bear")
                STATE.last_fast_direction = "bearish"

        except Exception as e:
            print(f"[{now_ist_str()}] observer_15m_macd_fast error: {e}")
        await asyncio.sleep(POLL_15M)


async def observer_15m_macd_signal():
    """
    Observes 15m closed candles and sends alerts when MACD signal (slow) line crosses zero.
    Runs independently and continuously.
    """
    while True:
        try:
            df = await fetch_yahoo_df(SYMBOL_15M, period="3d", interval="15m")
            if df is None:
                print(f"[{now_ist_str()}] observer_15m_macd_signal: no data returned")
                await asyncio.sleep(POLL_15M)
                continue

            closes = list(df["Close"].astype(float).tolist())
            timestamps = df_index_to_ms(df)
            macd = compute_macd_series(closes)
            if macd is None:
                await asyncio.sleep(POLL_15M)
                continue
            macd = macd.dropna()
            if len(macd) < 2:
                await asyncio.sleep(POLL_15M)
                continue

            # find signal column
            signal_cols = [c for c in macd.columns if "MACDs" in c or c.upper().startswith("MACDS")]
            if not signal_cols:
                print(f"[{now_ist_str()}] observer_15m_macd_signal: couldn't find signal cols")
                await asyncio.sleep(POLL_15M)
                continue

            sig = float(macd.iloc[-1][signal_cols[0]])
            sig_prev = float(macd.iloc[-2][signal_cols[0]])
            bar_ts = timestamps[-1]

            if sig_prev < 0 <= sig:
                idempotent_alert("MACD_SIGNAL_CROSS", "15m", bar_ts,
                                 f"MACD SIGNAL crossed ABOVE 0 | {SYMBOL_15M} | 15m | signal={sig:.8f}")
            elif sig_prev > 0 >= sig:
                idempotent_alert("MACD_SIGNAL_CROSS", "15m", bar_ts,
                                 f"MACD SIGNAL crossed BELOW 0 | {SYMBOL_15M} | 15m | signal={sig:.8f}")

        except Exception as e:
            print(f"[{now_ist_str()}] observer_15m_macd_signal error: {e}")
        await asyncio.sleep(POLL_15M)


# ----- engulfing helpers -----
def candle_body(o: float, c: float) -> float:
    return abs(c - o)

def is_textbook_engulf(prev_o: float, prev_c: float, o: float, c: float) -> Optional[str]:
    # bullish engulf: open_n < close_{n-1} AND close_n > open_{n-1}
    if o < prev_c and c > prev_o:
        return "bullish"
    if o > prev_c and c < prev_o:
        return "bearish"
    return None

def qualifies_watch(prev_o: float, prev_c: float, o: float, c: float, pct: float) -> Optional[str]:
    prev_body = candle_body(prev_o, prev_c)
    curr_body = candle_body(o, c)
    if prev_body <= 0:
        return None
    # need curr_body >= pct * prev_body
    if curr_body >= pct * prev_body:
        # direction heuristics
        if c > o and o <= prev_c:
            return "bullish"
        if c < o and o >= prev_c:
            return "bearish"
    return None

async def observer_5m_engulfing():
    """
    Uses Yahoo 1m data to build the current live 5m candle (intrabar) and also fetches 5m closed bars for confirmations.
    Emits ENGULFING_WATCH (intrabar) and ENGULFING_CONFIRMED (on close).
    """
    last_seen_5m_start: Optional[int] = None

    while True:
        try:
            # 1) Fetch recent 1m candles to reconstruct current 5m live candle
            df_1m = await fetch_yahoo_df(SYMBOL_5M, period="2d", interval="1m")
            if df_1m is None or df_1m.empty:
                print(f"[{now_ist_str()}] observer_5m_engulfing: no 1m data")
                await asyncio.sleep(POLL_1M)
                continue

            # convert to timezone-aware ms list
            idx_ms = df_index_to_ms(df_1m)
            # group into 5m buckets by floor-to-5min epoch
            # compute latest live 5m candle by aggregating last up-to-5 1m candles that belong to same 5m bar
            # timestamp in ms -> floor to 5m start
            def floor_5m(ms: int) -> int:
                return ms - (ms // (5 * 60 * 1000)) * (5 * 60 * 1000)

            rows = list(df_1m.itertuples(index=True, name=None))
            # build list of (ts_ms, o,h,l,c) for each 1m
            one_min_candles: List[Tuple[int, float, float, float, float]] = []
            for ts, o, h, l, c, v in zip(df_1m.index, df_1m["Open"], df_1m["High"], df_1m["Low"], df_1m["Close"], df_1m["Volume"]):
                ts_ms = int(pd.to_datetime(ts).tz_convert(timezone.utc).timestamp() * 1000)
                one_min_candles.append((ts_ms, float(o), float(h), float(l), float(c)))
            if not one_min_candles:
                await asyncio.sleep(POLL_1M)
                continue

            latest_ts = one_min_candles[-1][0]
            curr_5m_start = floor_5m(latest_ts)

            # build live 5m candle for curr_5m_start
            parts = [p for p in one_min_candles if floor_5m(p[0]) == curr_5m_start]
            if parts:
                o_live = parts[0][1]
                h_live = max(p[2] for p in parts)
                l_live = min(p[3] for p in parts)
                c_live = parts[-1][4]
                live_candle = {"o": o_live, "h": h_live, "l": l_live, "c": c_live, "t": curr_5m_start}
                STATE.live_5m = live_candle
            else:
                STATE.live_5m = None

            # 2) Fetch recent closed 5m candles (for prev candle and confirmations)
            df_5m = await fetch_yahoo_df(SYMBOL_5M, period="3d", interval="5m")
            if df_5m is None or df_5m.empty:
                print(f"[{now_ist_str()}] observer_5m_engulfing: no 5m data")
                await asyncio.sleep(POLL_5M)
                continue

            ts_5m = df_index_to_ms(df_5m)
            # last closed 5m starts at ts_5m[-1]; but careful: yahoo 5m df returns only closed bars up to last completed bar
            # ensure indices aligned
            o_list = df_5m["Open"].astype(float).tolist()
            c_list = df_5m["Close"].astype(float).tolist()
            start_ts_list = ts_5m

            # previous closed candle is second-last in this array (since last is most recent closed)
            if len(o_list) >= 2:
                prev_o = float(o_list[-2])
                prev_c = float(c_list[-2])
                prev_start = start_ts_list[-2]
                # store prev_5m in STATE for intrabar watch matches later
                STATE.prev_5m = {"o": prev_o, "c": prev_c, "t": prev_start}
            else:
                STATE.prev_5m = None

            # Detect ENGULFING_WATCH from live candle (intrabar) comparing to prev_5m
            if STATE.prev_5m and STATE.live_5m:
                watch_dir = qualifies_watch(STATE.prev_5m["o"], STATE.prev_5m["c"],
                                            STATE.live_5m["o"], STATE.live_5m["c"], ENGULF_WATCH_PCT)
                # bias filter
                if watch_dir:
                    if (not USE_BIAS_FILTER) or (STATE.last_fast_direction is None) or (STATE.last_fast_direction == watch_dir):
                        idempotent_alert("ENGULF_WATCH", "5m", STATE.live_5m["t"],
                                         f"ENGULFING_WATCH ({watch_dir}) forming (intrabar) | {SYMBOL_5M} | 5m | live_body={abs(STATE.live_5m['c']-STATE.live_5m['o']):.8f} prev_body={abs(STATE.prev_5m['c']-STATE.prev_5m['o']):.8f}",
                                         extra=watch_dir)

            # Detect ENGULFING_CONFIRMED on last closed 5m: check prev vs last closed bar
            if len(o_list) >= 2:
                # last closed bar is last element (index -1)
                last_o = float(o_list[-1])
                last_c = float(c_list[-1])
                last_start = start_ts_list[-1]
                direction = is_textbook_engulf(prev_o, prev_c, last_o, last_c)
                if direction:
                    if (not USE_BIAS_FILTER) or (STATE.last_fast_direction is None) or (STATE.last_fast_direction == direction):
                        idempotent_alert("ENGULF_CONFIRMED", "5m", last_start,
                                         f"ENGULFING_CONFIRMED ({direction}) | {SYMBOL_5M} | 5m | o={last_o:.2f} c={last_c:.2f} prev_o={prev_o:.2f} prev_c={prev_c:.2f}",
                                         extra=direction)

        except Exception as e:
            print(f"[{now_ist_str()}] observer_5m_engulfing error: {e}")

        # staggered sleep: keep 1m polling frequent for intrabar, but 5m checks slower
        await asyncio.sleep(POLL_1M)


# -----------------------
# STARTUP & FLASK
# -----------------------
app = Flask(__name__)

@app.get("/")
def home():
    bias = STATE.last_fast_direction or "none"
    return f"✅ Bot alive. Bias={bias} | ENGULF_WATCH_PCT={ENGULF_WATCH_PCT} | USE_BIAS_FILTER={USE_BIAS_FILTER}", 200

@app.get("/health")
def health():
    return "ok", 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

async def startup_notify():
    ts = int(time.time() * 1000)
    idempotent_alert("SYSTEM", "startup", ts, f"Bot started (Yahoo data) | {SYMBOL_5M} | ENGULF_WATCH_PCT={ENGULF_WATCH_PCT} | USE_BIAS_FILTER={USE_BIAS_FILTER}")

async def main_async():
    # create aiohttp session
    await ensure_session()
    await startup_notify()
    # spawn observers concurrently
    await asyncio.gather(
        observer_15m_macd_fast(),
        observer_15m_macd_signal(),
        observer_5m_engulfing(),
    )

def main():
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
