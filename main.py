# main.py
import os
import asyncio
import json
import math
import time
import random
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set

import aiohttp
import pandas as pd
import pandas_ta as ta
import websockets
from flask import Flask

# -----------------------
# CONFIG / ENV
# -----------------------
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
PORT = int(os.environ.get("PORT", "10000"))
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT").strip().lower()  # Binance format
USE_BIAS_FILTER = os.environ.get("USE_BIAS_FILTER", "true").lower() in ("1", "true", "yes")
ENGULF_WATCH_PCT = float(os.environ.get("ENGULF_WATCH_PCT", "1.0"))  # 1.0 => 100% of prior body
MACD_FAST = int(os.environ.get("MACD_FAST", "8"))
MACD_SLOW = int(os.environ.get("MACD_SLOW", "15"))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", "9"))

# Binance combined websocket URL for 5m and 15m klines
BINANCE_WS = f"wss://stream.binance.com:9443/stream?streams={SYMBOL}@kline_5m/{SYMBOL}@kline_15m"

# timezone display (IST)
IST = timezone(timedelta(hours=5, minutes=30))

# -----------------------
# STATE
# -----------------------
class State:
    def __init__(self):
        # store last closed candles
        self.closes_15m: List[float] = []
        self.timestamps_15m: List[int] = []
        self.last_fast_direction: Optional[str] = None  # "bullish" or "bearish" based on last fast zero-cross
        # 5m candles: previous closed and current open/live
        self.prev_5m = None  # dict with o,h,l,c,t
        self.curr_5m = None  # dict with o,h,l,c,t (latest update; closed when 'x' True)
        # idempotency
        self.sent_event_keys: Set[str] = set()
        # aiohttp session
        self.session: Optional[aiohttp.ClientSession] = None

STATE = State()

# -----------------------
# UTIL
# -----------------------
def now_ist() -> str:
    return datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

async def ensure_session() -> aiohttp.ClientSession:
    if STATE.session and not STATE.session.closed:
        return STATE.session
    STATE.session = aiohttp.ClientSession()
    return STATE.session

async def send_discord_message(content: str):
    if not DISCORD_WEBHOOK:
        print(f"[{now_ist()}] [ALERT - NO WEBHOOK] {content}")
        return
    session = await ensure_session()
    async def _post():
        async with session.post(DISCORD_WEBHOOK, json={"content": content}) as resp:
            if resp.status not in (200, 204):
                text = await resp.text()
                raise RuntimeError(f"Discord returned {resp.status}: {text}")
    # naive retry
    for attempt in range(3):
        try:
            await _post()
            return
        except Exception as e:
            print(f"[{now_ist()}] Discord send failed (attempt {attempt+1}): {e}")
            await asyncio.sleep(0.8 * (2 ** attempt))
    print(f"[{now_ist()}] Discord send ultimately failed: {content}")

def event_key(kind: str, tf: str, bar_time: int, extra: Optional[str] = None) -> str:
    if extra:
        return f"{kind}|{tf}|{bar_time}|{extra}"
    return f"{kind}|{tf}|{bar_time}"

def idempotent_send(kind: str, tf: str, bar_time: int, text: str, extra: Optional[str] = None):
    key = event_key(kind, tf, bar_time, extra)
    if key in STATE.sent_event_keys:
        # already sent
        return
    STATE.sent_event_keys.add(key)
    # fire and forget
    asyncio.create_task(send_discord_message(f"[{now_ist()}] {text}"))

# -----------------------
# MACD 15m logic
# -----------------------
def compute_macd_from_closes(closes: List[float]) -> Optional[pd.DataFrame]:
    # needs at least (max window * 3) but we will guard earlier
    if len(closes) < max(MACD_SLOW, MACD_SIGNAL) + 3:
        return None
    s = pd.Series(closes)
    macd = ta.macd(s, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
    # macd columns: MACD_{fast}_{slow}_{signal}, MACDh, MACDs...
    return macd

def check_15m_macd_crosses():
    # Called when a 15m bar closed and STATE.closes_15m updated
    macd = compute_macd_from_closes(STATE.closes_15m)
    if macd is None or macd.empty:
        return
    df = macd.dropna()
    if len(df) < 2:
        return

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    # column names come from pandas_ta, find the MACD line and signal names generically
    macd_line_col = [c for c in macd.columns if c.startswith("MACD_") and "_" in c]
    signal_col = [c for c in macd.columns if c.startswith("MACDs_")]
    if not macd_line_col or not signal_col:
        # fallback: names vary; try heuristics
        macd_line_col = [c for c in macd.columns if "MACD" in c and "MACDs" not in c]
        signal_col = [c for c in macd.columns if "MACDs" in c]
    if not macd_line_col or not signal_col:
        print(f"[{now_ist()}] Unable to identify MACD columns: {macd.columns}")
        return

    macd_line = float(latest[macd_line_col[0]])
    macd_prev = float(prev[macd_line_col[0]])
    signal_line = float(latest[signal_col[0]])
    signal_prev = float(prev[signal_col[0]])

    # FAST (MACD line) zero-cross detection (crossing zero between prev and latest)
    bar_time = STATE.timestamps_15m[-1] if STATE.timestamps_15m else int(time.time())
    if macd_prev < 0 <= macd_line:
        # crossed above
        idempotent_send("FAST_CROSS", "15m", bar_time, f"MACD FAST crossed ABOVE 0 | BTC/USDT | 15m | macd={macd_line:.8f}")
        STATE.last_fast_direction = "bullish"
    elif macd_prev > 0 >= macd_line:
        idempotent_send("FAST_CROSS", "15m", bar_time, f"MACD FAST crossed BELOW 0 | BTC/USDT | 15m | macd={macd_line:.8f}")
        STATE.last_fast_direction = "bearish"

    # SIGNAL (slow) zero-cross detection
    if signal_prev < 0 <= signal_line:
        idempotent_send("SIGNAL_CROSS", "15m", bar_time, f"MACD SIGNAL crossed ABOVE 0 | BTC/USDT | 15m | signal={signal_line:.8f}")
    elif signal_prev > 0 >= signal_line:
        idempotent_send("SIGNAL_CROSS", "15m", bar_time, f"MACD SIGNAL crossed BELOW 0 | BTC/USDT | 15m | signal={signal_line:.8f}")

# -----------------------
# 5m engulfing logic
# -----------------------
def candle_body(o: float, c: float) -> float:
    return abs(c - o)

def is_textbook_engulf(prev_o, prev_c, o, c) -> Optional[str]:
    # returns "bullish"/"bearish"/None per textbook strict rules
    # bullish engulfing: open_n < close_{n-1} AND close_n > open_{n-1}
    if o < prev_c and c > prev_o:
        return "bullish"
    # bearish engulfing: open_n > close_{n-1} AND close_n < open_{n-1}
    if o > prev_c and c < prev_o:
        return "bearish"
    return None

def check_engulfing_watch_and_confirm(prev_candle: Dict, curr_candle: Dict, closed: bool):
    """
    prev_candle and curr_candle format: { 'o': float, 'h': float, 'l': float, 'c': float, 't': int }
    'closed' indicates whether curr_candle is a closed bar (True) or intrabar update (False)
    """
    if not prev_candle or not curr_candle:
        return

    prev_o, prev_c = float(prev_candle["o"]), float(prev_candle["c"])
    o, c = float(curr_candle["o"]), float(curr_candle["c"])
    bar_time = curr_candle["t"]

    prev_body = candle_body(prev_o, prev_c)
    curr_body = candle_body(o, c)

    # intrabar watch: does current *live* body already engulf previous body by >= ENGULF_WATCH_PCT?
    # define engulf as curr_body >= ENGULF_WATCH_PCT * prev_body AND
    # the current midpoints indicate directional overlap with prior body.
    if prev_body > 0 and curr_body >= ENGULF_WATCH_PCT * prev_body and not closed:
        # determine direction by comparing midpoint movement (simpler heuristic)
        if (c > o) and (o <= prev_c):
            direction = "bullish"
        elif (c < o) and (o >= prev_c):
            direction = "bearish"
        else:
            direction = None
        if direction:
            # bias filter when enabled
            if USE_BIAS_FILTER and STATE.last_fast_direction and STATE.last_fast_direction != direction:
                return
            idempotent_send("ENGULF_WATCH", "5m", bar_time, f"ENGULFING_WATCH ({direction}) forming (intrabar) | BTC/USDT | 5m | curr_body={curr_body:.8f} prev_body={prev_body:.8f}")

    # on close, check textbook engulf
    if closed:
        direction = is_textbook_engulf(prev_o, prev_c, o, c)
        if direction:
            if USE_BIAS_FILTER and STATE.last_fast_direction and STATE.last_fast_direction != direction:
                return
            # send confirmed engulfing alert
            idempotent_send("ENGULF_CONFIRMED", "5m", bar_time, f"ENGULFING_CONFIRMED ({direction}) | BTC/USDT | 5m | open={o:.2f} close={c:.2f} prev_open={prev_o:.2f} prev_close={prev_c:.2f}")

# -----------------------
# WebSocket handling
# -----------------------
async def handle_combined_binance():
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20, ping_timeout=10) as ws:
                print(f"[{now_ist()}] Connected to Binance WS")
                backoff = 1.0
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    # Combined stream format: {"stream":"btcusdt@kline_5m","data":{...}}
                    stream = data.get("stream") or ""
                    payload = data.get("data") or data
                    # kline payload lives in payload['k']
                    k = payload.get("k") or {}
                    if not k:
                        continue
                    is_closed = bool(k.get("x", False))
                    tf = "5m" if "kline_5m" in stream else "15m" if "kline_15m" in stream else None
                    if tf == "15m":
                        # closed bars only for MACD calculations
                        if is_closed:
                            # k has t (start time), o,h,l,c,v
                            o = float(k["o"]); c = float(k["c"])
                            ts = int(k["t"])  # ms
                            # append close
                            STATE.closes_15m.append(c)
                            STATE.timestamps_15m.append(ts)
                            # keep lookback reasonable (300)
                            if len(STATE.closes_15m) > 500:
                                STATE.closes_15m = STATE.closes_15m[-500:]
                                STATE.timestamps_15m = STATE.timestamps_15m[-500:]
                            # compute macd crosses
                            try:
                                check_15m_macd_crosses()
                            except Exception as e:
                                print(f"[{now_ist()}] Error in MACD check: {e}")
                    elif tf == "5m":
                        # build prev/current candlesticks and feed to engulfing logic
                        o = float(k["o"]); c = float(k["c"]); h = float(k["h"]); l = float(k["l"]); ts = int(k["t"])
                        # if bar closed, move curr -> prev, set curr to closed bar
                        if is_closed:
                            # previous closed becomes prev
                            prev = STATE.curr_5m if STATE.curr_5m else STATE.prev_5m
                            closed_candle = {"o": o, "h": h, "l": l, "c": c, "t": ts}
                            # feed confirm: prev (previous closed) vs closed_candle
                            if prev:
                                try:
                                    check_engulfing_watch_and_confirm(prev, closed_candle, closed=True)
                                except Exception as e:
                                    print(f"[{now_ist()}] Error in engulf confirm: {e}")
                            # shift prev and curr
                            STATE.prev_5m = closed_candle
                            STATE.curr_5m = None
                        else:
                            # intrabar update: k contains current live open and current close
                            live_candle = {"o": o, "h": h, "l": l, "c": c, "t": ts}
                            # compare with previous closed (STATE.prev_5m)
                            try:
                                check_engulfing_watch_and_confirm(STATE.prev_5m, live_candle, closed=False)
                            except Exception as e:
                                print(f"[{now_ist()}] Error in engulf watch: {e}")
                            STATE.curr_5m = live_candle
                    # else: ignore
        except Exception as e:
            print(f"[{now_ist()}] Binance WS error: {type(e).__name__}: {e}. Reconnecting in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8 + random.random() * 0.5, 60.0)

# -----------------------
# STARTUP / FLASK
# -----------------------
app = Flask(__name__)

@app.get("/")
def home():
    bias = STATE.last_fast_direction or "none"
    return f"✅ Bot alive. Symbol={SYMBOL} | Bias={bias} | USE_BIAS_FILTER={USE_BIAS_FILTER}", 200

@app.get("/health")
def health():
    return "ok", 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

async def startup_probe():
    # small startup message
    idempotent_send("SYSTEM", "startup", int(time.time()*1000), f"Bot starting on Binance WS | SYMBOL={SYMBOL} | ENGULF_WATCH_PCT={ENGULF_WATCH_PCT} | USE_BIAS_FILTER={USE_BIAS_FILTER}")

async def main_async():
    # create session
    await ensure_session()
    await startup_probe()
    # run websocket handler
    await handle_combined_binance()

def main():
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
