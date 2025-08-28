import os
import asyncio
import time
import random
import threading
from datetime import datetime, timezone, timedelta

import aiohttp
import pandas as pd
import pandas_ta as ta
from flask import Flask

#  Configuration
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
PORT = int(os.environ.get("PORT", "10000"))
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
USE_BIAS_FILTER = os.environ.get("USE_BIAS_FILTER", "true").lower() in ("1","true","yes")
ENGULF_WATCH_PCT = float(os.environ.get("ENGULF_WATCH_PCT", "1.0"))
MACD_FAST = int(os.environ.get("MACD_FAST", "8"))
MACD_SLOW = int(os.environ.get("MACD_SLOW", "15"))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", "9"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))

IST = timezone(timedelta(hours=5, minutes=30))
STATE = {
    "closes_15m": [],
    "last_fast_direction": None,
    "sent_keys": set(),
    "session": None,
}

async def now_ist(): return datetime.now(timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

async def ensure_session():
    if STATE["session"] and not STATE["session"].closed:
        return STATE["session"]
    STATE["session"] = aiohttp.ClientSession()
    return STATE["session"]

async def send_discord(text):
    session = await ensure_session()
    if not DISCORD_WEBHOOK:
        print(f"[{await now_ist()}] {text}")
        return
    for attempt in range(3):
        try:
            async with session.post(DISCORD_WEBHOOK, json={"content": text}) as resp:
                if resp.status in (200,204): return
                if resp.status == 429:
                    await asyncio.sleep((2 ** attempt) + random.random())
                    continue
                text_body = await resp.text()
                print("Discord error:", resp.status, text_body)
                return
        except Exception as e:
            await asyncio.sleep((2 ** attempt) + random.random())

def idempotent(kind, tf, ts, msg):
    key = f"{kind}|{tf}|{ts}"
    if key in STATE["sent_keys"]: return
    STATE["sent_keys"].add(key)
    asyncio.create_task(send_discord(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"))

async def fetch_klines():
    session = await ensure_session()
    url_bin = f"https://data.binance.com/api/v3/klines?symbol={SYMBOL}&interval=15m&limit=100"
    try:
        async with session.get(url_bin, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            if resp.status == 451:
                raise ValueError("Binance blocked")
    except Exception:
        # fallback to Coinbase
        url_cb = f"https://api.pro.coinbase.com/products/BTC-USD/candles?granularity=900"
        async with session.get(url_cb, timeout=10) as resp:
            data = await resp.json()
            # Coinbase returns [time, low, high, open, close, volume]
            return [[d[0]*1000, d[3], d[2], d[1], d[4], d[5]] for d in data]

async def check_loop():
    while True:
        data = await fetch_klines()
        # each item: [open_time, o, h, l, c, v]
        closes = [float(d[4]) for d in data]
        timestamps = [d[0] for d in data]
        STATE["closes_15m"] = closes
        if len(closes) >= max(MACD_FAST, MACD_SLOW) + 2:
            s = pd.Series(closes)
            macd = ta.macd(s, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL).dropna()
            if len(macd) >= 2:
                prev = macd.iloc[-2][0]
                now = macd.iloc[-1][0]
                bar_ts = timestamps[-1]
                if prev < 0 <= now:
                    STATE["last_fast_direction"] = "bullish"
                    idempotent("fast_cross", "15m", bar_ts, f"MACD FAST crossed ABOVE 0 ({now:.8f})")
                elif prev > 0 >= now:
                    STATE["last_fast_direction"] = "bearish"
                    idempotent("fast_cross", "15m", bar_ts, f"MACD FAST crossed BELOW 0 ({now:.8f})")
        await asyncio.sleep(POLL_INTERVAL)

app = Flask(__name__)
@app.get("/")
def home():
    return f"Bot running | Symbol={SYMBOL} | Bias={STATE['last_fast_direction']}"

def run_flask(): app.run(host="0.0.0.0", port=PORT)

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(check_loop())

if __name__ == "__main__":
    main()
