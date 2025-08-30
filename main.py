import asyncio
import ccxt
import finnhub
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import logging
import os
import aiohttp
from aiohttp import web

# ------------------------------
# Logging Setup
# ------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------------------
# Config
# ------------------------------
SYMBOL = "BTC/USDT"
INTERVAL = "15m"
LOOKBACK = 50
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

# Finnhub client (fallback)
finnhub_client = finnhub.Client(api_key="d2pi0npr01qnf9nl5mkgd2pi0npr01qnf9nl5ml0")


# Binance via CCXT
exchange = ccxt.binance()

# ------------------------------
# Technical Indicators
# ------------------------------
def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def macd(df, fast=8, slow=15, signal=9):
    df["ema_fast"] = ema(df["close"], fast)
    df["ema_slow"] = ema(df["close"], slow)
    df["macd"] = df["ema_fast"] - df["ema_slow"]
    df["signal"] = ema(df["macd"], signal)
    return df

def detect_engulfing(df):
    signals = []
    for i in range(1, len(df)):
        prev = df.iloc[i-1]
        curr = df.iloc[i]
        if curr["close"] > curr["open"] and prev["close"] < prev["open"]:
            if curr["close"] > prev["open"] and curr["open"] < prev["close"]:
                signals.append("bullish_engulfing")
        elif curr["close"] < curr["open"] and prev["close"] > prev["open"]:
            if curr["close"] < prev["open"] and curr["open"] > prev["close"]:
                signals.append("bearish_engulfing")
        else:
            signals.append(None)
    df["engulfing"] = [None] + signals
    return df

# ------------------------------
# Data Fetching
# ------------------------------
async def fetch_binance():
    """Fetch candles from Binance using CCXT"""
    try:
        ohlcv = exchange.fetch_ohlcv(SYMBOL, INTERVAL, limit=LOOKBACK)
        df = pd.DataFrame(ohlcv, columns=["time","open","high","low","close","volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"Binance fetch failed: {e}")
        return None

async def fetch_finnhub():
    """Fallback: fetch candles from Finnhub"""
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        frm = now - LOOKBACK * 15 * 60
        res = finnhub_client.crypto_candles("BINANCE:BTCUSDT", "15", frm, now)
        if res.get("s") != "ok":
            return None
        df = pd.DataFrame({
            "time": pd.to_datetime(res["t"], unit="s", utc=True),
            "open": res["o"], "high": res["h"], "low": res["l"], "close": res["c"], "volume": res["v"]
        })
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
        return df
    except Exception as e:
        logging.error(f"Finnhub fetch failed: {e}")
        return None

async def get_data():
    df = await fetch_binance()
    if df is None:
        df = await fetch_finnhub()
    return df

# ------------------------------
# Alerts
# ------------------------------
async def send_discord(message):
    if not DISCORD_WEBHOOK:
        return
    async with aiohttp.ClientSession() as session:
        try:
            await session.post(DISCORD_WEBHOOK, json={"content": message})
        except Exception as e:
            logging.error(f"Discord send failed: {e}")

# ------------------------------
# Observer
# ------------------------------
async def analyze():
    df = await get_data()
    if df is None or len(df) < LOOKBACK:
        logging.warning("Not enough data.")
        return

    df = macd(df)
    df = detect_engulfing(df)
    last = df.iloc[-1]

    msg = []
    if last["macd"] > last["signal"]:
        msg.append("MACD bullish")
    elif last["macd"] < last["signal"]:
        msg.append("MACD bearish")

    if last["engulfing"] == "bullish_engulfing":
        msg.append("Bullish engulfing")
    elif last["engulfing"] == "bearish_engulfing":
        msg.append("Bearish engulfing")

    if msg:
        message = f"Signal @ {last['time']}: {', '.join(msg)}"
        logging.info(message)
        await send_discord(message)

# ------------------------------
# Healthcheck Server
# ------------------------------
async def health(request):
    return web.Response(text="OK")

async def start_server():
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

# ------------------------------
# Main Loop
# ------------------------------
async def main():
    await start_server()
    while True:
        try:
            await analyze()
        except Exception as e:
            logging.error(f"Main loop error: {e}")
        await asyncio.sleep(60)  # 1 call per min

if __name__ == "__main__":
    asyncio.run(main())


