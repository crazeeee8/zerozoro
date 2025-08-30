import asyncio
import yfinance as yf
import pandas as pd
import numpy as np
import logging
import signal
import sys
import aiohttp
import time
from datetime import datetime, timedelta
from aiohttp import web
from pandas_datareader import data as pdr

# ------------------------------
# Logging Setup
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ------------------------------
# Config
# ------------------------------
TICKER = "BTC-USD"
INTERVAL = "1h"
LOOKBACK = 100
DISCORD_WEBHOOK_URL = "YOUR_DISCORD_WEBHOOK"

# ------------------------------
# Global State
# ------------------------------
shutdown_flag = False
last_macd_signal = None
last_check_time = None

# ------------------------------
# Graceful Shutdown
# ------------------------------
def handle_shutdown(signum, frame):
    global shutdown_flag
    logging.info("Shutdown signal received")
    shutdown_flag = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# ------------------------------
# Dual-Source Price Fetcher
# ------------------------------
def get_price_data(ticker=TICKER, period="1mo", interval=INTERVAL):
    try:
        logging.info("Fetching data via yfinance...")
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        if df is None or df.empty:
            raise ValueError("Empty dataframe from yfinance")
        return df
    except Exception as e:
        logging.warning(f"yfinance failed: {e}. Trying pandas_datareader...")
        try:
            end = datetime.now()
            start = end - timedelta(days=30)
            df = pdr.get_data_yahoo(ticker, start=start, end=end)
            if df is None or df.empty:
                raise ValueError("Empty dataframe from pandas_datareader")
            df = df.rename(columns={
                "Open": "Open",
                "High": "High",
                "Low": "Low",
                "Close": "Close",
                "Volume": "Volume"
            })
            return df
        except Exception as e2:
            logging.error(f"pandas_datareader also failed: {e2}")
            return None

# ------------------------------
# Indicators
# ------------------------------
def calculate_macd(df, fast=12, slow=26, signal=9):
    df["EMA_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=signal, adjust=False).mean()
    return df

# ------------------------------
# Alerts
# ------------------------------
async def send_discord_alert(message: str):
    try:
        async with aiohttp.ClientSession() as session:
            webhook = {"content": message}
            async with session.post(DISCORD_WEBHOOK_URL, json=webhook) as resp:
                if resp.status != 204:
                    logging.error(f"Discord webhook failed: {resp.status}")
    except Exception as e:
        logging.error(f"Failed to send Discord alert: {e}")

# ------------------------------
# Core Logic
# ------------------------------
async def monitor():
    global last_macd_signal, last_check_time
    backoff = 5

    while not shutdown_flag:
        df = get_price_data()

        if df is None or df.empty:
            logging.warning(f"No data fetched. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)
            continue

        backoff = 5  # reset backoff if successful
        df = calculate_macd(df)

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        now = datetime.utcnow()

        # Ensure timestamp-based detection
        if last_check_time is None or last_row.name > last_check_time:
            # MACD cross detection
            if prev_row["MACD"] <= prev_row["Signal"] and last_row["MACD"] > last_row["Signal"]:
                if last_macd_signal != "bullish":
                    msg = f"🚀 Bullish MACD cross detected at {now}"
                    logging.info(msg)
                    await send_discord_alert(msg)
                    last_macd_signal = "bullish"

            elif prev_row["MACD"] >= prev_row["Signal"] and last_row["MACD"] < last_row["Signal"]:
                if last_macd_signal != "bearish":
                    msg = f"🔻 Bearish MACD cross detected at {now}"
                    logging.info(msg)
                    await send_discord_alert(msg)
                    last_macd_signal = "bearish"

            last_check_time = last_row.name

        await asyncio.sleep(60)

# ------------------------------
# Health Server
# ------------------------------
async def health(request):
    return web.Response(text="OK")

async def start_server():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 10000)
    await site.start()

# ------------------------------
# Main
# ------------------------------
async def main():
    await start_server()
    await monitor()

if __name__ == "__main__":
    asyncio.run(main())
