import os
import requests
import pandas as pd
import pandas_ta as ta
import ccxt
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

# ================= CONFIG =================
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY")
SYMBOL = "BTC/USDT"
EXCHANGE = ccxt.binance()
TIMEFRAME = "15m"
LOOKBACK = 100

# Track last states to prevent duplicate alerts
last_macd_state = {"early": None, "confirm": None}

# ================= HELPERS =================
async def send_discord_message(message: str):
    if not DISCORD_WEBHOOK:
        print("[WARN] Discord webhook not configured")
        return

    # chunking to avoid Discord 2000 char limit
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    async with aiohttp.ClientSession() as session:
        for chunk in chunks:
            async with session.post(DISCORD_WEBHOOK, json={"content": chunk}) as resp:
                if resp.status != 204:
                    print(f"[ERROR] Discord webhook failed: {resp.status}")

async def fetch_news():
    url = f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}&currencies=BTC"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("results", [])

async def fetch_ohlcv():
    ohlcv = EXCHANGE.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=LOOKBACK)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")  # IST conversion
    return df

# ================= INDICATOR LOGIC =================
def check_macd_signals(df: pd.DataFrame):
    global last_macd_state

    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    df = pd.concat([df, macd], axis=1)

    # Latest values
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    macd_val = latest["MACD_12_26_9"]
    macd_prev = prev["MACD_12_26_9"]

    signals = []

    # ---- Early Warning ----
    if macd_prev < 0 and macd_val >= 0:
        if last_macd_state["early"] != "bullish":
            signals.append("⚠️ Early Warning: MACD crossing ABOVE 0 (intracandle)")
            last_macd_state["early"] = "bullish"
    elif macd_prev > 0 and macd_val <= 0:
        if last_macd_state["early"] != "bearish":
            signals.append("⚠️ Early Warning: MACD crossing BELOW 0 (intracandle)")
            last_macd_state["early"] = "bearish"

    # ---- Confirmation (on candle close) ----
    # Here, we confirm when the latest CLOSED candle is above/below 0
    if macd_val > 0:
        if last_macd_state["confirm"] != "bullish":
            signals.append("✅ Confirmation: MACD CLOSED above 0")
            last_macd_state["confirm"] = "bullish"
    elif macd_val < 0:
        if last_macd_state["confirm"] != "bearish":
            signals.append("✅ Confirmation: MACD CLOSED below 0")
            last_macd_state["confirm"] = "bearish"

    return signals

# ================= MAIN LOOP =================
async def monitor():
    while True:
        try:
            df = await asyncio.to_thread(fetch_ohlcv)
            signals = check_macd_signals(df)

            for signal in signals:
                ts = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
                await send_discord_message(f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] {signal}")

            # news alerts
            news_items = await fetch_news()
            for n in news_items[:3]:  # limit to 3 latest
                title = n.get("title", "")
                url = n.get("url", "")
                await send_discord_message(f"📰 News: {title}\n{url}")

        except Exception as e:
            print(f"[ERROR] {e}")

        await asyncio.sleep(60)  # check every 1m

if __name__ == "__main__":
    asyncio.run(monitor())
