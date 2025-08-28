import os
import asyncio
from binance
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta

# ============ CONFIG ============
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"  # <--- replace or set in Render
)
TICKER = os.environ.get("TICKER", "BTC-USD")
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", 60))  # seconds

# ============ DISCORD ============
def send_discord_alert(message: str):
    """Send message to Discord via webhook"""
    if not DISCORD_WEBHOOK_URL:
        print("[WARN] No Discord webhook URL configured.")
        return
    try:
        payload = {"content": message}
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            print(f"[ERROR] Discord webhook failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[ERROR] Failed to send Discord alert: {e}")

# ============ TECHNICAL INDICATORS ============
def calculate_macd(df, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
    df["EMA_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=signal, adjust=False).mean()
    df["Histogram"] = df["MACD"] - df["Signal"]
    return df

def detect_engulfing(df):
    df["Engulfing"] = False
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        if curr["Close"] > curr["Open"] and prev["Close"] < prev["Open"]:  # Bullish engulfing
            if curr["Close"] >= prev["Open"] and curr["Open"] <= prev["Close"]:
                df.at[df.index[i], "Engulfing"] = True
        elif curr["Close"] < curr["Open"] and prev["Close"] > prev["Open"]:  # Bearish engulfing
            if curr["Open"] >= prev["Close"] and curr["Close"] <= prev["Open"]:
                df.at[df.index[i], "Engulfing"] = True
    return df

def detect_macd_zero_cross(df):
    df["ZeroCross"] = (df["MACD"] * df["MACD"].shift(1) < 0)
    return df

# ============ DATA FETCH ============
async def fetch_data():
    """Fetch data from Yahoo Finance with retries & fallback period"""
    for period in ["1d", "5d", "7d"]:
        try:
            df = yf.download(TICKER, interval="15m", period=period, progress=False)
            if df.empty:
                print(f"[WARN] No data for {TICKER} with period={period}")
                continue
            df.dropna(inplace=True)
            return df
        except Exception as e:
            print(f"[ERROR] Failed to get ticker '{TICKER}' (period={period}): {e}")
    return pd.DataFrame()

# ============ MAIN LOGIC ============
async def analyze_and_alert():
    df = await fetch_data()
    if df.empty:
        print(f"[ERROR] No data found for {TICKER}. Skipping this cycle.")
        return

    df = calculate_macd(df)
    df = detect_engulfing(df)
    df = detect_macd_zero_cross(df)

    latest = df.iloc[-1]
    ts = latest.name.strftime("%Y-%m-%d %H:%M")

    # MACD cross alert
    if latest["MACD"] > latest["Signal"] and df.iloc[-2]["MACD"] <= df.iloc[-2]["Signal"]:
        send_discord_alert(f"📈 {TICKER} Bullish MACD crossover at {ts}")
    elif latest["MACD"] < latest["Signal"] and df.iloc[-2]["MACD"] >= df.iloc[-2]["Signal"]:
        send_discord_alert(f"📉 {TICKER} Bearish MACD crossover at {ts}")

    # Zero line cross
    if latest["ZeroCross"]:
        direction = "above" if latest["MACD"] > 0 else "below"
        send_discord_alert(f"⚡ {TICKER} MACD crossed {direction} zero line at {ts}")

    # Engulfing pattern
    if ENGULFING_FILTER and latest["Engulfing"]:
        send_discord_alert(f"🕯️ {TICKER} Engulfing pattern detected at {ts}")

# ============ LOOP ============
async def main():
    send_discord_alert("🤖 Trading bot is now LIVE and monitoring signals...")
    while True:
        try:
            await analyze_and_alert()
        except Exception as e:
            print(f"[ERROR] Main loop error: {e}")
        await asyncio.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())

