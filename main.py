import asyncio
import datetime
import pandas as pd
import yfinance as yf
import requests

# ================== CONFIG ==================
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"
TICKER = "BTC-USD"   # works with Yahoo Finance
MACD_FAST = 8
MACD_SLOW = 15
MACD_SIGNAL = 9
ENGULFING_FILTER = True  # only in direction of 15m MACD cross
# ============================================


def send_discord_alert(message: str):
    """Send message to Discord webhook"""
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"[Discord Error] {e}")


def get_ohlc(ticker: str, interval: str, lookback: str):
    """Download OHLCV from Yahoo"""
    data = yf.download(ticker, interval=interval, period=lookback)
    return data


def calculate_macd(df: pd.DataFrame):
    """Compute MACD and signal lines"""
    df["EMA_fast"] = df["Close"].ewm(span=MACD_FAST, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    return df


def detect_engulfing(df: pd.DataFrame):
    """Detect engulfing candles"""
    engulfings = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        prev_body = prev["Close"] - prev["Open"]
        curr_body = curr["Close"] - curr["Open"]

        # Bullish engulfing
        if prev_body < 0 and curr_body > 0 and curr["Close"] > prev["Open"] and curr["Open"] < prev["Close"]:
            engulfings.append((curr.name, "bullish"))

        # Bearish engulfing
        elif prev_body > 0 and curr_body < 0 and curr["Close"] < prev["Open"] and curr["Open"] > prev["Close"]:
            engulfings.append((curr.name, "bearish"))

    return engulfings


# ============= OBSERVERS =============

async def macd_observer():
    """Observe 15m MACD fast and signal line crosses"""
    last_fast_cross = None
    last_signal_cross = None

    while True:
        df = get_ohlc(TICKER, "15m", "2d")
        df = calculate_macd(df)

        if len(df) < 2:
            await asyncio.sleep(60)
            continue

        prev, curr = df.iloc[-2], df.iloc[-1]

        # Fast zero-cross
        if prev["MACD"] <= 0 < curr["MACD"]:
            last_fast_cross = "bullish"
            send_discord_alert("🚀 MACD FAST crossed ABOVE zero (15m) → bullish bias")
        elif prev["MACD"] >= 0 > curr["MACD"]:
            last_fast_cross = "bearish"
            send_discord_alert("🔻 MACD FAST crossed BELOW zero (15m) → bearish bias")

        # Signal zero-cross
        if prev["MACD"] <= prev["Signal"] and curr["MACD"] > curr["Signal"]:
            last_signal_cross = "bullish"
            send_discord_alert("⚡ MACD crossed ABOVE signal (15m) → bullish")
        elif prev["MACD"] >= prev["Signal"] and curr["MACD"] < curr["Signal"]:
            last_signal_cross = "bearish"
            send_discord_alert("⚡ MACD crossed BELOW signal (15m) → bearish")

        await asyncio.sleep(60)


async def engulfing_observer():
    """Observe 5m engulfing patterns"""
    last_fast_bias = None

    while True:
        # Get MACD bias (from 15m observer)
        df15 = get_ohlc(TICKER, "15m", "2d")
        df15 = calculate_macd(df15)
        prev, curr = df15.iloc[-2], df15.iloc[-1]
        if prev["MACD"] <= 0 < curr["MACD"]:
            last_fast_bias = "bullish"
        elif prev["MACD"] >= 0 > curr["MACD"]:
            last_fast_bias = "bearish"

        # Detect engulfings
        df5 = get_ohlc(TICKER, "5m", "1d")
        engulfings = detect_engulfing(df5)

        if engulfings:
            t, direction = engulfings[-1]

            # Filter if enabled
            if ENGULFING_FILTER and last_fast_bias and direction != last_fast_bias:
                pass  # skip mismatched direction
            else:
                msg = f"📊 ENGULFING CONFIRMED ({direction}) on 5m at {t}"
                send_discord_alert(msg)

        await asyncio.sleep(60)


# ============= MAIN =============

async def main():
    await asyncio.gather(
        macd_observer(),
        engulfing_observer(),
    )

if __name__ == "__main__":
    asyncio.run(main())

