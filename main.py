import os
import asyncio
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from binance import AsyncClient

# ============ CONFIG ============
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL","https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR")
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", "15m")
LOOKBACK = os.environ.get("LOOKBACK", "7 days ago UTC")

MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
SMA_WINDOW = int(os.environ.get("SMA_WINDOW", 20))
RSI_WINDOW = int(os.environ.get("RSI_WINDOW", 14))
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

# ============ DATA FETCH ============
async def fetch_binance_klines(symbol=SYMBOL, interval=INTERVAL, lookback=LOOKBACK):
    """Fetch historical klines from Binance"""
    client = await AsyncClient.create()
    try:
        klines = await client.get_historical_klines(symbol, interval, lookback)
        if not klines:
            return pd.DataFrame()

        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "qav", "num_trades", "taker_base", "taker_quote", "ignore"
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
        df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        df.rename(columns=str.capitalize, inplace=True)  # 'Open', 'High', 'Low', 'Close', 'Volume'
        return df
    except Exception as e:
        print(f"[ERROR] Binance fetch failed: {e}")
        return pd.DataFrame()
    finally:
        await client.close_connection()

# ============ TECHNICAL INDICATORS ============
def calculate_sma(df, window=SMA_WINDOW):
    df[f"SMA_{window}"] = df["Close"].rolling(window=window).mean()
    return df

def calculate_rsi(df, window=RSI_WINDOW):
    delta = df["Close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=window).mean()
    avg_loss = pd.Series(loss).rolling(window=window).mean()
    rs = avg_gain / (avg_loss + 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

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
        if curr["Close"] > curr["Open"] and prev["Close"] < prev["Open"]:  # Bullish
            if curr["Close"] >= prev["Open"] and curr["Open"] <= prev["Close"]:
                df.at[df.index[i], "Engulfing"] = True
        elif curr["Close"] < curr["Open"] and prev["Close"] > prev["Open"]:  # Bearish
            if curr["Open"] >= prev["Close"] and curr["Close"] <= prev["Open"]:
                df.at[df.index[i], "Engulfing"] = True
    return df

def detect_macd_zero_cross(df):
    df["ZeroCross"] = (df["MACD"] * df["MACD"].shift(1) < 0)
    return df

# ============ SIGNAL GENERATION ============
def generate_signals(df):
    df = calculate_sma(df)
    df = calculate_rsi(df)
    df = calculate_macd(df)
    df = detect_engulfing(df)
    df = detect_macd_zero_cross(df)

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    ts = latest["open_time"].strftime("%Y-%m-%d %H:%M") if "open_time" in df.columns else datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    # Alerts
    if latest["MACD"] > latest["Signal"] and prev["MACD"] <= prev["Signal"]:
        send_discord_alert(f"📈 {SYMBOL} Bullish MACD crossover at {ts}")
    elif latest["MACD"] < latest["Signal"] and prev["MACD"] >= prev["Signal"]:
        send_discord_alert(f"📉 {SYMBOL} Bearish MACD crossover at {ts}")

    if latest["ZeroCross"]:
        direction = "above" if latest["MACD"] > 0 else "below"
        send_discord_alert(f"⚡ {SYMBOL} MACD crossed {direction} zero line at {ts}")

    if ENGULFING_FILTER and latest["Engulfing"]:
        send_discord_alert(f"🕯️ {SYMBOL} Engulfing pattern detected at {ts}")

    if latest["RSI"] < 30 and latest["Close"] > latest[f"SMA_{SMA_WINDOW}"]:
        send_discord_alert(f"✅ {SYMBOL} RSI oversold bounce at {ts}")

    if latest["RSI"] > 70 and latest["Close"] < latest[f"SMA_{SMA_WINDOW}"]:
        send_discord_alert(f"⚠️ {SYMBOL} RSI overbought drop at {ts}")

# ============ LOOP ============
async def main():
    send_discord_alert(f"🤖 Binance Trading bot LIVE - Monitoring {SYMBOL}...")
    while True:
        try:
            df = await fetch_binance_klines()
            if df.empty:
                print("[ERROR] No market data - retrying...")
            else:
                generate_signals(df)
        except Exception as e:
            print(f"[ERROR] Main loop exception: {e}")
        await asyncio.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
