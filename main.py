import asyncio
import datetime
import pandas as pd
import yfinance as yf
import requests

# ================== CONFIG ==================
# IMPORTANT: Replace with your actual Discord webhook URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"
TICKER = "BTC-USD"   # works with Yahoo Finance
MACD_FAST = 8
MACD_SLOW = 15
MACD_SIGNAL = 9
ENGULFING_FILTER = True  # only in direction of 15m MACD cross
# ============================================


# ============= SHARED STATE =============
# A simple dictionary for sharing state between the async observer tasks.
# This prevents redundant API calls and calculations.
shared_state = {
    "last_15m_macd_bias": None,  # Can be 'bullish' or 'bearish'
}


def send_discord_alert(message: str):
    """Send message to Discord webhook."""
    try:
        # Add a timestamp to every message for clarity
        timestamped_message = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        requests.post(DISCORD_WEBHOOK_URL, json={"content": timestamped_message})
    except Exception as e:
        print(f"[Discord Error] {e}")


def get_ohlc(ticker: str, interval: str, lookback: str) -> pd.DataFrame:
    """
    Download OHLCV data from Yahoo Finance.
    Includes error handling and returns an empty DataFrame on failure.
    """
    try:
        # Set progress=False to avoid printing download status in logs
        data = yf.download(ticker, interval=interval, period=lookback, progress=False)
        if data.empty:
            print(f"Warning: No data returned for {ticker} with interval {interval}")
            return pd.DataFrame()
        return data
    except Exception as e:
        print(f"[yfinance Error] Failed to download data for {ticker}: {e}")
        return pd.DataFrame()


def calculate_macd(df: pd.DataFrame) -> pd.DataFrame:
    """Compute MACD and signal lines."""
    df["EMA_fast"] = df["Close"].ewm(span=MACD_FAST, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    return df


def detect_engulfing(df: pd.DataFrame) -> list:
    """
    Detects bullish and bearish engulfing candles.
    Returns a list of tuples, where each tuple contains the timestamp and direction.
    """
    engulfings = []
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        prev_body = prev["Close"] - prev["Open"]
        curr_body = curr["Close"] - curr["Open"]

        # Bullish engulfing: Previous candle is red, current is green and bigger
        if prev_body < 0 and curr_body > 0 and curr["Close"] > prev["Open"] and curr["Open"] < prev["Close"]:
            engulfings.append((curr.name, "bullish"))

        # Bearish engulfing: Previous candle is green, current is red and bigger
        elif prev_body > 0 and curr_body < 0 and curr["Close"] < prev["Open"] and curr["Open"] > prev["Close"]:
            engulfings.append((curr.name, "bearish"))

    return engulfings


# ============= OBSERVERS =============

async def macd_observer():
    """
    Observes 15m MACD for zero-line and signal-line crosses.
    Updates the shared_state with the current bias and sends Discord alerts.
    """
    # State to track the last alerted cross to prevent duplicate messages
    last_alerted_status = {"fast_cross": None, "signal_cross": None}

    while True:
        df = get_ohlc(TICKER, "15m", "2d")
        if len(df) < 2:
            print("MACD Observer: Not enough data to process. Waiting...")
            await asyncio.sleep(60)
            continue

        df = calculate_macd(df)
        prev, curr = df.iloc[-2], df.iloc[-1]

        # --- Check for MACD Fast crossing the zero line ---
        current_fast_cross = None
        # FIX: Use .item() to extract the scalar value for comparison
        if prev["MACD"].item() <= 0 < curr["MACD"].item():
            current_fast_cross = "bullish"
        elif prev["MACD"].item() >= 0 > curr["MACD"].item():
            current_fast_cross = "bearish"

        # If a new cross event happens, send an alert and update state
        if current_fast_cross and current_fast_cross != last_alerted_status["fast_cross"]:
            shared_state["last_15m_macd_bias"] = current_fast_cross
            last_alerted_status["fast_cross"] = current_fast_cross
            
            emoji = "🚀" if current_fast_cross == "bullish" else "🔻"
            direction_msg = "ABOVE" if current_fast_cross == "bullish" else "BELOW"
            send_discord_alert(f"{emoji} 15m MACD FAST crossed {direction_msg} zero → {current_fast_cross} bias")

        # --- Check for MACD line crossing the Signal line ---
        current_signal_cross = None
        # FIX: Use .item() for all comparisons
        if prev["MACD"].item() <= prev["Signal"].item() and curr["MACD"].item() > curr["Signal"].item():
            current_signal_cross = "bullish"
        elif prev["MACD"].item() >= prev["Signal"].item() and curr["MACD"].item() < curr["Signal"].item():
            current_signal_cross = "bearish"
            
        # If a new signal cross happens, send an alert and update state
        if current_signal_cross and current_signal_cross != last_alerted_status["signal_cross"]:
            last_alerted_status["signal_cross"] = current_signal_cross
            direction_msg = "ABOVE" if current_signal_cross == "bullish" else "BELOW"
            send_discord_alert(f"⚡ 15m MACD crossed {direction_msg} signal → {current_signal_cross}")

        await asyncio.sleep(60)


async def engulfing_observer():
    """
    Observes 5m data for engulfing patterns.
    Uses the MACD bias from the shared_state to filter signals if enabled.
    """
    last_alerted_engulfing_time = None

    while True:
        # Get the latest MACD bias from the other observer task
        last_fast_bias = shared_state["last_15m_macd_bias"]

        # This observer now only needs to download 5m data
        df5 = get_ohlc(TICKER, "5m", "1d")
        if len(df5) < 2:
            print("Engulfing Observer: Not enough data to process. Waiting...")
            await asyncio.sleep(60)
            continue
            
        engulfings = detect_engulfing(df5)

        if engulfings:
            # Get the most recent engulfing pattern found
            t, direction = engulfings[-1]

            # If this is a new engulfing candle we haven't alerted for yet
            if t != last_alerted_engulfing_time:
                # Check if the filter is on and if the direction mismatches the bias
                if ENGULFING_FILTER and last_fast_bias and direction != last_fast_bias:
                    print(f"Skipping {direction} engulfing at {t} due to {last_fast_bias} bias.")
                else:
                    msg = f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {t.strftime('%Y-%m-%d %H:%M')}"
                    send_discord_alert(msg)
                    # Record the timestamp of this candle to prevent re-alerting
                    last_alerted_engulfing_time = t

        await asyncio.sleep(60)


# ============= MAIN =============

async def main():
    """Run the observer tasks concurrently."""
    print("Starting trading alert observers...")
    send_discord_alert(f"✅ Bot started for {TICKER}.")
    await asyncio.gather(
        macd_observer(),
        engulfing_observer(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")



