#!/usr/bin/env python3
"""
main.py - Trading alert observers using Binance for OHLC data.
Features:
- 15m MACD observer (zero-line + MACD-signal crosses)
- 5m Engulfing observer (filtered by 15m MACD bias if enabled)
- Discord alerts via webhook
- Tiny HTTP health server (binds to $PORT for Render web services)
- Graceful shutdown on SIGINT/SIGTERM
"""

import os
import time
import threading
import http.server
import socketserver
import signal
import traceback
import asyncio
import datetime
import requests
import pandas as pd
import numpy as np
from binance import AsyncClient

# ============ CONFIG (env overrides) ============
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"
)
TICKER = os.environ.get("TICKER", "BTC-USD")  # keeps compatibility; mapped to Binance symbol
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
# observer sleep settings
MACD_OBSERVER_SLEEP = int(os.environ.get("MACD_OBSERVER_SLEEP", 60))  # seconds (main loop delay)
ENGULFING_OBSERVER_SLEEP = int(os.environ.get("ENGULFING_OBSERVER_SLEEP", 60))
# ==================================================================

# Shared state and runtime globals
shared_state = {"last_15m_macd_bias": None}
STOP_EVENT = threading.Event()
START_TIME = time.time()
HEALTH_SERVER = None
BINANCE_CLIENT = None  # AsyncClient instance (created lazily)


# ---------------- Discord ----------------
def send_discord_alert(message: str):
    """Send a message to Discord webhook (non-blocking-ish, with timeout)."""
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not configured - message:", message)
        return
    try:
        payload = {"content": f"[{datetime.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"}
        # small timeout to avoid blocking the app indefinitely
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if resp.status_code >= 400:
            print(f"[Discord] HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print("[Discord error]", e)


# ---------------- Health HTTP server (so Render detects $PORT) ----------------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            uptime = int(time.time() - START_TIME)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"OK - uptime {uptime}s\n".encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # silence default logging
        return


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_health_server_now():
    global HEALTH_SERVER
    port_s = os.environ.get("PORT", "8000")
    try:
        port = int(port_s)
    except Exception:
        port = 8000
    server = ThreadingTCPServer(("", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    HEALTH_SERVER = server
    print(f"[Health] server started and bound to port {port}")


# start early so Render sees the port quickly
start_health_server_now()


# ---------------- Binance client helpers ----------------
async def get_binance_client():
    """Create or return a singleton AsyncClient for Binance."""
    global BINANCE_CLIENT
    if BINANCE_CLIENT is None:
        BINANCE_CLIENT = await AsyncClient.create()
    return BINANCE_CLIENT


async def close_binance_client():
    global BINANCE_CLIENT
    if BINANCE_CLIENT:
        try:
            await BINANCE_CLIENT.close_connection()
        except Exception:
            pass
        BINANCE_CLIENT = None


def ticker_to_binance_symbol(ticker: str) -> str:
    """
    Convert a ticker like 'BTC-USD' or 'BTC/USD' to a Binance spot symbol 'BTCUSDT'.
    If the ticker already looks like a Binance symbol, return as-is.
    """
    t = ticker.strip().upper()
    # common mapping: -USD or /USD -> USDT
    t = t.replace("/", "").replace("-", "")
    if t.endswith("USD"):
        t = t[:-3] + "USDT"
    # If user already provided e.g. BTCUSDT, we return it
    return t


async def fetch_klines_binance(symbol: str, interval: str, lookback: str) -> pd.DataFrame:
    """
    Fetch historical klines using Binance AsyncClient.get_historical_klines.
    lookback is a string acceptable to the Binance client, e.g. "2 day ago UTC" or "1 day ago UTC".
    Returns a DataFrame with datetime index and columns Open, High, Low, Close, Volume.
    If AsyncClient path fails, falls back to the REST /api/v3/klines via requests.
    """
    data = []
    bin_sym = ticker_to_binance_symbol(symbol)
    try:
        client = await get_binance_client()
        # Binance client accepts start_str like "2 day ago UTC"
        klines = await client.get_historical_klines(bin_sym, interval, lookback)
        if klines:
            # columns: open_time, open, high, low, close, volume, close_time, ...
            cols = ["OpenTime", "Open", "High", "Low", "Close", "Volume", "CloseTime",
                    "QuoteAssetVolume", "NumTrades", "TakerBase", "TakerQuote", "Ignore"]
            df = pd.DataFrame(klines, columns=cols)
            df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="ms")
            df.set_index("OpenTime", inplace=True)
            # convert numeric fields
            for c in ["Open", "High", "Low", "Close", "Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df.dropna(subset=["Close"], inplace=True)
            return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"[Binance async] failed to fetch {bin_sym} {interval} {lookback}: {e}")
        traceback.print_exc()

    # Fallback to REST endpoint
    try:
        print(f"[Binance fallback] using REST for {bin_sym} {interval}")
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": bin_sym, "interval": interval, "limit": 1000}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        klines = r.json()
        if not klines:
            return pd.DataFrame()
        cols = ["OpenTime", "Open", "High", "Low", "Close", "Volume", "CloseTime",
                "QuoteAssetVolume", "NumTrades", "TakerBase", "TakerQuote", "Ignore"]
        df = pd.DataFrame(klines, columns=cols)
        df["OpenTime"] = pd.to_datetime(df["OpenTime"], unit="ms")
        df.set_index("OpenTime", inplace=True)
        for c in ["Open", "High", "Low", "Close", "Volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df.dropna(subset=["Close"], inplace=True)
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"[Binance REST] fallback failed for {bin_sym}: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# ---------------- Technical indicators ----------------
def calculate_macd(df: pd.DataFrame, fast: int = MACD_FAST, slow: int = MACD_SLOW, signal: int = MACD_SIGNAL) -> pd.DataFrame:
    """Compute MACD and signal lines (works in-place and returns df). Expects column 'Close'."""
    if df.empty:
        return df
    df["EMA_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=signal, adjust=False).mean()
    return df


def detect_engulfing_list(df: pd.DataFrame) -> list:
    """
    Detect bullish and bearish engulfing candles.
    Returns a list of tuples: (timestamp_index, "bullish"/"bearish").
    Expects 'Open' and 'Close' columns.
    """
    engulfings = []
    if df.empty or "Open" not in df.columns or "Close" not in df.columns:
        return engulfings

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        try:
            prev_open = float(prev["Open"])
            prev_close = float(prev["Close"])
            curr_open = float(curr["Open"])
            curr_close = float(curr["Close"])
        except Exception:
            continue

        prev_body = prev_close - prev_open
        curr_body = curr_close - curr_open

        # Bullish engulfing: previous red, current green and larger
        if prev_body < 0 and curr_body > 0 and curr_close > prev_open and curr_open < prev_close:
            engulfings.append((curr.name, "bullish"))
        # Bearish engulfing: previous green, current red and larger
        elif prev_body > 0 and curr_body < 0 and curr_close < prev_open and curr_open > prev_close:
            engulfings.append((curr.name, "bearish"))

    return engulfings


# ---------------- Observers ----------------
async def macd_observer():
    """
    Observes 15m MACD for zero-line and signal-line crosses.
    Updates shared_state with the current bias and sends Discord alerts.
    """
    last_alerted_status = {"fast_cross": None, "signal_cross": None}

    while not STOP_EVENT.is_set():
        try:
            # fetch 15m data for last ~2 days
            df15 = await fetch_klines_binance(TICKER, "15m", "2 day ago UTC")
            if len(df15) < 3:
                print("[MACD] not enough 15m data; sleeping...") 
                await asyncio.sleep(MACD_OBSERVER_SLEEP)
                continue

            df15 = calculate_macd(df15)
            prev = df15.iloc[-2]
            curr = df15.iloc[-1]

            prev_macd = float(prev["MACD"])
            curr_macd = float(curr["MACD"])

            # Zero-line cross (fast line crossing zero)
            current_fast_cross = None
            if prev_macd <= 0 < curr_macd:
                current_fast_cross = "bullish"
            elif prev_macd >= 0 > curr_macd:
                current_fast_cross = "bearish"

            if current_fast_cross and current_fast_cross != last_alerted_status["fast_cross"]:
                shared_state["last_15m_macd_bias"] = current_fast_cross
                last_alerted_status["fast_cross"] = current_fast_cross
                emoji = "🚀" if current_fast_cross == "bullish" else "🔻"
                direction_msg = "ABOVE" if current_fast_cross == "bullish" else "BELOW"
                send_discord_alert(f"{emoji} 15m MACD FAST crossed {direction_msg} zero → {current_fast_cross} bias")

            # MACD line vs Signal line cross
            prev_signal = float(prev["Signal"])
            curr_signal = float(curr["Signal"])
            current_signal_cross = None
            if prev_macd <= prev_signal and curr_macd > curr_signal:
                current_signal_cross = "bullish"
            elif prev_macd >= prev_signal and curr_macd < curr_signal:
                current_signal_cross = "bearish"

            if current_signal_cross and current_signal_cross != last_alerted_status["signal_cross"]:
                last_alerted_status["signal_cross"] = current_signal_cross
                direction_msg = "ABOVE" if current_signal_cross == "bullish" else "BELOW"
                send_discord_alert(f"⚡ 15m MACD crossed {direction_msg} signal → {current_signal_cross}")

        except Exception as e:
            print("[MACD observer] error:", e)
            traceback.print_exc()

        # sleep with frequent stop checks (total ~MACD_OBSERVER_SLEEP)
        remaining = MACD_OBSERVER_SLEEP
        while remaining > 0 and not STOP_EVENT.is_set():
            await asyncio.sleep(min(10, remaining))
            remaining -= min(10, remaining)


async def engulfing_observer():
    """
    Observes 5m data for engulfing patterns and filters by 15m bias if enabled.
    """
    last_alerted_engulfing_time = None

    while not STOP_EVENT.is_set():
        try:
            last_fast_bias = shared_state.get("last_15m_macd_bias")
            df5 = await fetch_klines_binance(TICKER, "5m", "1 day ago UTC")
            if len(df5) < 3:
                print("[Engulfing] not enough 5m data; sleeping...")
                await asyncio.sleep(ENGULFING_OBSERVER_SLEEP)
                continue

            engulfings = detect_engulfing_list(df5)

            if engulfings:
                # most recent engulfing
                t, direction = engulfings[-1]
                if t != last_alerted_engulfing_time:
                    if ENGULFING_FILTER and last_fast_bias and direction != last_fast_bias:
                        print(f"[Engulfing] skipping {direction} engulfing at {t} due to bias={last_fast_bias}")
                    else:
                        try:
                            timestr = t.strftime("%Y-%m-%d %H:%M")
                        except Exception:
                            timestr = str(t)
                        send_discord_alert(f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {timestr}")
                        last_alerted_engulfing_time = t

        except Exception as e:
            print("[Engulfing observer] error:", e)
            traceback.print_exc()

        remaining = ENGULFING_OBSERVER_SLEEP
        while remaining > 0 and not STOP_EVENT.is_set():
            await asyncio.sleep(min(10, remaining))
            remaining -= min(10, remaining)


# ---------------- Graceful shutdown ----------------
def _signal_handler(signum, frame):
    print(f"[Signal] received {signum}, initiating shutdown...")
    STOP_EVENT.set()
    # shutdown health server quickly
    try:
        if HEALTH_SERVER:
            HEALTH_SERVER.shutdown()
            HEALTH_SERVER.server_close()
    except Exception:
        pass


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ---------------- Main ----------------
async def main():
    print("Starting trading alert observers...")
    send_discord_alert(f"✅ Bot started for {TICKER}.")

    # Create binance client early (lazy inside fetch), but ensure it is created when needed.
    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer()),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print("[Main] exception:", e)
        traceback.print_exc()
    finally:
        print("[Main] shutting down observers and Binance client...")
        STOP_EVENT.set()
        for t in tasks:
            t.cancel()
        try:
            await close_binance_client()
        except Exception:
            pass
        try:
            if HEALTH_SERVER:
                HEALTH_SERVER.shutdown()
                HEALTH_SERVER.server_close()
        except Exception:
            pass
        print("[Main] stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print("[Fatal] Unhandled exception:", exc)
        traceback.print_exc()
        raise
