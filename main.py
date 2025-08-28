#!/usr/bin/env python3
"""
main.py - trading alert observers with a tiny health HTTP server so Render detects an open port.
Copy into your project and deploy as a Web Service (Render sets $PORT).
"""

import asyncio
import datetime
import os
import threading
import http.server
import socketserver
import time
import signal
import sys
import traceback

import requests
import pandas as pd
import yfinance as yf

# ================ CONFIG (override with env vars) =================
DISCORD_WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR",
)
TICKER = os.environ.get("TICKER", "BTC-USD")
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
# ==================================================================

# Shared state for lightweight cross-task communication
shared_state = {"last_15m_macd_bias": None}

# Thread-safe stop flag (used by both threaded HTTP server and async tasks)
STOP_EVENT = threading.Event()
START_TIME = time.time()


def send_discord_alert(message: str):
    """Post a message to Discord webhook (safely, with timeout)."""
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not configured - would send:", message)
        return
    try:
        timestamped_message = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": timestamped_message}, timeout=10)
        if resp.status_code >= 400:
            print(f"[Discord] webhook returned {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[Discord Error] {e}")


def get_ohlc(ticker: str, interval: str, lookback: str) -> pd.DataFrame:
    """
    Download OHLCV data from Yahoo Finance.
    Specifies auto_adjust to avoid the future-warning spam.
    Returns an empty DataFrame on failure.
    """
    try:
        data = yf.download(ticker, interval=interval, period=lookback, progress=False, auto_adjust=False)
        if data is None or data.empty:
            print(f"[yfinance] No data returned for {ticker} {interval} {lookback}")
            return pd.DataFrame()
        return data
    except Exception as e:
        print(f"[yfinance Error] Failed to download data for {ticker} {interval} {lookback}: {e}")
        return pd.DataFrame()


def calculate_macd(df: pd.DataFrame) -> pd.DataFrame:
    """Compute MACD and signal lines (in-place)."""
    if df.empty:
        return df
    df["EMA_fast"] = df["Close"].ewm(span=MACD_FAST, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    return df


def detect_engulfing(df: pd.DataFrame) -> list:
    """
    Detect bullish and bearish engulfing candles.
    Returns list of (timestamp, "bullish"|"bearish").
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
            # skip malformed rows
            continue

        prev_body = prev_close - prev_open
        curr_body = curr_close - curr_open

        # Bullish engulfing
        if prev_body < 0 and curr_body > 0 and curr_close > prev_open and curr_open < prev_close:
            engulfings.append((curr.name, "bullish"))

        # Bearish engulfing
        elif prev_body > 0 and curr_body < 0 and curr_close < prev_open and curr_open > prev_close:
            engulfings.append((curr.name, "bearish"))

    return engulfings


# ============= Observers (async) =============

async def macd_observer():
    """
    Observes 15m MACD zero-line and MACD-signal crosses.
    Updates shared_state and posts alerts.
    Exits when STOP_EVENT is set.
    """
    last_alerted_status = {"fast_cross": None, "signal_cross": None}

    while not STOP_EVENT.is_set():
        try:
            df = get_ohlc(TICKER, "15m", "2d")
            if len(df) < 2:
                print("MACD Observer: not enough data; sleeping...")
                await asyncio.sleep(60)
                continue

            df = calculate_macd(df)
            prev, curr = df.iloc[-2], df.iloc[-1]

            # Safe scalar extraction
            prev_macd = float(prev["MACD"])
            curr_macd = float(curr["MACD"])

            # MACD fast (zero-line) cross
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

            # MACD crossing the signal line
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
            print("[MACD Observer] error:", e)
            traceback.print_exc()

        # check stop condition frequently
        for _ in range(6):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(10)


async def engulfing_observer():
    """
    Observes 5m bars for engulfing patterns.
    Optionally filters by the 15m MACD bias in shared_state.
    Exits when STOP_EVENT is set.
    """
    last_alerted_engulfing_time = None

    while not STOP_EVENT.is_set():
        try:
            last_fast_bias = shared_state.get("last_15m_macd_bias")

            df5 = get_ohlc(TICKER, "5m", "1d")
            if len(df5) < 2:
                print("Engulfing Observer: not enough data; sleeping...")
                await asyncio.sleep(60)
                continue

            engulfings = detect_engulfing(df5)
            if engulfings:
                t, direction = engulfings[-1]
                if t != last_alerted_engulfing_time:
                    if ENGULFING_FILTER and last_fast_bias and direction != last_fast_bias:
                        print(f"Skipping {direction} engulfing at {t} due to bias={last_fast_bias}")
                    else:
                        # format timestamp safely
                        try:
                            timestr = t.strftime("%Y-%m-%d %H:%M")
                        except Exception:
                            timestr = str(t)
                        msg = f"🕯️ 5m ENGULFING CONFIRMED ({direction.upper()}) at {timestr}"
                        send_discord_alert(msg)
                        last_alerted_engulfing_time = t

        except Exception as e:
            print("[Engulfing Observer] error:", e)
            traceback.print_exc()

        # check stop condition frequently
        for _ in range(6):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(10)


# ============= tiny health HTTP server (so Render detects an open port) =============

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

    # silence the default logging
    def log_message(self, format, *args):
        return


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_health_server(port: int):
    server = ThreadingTCPServer(("", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[Health] server started on port {port}")
    return server


# ============= graceful shutdown handlers =============

def _signal_handler(signum, frame):
    print(f"[Signal] received {signum}, initiating shutdown...")
    STOP_EVENT.set()


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ============= main =============

async def main():
    # Start HTTP health server (Render gives a PORT env variable)
    port_str = os.environ.get("PORT", "8000")
    try:
        port = int(port_str)
    except Exception:
        port = 8000
    server = start_health_server(port)

    print("Starting trading alert observers...")
    send_discord_alert(f"✅ Bot started for {TICKER}.")

    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer()),
    ]

    try:
        # Wait for both observers to finish (they will finish when STOP_EVENT is set)
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print("[Main] exception:", e)
        traceback.print_exc()
    finally:
        print("[Main] shutting down observers and HTTP server...")
        STOP_EVENT.set()
        for t in tasks:
            t.cancel()
        try:
            server.shutdown()
            server.server_close()
        except Exception:
            pass
        print("[Main] stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print("[Fatal] Unhandled exception:", exc)
        traceback.print_exc()
        sys.exit(1)
