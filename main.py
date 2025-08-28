#!/usr/bin/env python3
"""
main.py - trading alert observers with an immediately-bound health HTTP server.
Put this file at the repo root as main.py and Deploy as a Web Service on Render
(or use the Background Worker variant described below).
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
import yfinance as yf

# ============ CONFIG (override via environment variables in Render) ============
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
TICKER = os.environ.get("TICKER", "BTC-USD")
MACD_FAST = int(os.environ.get("MACD_FAST", 8))
MACD_SLOW = int(os.environ.get("MACD_SLOW", 15))
MACD_SIGNAL = int(os.environ.get("MACD_SIGNAL", 9))
ENGULFING_FILTER = os.environ.get("ENGULFING_FILTER", "True").lower() in ("1", "true", "yes")
# =============================================================================

# Shared state
shared_state = {"last_15m_macd_bias": None}

# Stop flag used across threads/tasks
STOP_EVENT = threading.Event()
START_TIME = time.time()
HEALTH_SERVER = None  # will hold the server object if started


def send_discord_alert(message: str):
    """Post to Discord webhook safely (short timeout)."""
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not configured; message:", message)
        return
    try:
        payload = {"content": f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"}
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=8)
        if resp.status_code >= 400:
            print(f"[Discord] HTTP {resp.status_code} - {resp.text}")
    except Exception as e:
        print("[Discord error]", e)


def get_ohlc(ticker: str, interval: str, lookback: str) -> pd.DataFrame:
    """
    Safe wrapper for yfinance.download. Set auto_adjust explicitly to avoid FutureWarning noise.
    Returns empty DataFrame on error.
    """
    try:
        df = yf.download(ticker, interval=interval, period=lookback, progress=False, auto_adjust=False)
        if df is None or df.empty:
            # no data returned
            return pd.DataFrame()
        return df
    except Exception as e:
        print(f"[yfinance] error for {ticker} {interval} {lookback}: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def calculate_macd(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["EMA_fast"] = df["Close"].ewm(span=MACD_FAST, adjust=False).mean()
    df["EMA_slow"] = df["Close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["MACD"] = df["EMA_fast"] - df["EMA_slow"]
    df["Signal"] = df["MACD"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    return df


def detect_engulfing(df: pd.DataFrame) -> list:
    """Return list of (timestamp_index, 'bullish'|'bearish')."""
    out = []
    if df.empty or "Open" not in df.columns or "Close" not in df.columns:
        return out

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

        # bullish engulfing
        if prev_body < 0 and curr_body > 0 and curr_close > prev_open and curr_open < prev_close:
            out.append((curr.name, "bullish"))
        # bearish engulfing
        elif prev_body > 0 and curr_body < 0 and curr_close < prev_open and curr_open > prev_close:
            out.append((curr.name, "bearish"))

    return out


# ----- Observers -----
async def macd_observer():
    last_alerted_status = {"fast_cross": None, "signal_cross": None}
    while not STOP_EVENT.is_set():
        try:
            df = get_ohlc(TICKER, "15m", "2d")
            if len(df) < 2:
                await asyncio.sleep(30)
                continue

            df = calculate_macd(df)
            prev, curr = df.iloc[-2], df.iloc[-1]
            prev_macd = float(prev["MACD"])
            curr_macd = float(curr["MACD"])

            current_fast_cross = None
            if prev_macd <= 0 < curr_macd:
                current_fast_cross = "bullish"
            elif prev_macd >= 0 > curr_macd:
                current_fast_cross = "bearish"

            if current_fast_cross and current_fast_cross != last_alerted_status["fast_cross"]:
                shared_state["last_15m_macd_bias"] = current_fast_cross
                last_alerted_status["fast_cross"] = current_fast_cross
                emoji = "🚀" if current_fast_cross == "bullish" else "🔻"
                direction = "ABOVE" if current_fast_cross == "bullish" else "BELOW"
                send_discord_alert(f"{emoji} 15m MACD FAST crossed {direction} zero → {current_fast_cross} bias")

            prev_signal = float(prev["Signal"])
            curr_signal = float(curr["Signal"])
            current_signal_cross = None
            if prev_macd <= prev_signal and curr_macd > curr_signal:
                current_signal_cross = "bullish"
            elif prev_macd >= prev_signal and curr_macd < curr_signal:
                current_signal_cross = "bearish"

            if current_signal_cross and current_signal_cross != last_alerted_status["signal_cross"]:
                last_alerted_status["signal_cross"] = current_signal_cross
                direction = "ABOVE" if current_signal_cross == "bullish" else "BELOW"
                send_discord_alert(f"⚡ 15m MACD crossed {direction} signal → {current_signal_cross}")

        except Exception as e:
            print("[MACD observer] error:", e)
            traceback.print_exc()

        # sleep with frequent stop checks (total ~60s)
        for _ in range(6):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(10)


async def engulfing_observer():
    last_alerted_engulfing_time = None
    while not STOP_EVENT.is_set():
        try:
            bias = shared_state.get("last_15m_macd_bias")
            df5 = get_ohlc(TICKER, "5m", "1d")
            if len(df5) < 2:
                await asyncio.sleep(30)
                continue

            engulfings = detect_engulfing(df5)
            if engulfings:
                t, direction = engulfings[-1]
                if t != last_alerted_engulfing_time:
                    if ENGULFING_FILTER and bias and direction != bias:
                        print(f"[Engulfing] skipping {direction} at {t} due to bias {bias}")
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

        for _ in range(6):
            if STOP_EVENT.is_set():
                break
            await asyncio.sleep(10)


# ----- tiny health HTTP server so Render detects port immediately -----
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
        # silence default logs
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


# Start the health server immediately on import to ensure Render sees the port ASAP
start_health_server_now()


# ----- graceful shutdown -----
def _signal_handler(signum, frame):
    print(f"[Signal] received {signum}, shutting down...")
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


# ----- main runner -----
async def main():
    send_discord_alert(f"✅ Bot started for {TICKER}.")
    print("Starting observers...")
    tasks = [
        asyncio.create_task(macd_observer()),
        asyncio.create_task(engulfing_observer()),
    ]

    # wait until stop
    try:
        while not STOP_EVENT.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print("[Main] shutting down tasks...")
        STOP_EVENT.set()
        for t in tasks:
            t.cancel()
        try:
            if HEALTH_SERVER:
                HEALTH_SERVER.shutdown()
                HEALTH_SERVER.server_close()
        except Exception:
            pass
        print("[Main] exit.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("[Fatal] unhandled:", e)
        traceback.print_exc()
        raise
