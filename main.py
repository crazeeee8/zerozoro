import os
import asyncio
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandas_ta as ta
import ccxt
import aiohttp
from aiohttp import ClientResponseError, ClientConnectorError, ClientPayloadError, ServerTimeoutError
from flask import Flask
from config import CRYPTOPANIC_API_KEY as FILE_CRYPTOPANIC, DISCORD_WEBHOOK as FILE_WEBHOOK, COINMARKETCAL_API_KEY as FILE_CMC

CRYPTOPANIC_API_KEY = os.environ.get("CRYPTOPANIC_API_KEY", FILE_CRYPTOPANIC)
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", FILE_WEBHOOK)
COINMARKETCAL_API_KEY = os.environ.get("COINMARKETCAL_API_KEY", FILE_CMC)

# Comma-separated list of exchanges to try in order (auto-fallback if one fails/geoblocked)
EXCHANGES_PREF = os.getenv(
    "EXCHANGES",
    "binance,binanceus,kucoin,kraken,bybit"
).lower().replace(" ", "").split(",")

SYMBOL = os.getenv("SYMBOL", "BTC/USDT").strip()
TIMEFRAME = os.getenv("TIMEFRAME", "15m").strip()
LOOKBACK = int(os.getenv("LOOKBACK", "120"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

# Timezone for display (IST)
IST = timezone(timedelta(hours=5, minutes=30))

# Flask port for Render keep-alive
PORT = int(os.getenv("PORT", "10000"))

# =========================
# ====== STATE ============
# =========================

@dataclass
class BotState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_macd_state: Dict[str, Optional[str]] = field(default_factory=lambda: {"early": None, "confirm": None})
    seen_news_ids: set = field(default_factory=set)
    exchange_name: Optional[str] = None
    exchange: Optional[ccxt.Exchange] = None
    session: Optional[aiohttp.ClientSession] = None
    posted_webhook_warning: bool = False

STATE = BotState()

# =========================
# ====== UTILITIES ========
# =========================

def now_ist() -> datetime:
    return datetime.now(timezone.utc).astimezone(IST)

def jitter(base: float, frac: float = 0.3) -> float:
    # Adds +/- 30% jitter by default
    delta = base * frac
    return base + random.uniform(-delta, delta)

async def ensure_session() -> aiohttp.ClientSession:
    async with STATE.lock:
        if STATE.session and not STATE.session.closed:
            return STATE.session
        # Reasonable default timeouts
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        STATE.session = aiohttp.ClientSession(timeout=timeout)
        return STATE.session

async def async_backoff(
    fn,
    *,
    retries: int = 5,
    base_delay: float = 1.5,
    max_delay: float = 20.0,
    exceptions: Tuple = (
        ClientConnectorError,
        ClientResponseError,
        ClientPayloadError,
        asyncio.TimeoutError,
        ServerTimeoutError,
    ),
    retry_on_status: Tuple[int, ...] = (429, 500, 502, 503, 504),
    label: str = "op"
):
    """
    Generic async backoff wrapper for HTTP-like operations.
    """
    for attempt in range(1, retries + 1):
        try:
            return await fn()
        except ClientResponseError as cre:
            # Retry on transient HTTP
            if cre.status in retry_on_status:
                delay = min(max_delay, jitter(base_delay * (2 ** (attempt - 1))))
                print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {label} HTTP {cre.status}, retry {attempt}/{retries} in {delay:.1f}s")
                await asyncio.sleep(delay)
                continue
            # Non-retryable HTTP
            raise
        except exceptions as e:
            delay = min(max_delay, jitter(base_delay * (2 ** (attempt - 1))))
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {label} error {type(e).__name__}, retry {attempt}/{retries} in {delay:.1f}s: {e}")
            await asyncio.sleep(delay)
    # Final attempt (let exception bubble)
    return await fn()

async def to_thread_backoff(fn_sync, *, retries: int = 5, base_delay: float = 1.5, max_delay: float = 20.0, label: str = "ccxt"):
    """
    Backoff wrapper for sync functions (like ccxt) run in a thread.
    """
    for attempt in range(1, retries + 1):
        try:
            return await asyncio.to_thread(fn_sync)
        except ccxt.BaseError as e:
            # Retry for transient or rate-limit errors; geoblocks will be handled outside.
            msg = str(e)
            transient = any(k in msg.lower() for k in ["timeout", "temporarily unavailable", "rate limit", "request timeout", "service unavailable", "429", "5", "network"])
            if transient and attempt < retries:
                delay = min(max_delay, jitter(base_delay * (2 ** (attempt - 1))))
                print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {label} transient error, retry {attempt}/{retries} in {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
                continue
            raise

# =========================
# ====== EXCHANGES ========
# =========================

def instantiate_exchange(name: str) -> ccxt.Exchange:
    # Map user string to ccxt constructor
    name = name.lower()
    if not hasattr(ccxt, name):
        raise ValueError(f"Exchange '{name}' not found in ccxt")
    ex_cls = getattr(ccxt, name)
    ex = ex_cls({
        "enableRateLimit": True,
        "timeout": 15000,  # ms
    })
    return ex

async def pick_working_exchange() -> Tuple[ccxt.Exchange, str]:
    """
    Try exchanges in EXCHANGES_PREF order; return first that loads markets successfully.
    Detect geoblock (HTTP 451) and move on gracefully.
    """
    for name in EXCHANGES_PREF:
        try:
            ex = instantiate_exchange(name)
            # load_markets is sync; run in thread + backoff
            await to_thread_backoff(ex.load_markets, label=f"{name}.load_markets")
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Using exchange: {name}")
            return ex, name
        except ccxt.BaseError as e:
            lower = str(e).lower()
            if "451" in lower or "restricted location" in lower or "eligibility" in lower:
                print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {name} blocked in region (451). Trying next exchange...")
                continue
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Failed to init {name}: {e}. Trying next...")
        except Exception as e:
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Unexpected init error for {name}: {e}. Trying next...")
    raise RuntimeError("No working exchange available from EXCHANGES list.")

async def ensure_exchange():
    async with STATE.lock:
        if STATE.exchange is not None:
            return STATE.exchange, STATE.exchange_name or "unknown"
        ex, name = await pick_working_exchange()
        STATE.exchange = ex
        STATE.exchange_name = name
        return ex, name

# =========================
# ====== HTTP CLIENTS =====
# =========================

async def send_discord_message(content: str):
    if not DISCORD_WEBHOOK:
        if not STATE.posted_webhook_warning:
            print("[WARN] Discord webhook not configured; set DISCORD_WEBHOOK to receive alerts.")
            STATE.posted_webhook_warning = True
        return

    session = await ensure_session()

    async def _post():
        async with session.post(DISCORD_WEBHOOK, json={"content": content}) as resp:
            if resp.status not in (200, 204):
                text = await resp.text()
                raise ClientResponseError(resp.request_info, resp.history, status=resp.status, message=text)

    try:
        await async_backoff(_post, label="discord.post")
    except Exception as e:
        print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Discord send failed: {type(e).__name__}: {e}")

async def fetch_cryptopanic_news() -> List[Dict[str, Any]]:
    if not CRYPTOPANIC_API_KEY:
        return []

    session = await ensure_session()

    url = "https://cryptopanic.com/api/v1/posts/"
    params = {"auth_token": CRYPTOPANIC_API_KEY, "currencies": "BTC", "public": "true"}

    async def _get():
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise ClientResponseError(resp.request_info, resp.history, status=resp.status, message=text)
            data = await resp.json()
            results = data.get("results", [])
            if not isinstance(results, list):
                raise ValueError("Unexpected CryptoPanic response shape: 'results' not a list")
            return results

    try:
        return await async_backoff(_get, label="cryptopanic.get")
    except Exception as e:
        print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] CryptoPanic failed: {type(e).__name__}: {e}")
        return []

# =========================
# ====== MARKET DATA ======
# =========================

async def fetch_ohlcv() -> pd.DataFrame:
    ex, ex_name = await ensure_exchange()

    def _sync_fetch():
        return ex.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=LOOKBACK)

    try:
        ohlcv = await to_thread_backoff(_sync_fetch, label=f"{ex_name}.fetch_ohlcv")
    except ccxt.BaseError as e:
        # If geoblock or exchange outage mid-run, rotate exchange next cycle
        lower = str(e).lower()
        if "451" in lower or "restricted location" in lower or "eligibility" in lower:
            async with STATE.lock:
                print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {ex_name} became blocked; rotating exchange...")
                STATE.exchange = None
                STATE.exchange_name = None
        raise

    if not isinstance(ohlcv, list) or len(ohlcv) == 0:
        raise ValueError("Empty OHLCV from exchange.")

    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    # Basic validation
    for col in ["timestamp", "open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"OHLCV missing column: {col}")

    if len(df) < 35:
        raise ValueError(f"Insufficient candles: {len(df)} (need >= 35)")

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(IST)
    return df

# =========================
# ====== INDICATORS =======
# =========================

def check_macd_signals(df: pd.DataFrame) -> List[str]:
    if "close" not in df or df["close"].isna().any():
        raise ValueError("DataFrame missing close or contains NaNs in close.")

    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd is None or macd.empty:
        raise ValueError("MACD calculation failed or returned empty.")

    df2 = pd.concat([df, macd], axis=1).dropna(subset=["MACD_12_26_9", "MACDs_12_26_9"])
    if len(df2) < 3:
        raise ValueError("Not enough MACD rows after dropna.")

    latest = df2.iloc[-1]
    prev = df2.iloc[-2]

    macd_val = float(latest["MACD_12_26_9"])
    macd_prev = float(prev["MACD_12_26_9"])

    signals: List[str] = []

    # Early warning: zero-line crossing intra/at close detection
    if macd_prev < 0 <= macd_val:
        if STATE.last_macd_state.get("early") != "bullish":
            signals.append("⚠️ Early Warning: MACD crossing ABOVE 0")
            STATE.last_macd_state["early"] = "bullish"
    elif macd_prev > 0 >= macd_val:
        if STATE.last_macd_state.get("early") != "bearish":
            signals.append("⚠️ Early Warning: MACD crossing BELOW 0")
            STATE.last_macd_state["early"] = "bearish"

    # Confirmation: where it currently closes relative to zero
    if macd_val > 0:
        if STATE.last_macd_state.get("confirm") != "bullish":
            signals.append("✅ Confirmation: MACD CLOSED above 0")
            STATE.last_macd_state["confirm"] = "bullish"
    elif macd_val < 0:
        if STATE.last_macd_state.get("confirm") != "bearish":
            signals.append("✅ Confirmation: MACD CLOSED below 0")
            STATE.last_macd_state["confirm"] = "bearish"

    return signals

# =========================
# ====== NEWS HANDLING ====
# =========================

def pick_new_news(news: List[Dict[str, Any]]) -> List[Tuple[str, str, str]]:
    """
    Returns list of (id, title, url) that haven't been seen yet.
    """
    fresh: List[Tuple[str, str, str]] = []
    for n in news:
        nid = str(n.get("id", ""))
        title = n.get("title", "")
        url = n.get("url", "")
        if not nid or not title or not url:
            continue
        if nid in STATE.seen_news_ids:
            continue
        STATE.seen_news_ids.add(nid)
        fresh.append((nid, title, url))
    return fresh

# =========================
# ====== FLASK KEEPALIVE ==
# =========================

app = Flask(__name__)

@app.get("/")
def home():
    ex = STATE.exchange_name or "uninitialized"
    return f"✅ Bot alive. Exchange={ex} | Symbol={SYMBOL} | TF={TIMEFRAME} | Lookback={LOOKBACK}", 200

@app.get("/health")
def health():
    return "ok", 200

def run_flask():
    # Production note: this is only a keep-alive server, dev WSGI is fine here.
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# =========================
# ====== MAIN LOOP ========
# =========================

async def startup_test():
    # Test message so you don't wait for the first event
    ex, ex_name = await ensure_exchange()
    await send_discord_message(f"🤖 Bot started on `{ex_name}` | {SYMBOL} {TIMEFRAME} | {now_ist().strftime('%Y-%m-%d %H:%M:%S')}")
    # Try market + MACD quickly
    try:
        df = await fetch_ohlcv()
        sigs = check_macd_signals(df)
        if sigs:
            for s in sigs:
                await send_discord_message(f"🧪 Startup check: {s}")
        else:
            await send_discord_message("🧪 Startup check: no new MACD signals right now.")
    except Exception as e:
        await send_discord_message(f"🧪 Startup market check failed: {type(e).__name__}: {e}")

    # Try news quickly
    try:
        news = await fetch_cryptopanic_news()
        fresh = pick_new_news(news)[:3]
        if fresh:
            for _, title, url in fresh:
                await send_discord_message(f"🧪 Startup news: {title}\n{url}")
        else:
            await send_discord_message("🧪 Startup news: none returned (or API key missing).")
    except Exception as e:
        await send_discord_message(f"🧪 Startup news check failed: {type(e).__name__}: {e}")

async def monitor():
    await startup_test()

    while True:
        try:
            # Market
            df = await fetch_ohlcv()
            signals = check_macd_signals(df)
            for signal in signals:
                ts = now_ist().strftime('%Y-%m-%d %H:%M:%S')
                await send_discord_message(f"[{ts}] {signal}")

            # News
            news_items = await fetch_cryptopanic_news()
            for _, title, url in pick_new_news(news_items)[:3]:
                await send_discord_message(f"📰 News: {title}\n{url}")

        except ccxt.BaseError as e:
            # If exchange error occurs, try rotating next loop
            msg = str(e).lower()
            if "451" in msg or "restricted location" in msg or "eligibility" in msg:
                async with STATE.lock:
                    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Exchange geoblocked; resetting exchange.")
                    STATE.exchange = None
                    STATE.exchange_name = None
            else:
                print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] CCXT error: {e}")
        except Exception as e:
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Unexpected error: {type(e).__name__}: {e}")

        await asyncio.sleep(POLL_SECONDS)

async def main_async():
    # Pre-create client session
    await ensure_session()
    # Ensure exchange selected
    await ensure_exchange()
    # Run monitor
    await monitor()

def main():
    # Start keep-alive server
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    # Run async bot
    asyncio.run(main_async())

if __name__ == "__main__":
    main()




