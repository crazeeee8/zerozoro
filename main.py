import os
import asyncio
import aiohttp
import threading
from datetime import datetime, timedelta, timezone

import pandas as pd
import ccxt
from flask import Flask
from typing import Optional, Tuple

# ============== CONFIG ==============
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")  # required
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY")  # optional but recommended

SYMBOL = "BTC/USDT"
EXCHANGE = ccxt.binance()  # public market data only
TIMEFRAME = "15m"
LOOKBACK = 300  # enough history for EMAs

IST_OFFSET = timedelta(hours=5, minutes=30)

# News settings (kept same behavior; you can time-gate elsewhere if needed)
MAX_NEWS_PER_LOOP = 3

# Retry/backoff settings
MAX_RETRIES = 5
BASE_DELAY = 1.0  # seconds


# ============== KEEP-ALIVE (Flask) ==============
app = Flask(__name__)

@app.route("/")
def root():
    return "✅ Bot is alive and monitoring MACD + news alerts!"

@app.route("/health")
def health():
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


# ============== UTIL: Time helpers ==============
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_ist() -> datetime:
    return (now_utc() + IST_OFFSET)


# ============== UTIL: Retry wrappers ==============
async def async_with_retries(fn, *args, **kwargs):
    """Exponential backoff w/ jitter for async functions (aiohttp calls)."""
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await fn(*args, **kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(delay)
            delay *= 2

def sync_with_retries(fn, *args, **kwargs):
    """Exponential backoff w/ jitter for sync functions (ccxt)."""
    delay = BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except ccxt.NetworkError as e:
            if attempt == MAX_RETRIES:
                raise
            asyncio.run(asyncio.sleep(delay))
            delay *= 2


# ============== STATE (thread-safe) ==============
import threading as _threading

class BotState:
    """Encapsulates mutable state with a lock."""
    def __init__(self):
        self._lock = _threading.Lock()
        # MACD alert states
        self.last_early_state: Optional[str] = None     # "bullish" / "bearish" / None
        self.last_confirm_state: Optional[str] = None   # "bullish" / "bearish" / None
        # News state
        self.sent_news_ids = set()
        self.last_startup_tests_done = False

    def set_early_state(self, state: str):
        with self._lock:
            self.last_early_state = state

    def get_early_state(self) -> Optional[str]:
        with self._lock:
            return self.last_early_state

    def set_confirm_state(self, state: str):
        with self._lock:
            self.last_confirm_state = state

    def get_confirm_state(self) -> Optional[str]:
        with self._lock:
            return self.last_confirm_state

    def add_news_id(self, nid: str) -> bool:
        """Returns True if newly added; False if already present."""
        with self._lock:
            if nid in self.sent_news_ids:
                return False
            self.sent_news_ids.add(nid)
            return True

    def mark_startup_tests(self):
        with self._lock:
            self.last_startup_tests_done = True

    def startup_tests_done(self) -> bool:
        with self._lock:
            return self.last_startup_tests_done

STATE = BotState()


# ============== DISCORD (async with retries) ==============
async def _post_discord(session: aiohttp.ClientSession, content: str) -> None:
    if not DISCORD_WEBHOOK:
        print("[WARN] Missing DISCORD_WEBHOOK env var.")
        return
    async with session.post(DISCORD_WEBHOOK, json={"content": content}, timeout=15) as resp:
        # Discord returns 204 No Content when webhook posts successfully
        if resp.status not in (200, 204):
            text = await resp.text()
            raise aiohttp.ClientError(f"Discord error {resp.status}: {text[:200]}")

async def send_discord_message(message: str, session: Optional[aiohttp.ClientSession] = None):
    """Chunk and send to Discord with retries."""
    if not DISCORD_WEBHOOK:
        print("[WARN] Missing DISCORD_WEBHOOK env var.")
        return

    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    created = False
    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        for chunk in chunks:
            await async_with_retries(_post_discord, session, chunk)
            created = True
    finally:
        if own_session and session:
            await session.close()
    return created


# ============== NEWS (CryptoPanic) ==============
async def _fetch_cryptopanic(session: aiohttp.ClientSession) -> list:
    if not CRYPTOPANIC_API_KEY:
        return []
    url = f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}&currencies=BTC&kind=news"
    async with session.get(url, timeout=15) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise aiohttp.ClientError(f"CryptoPanic error {resp.status}: {text[:200]}")
        data = await resp.json()
        return data.get("results", [])

async def fetch_news_new(session: aiohttp.ClientSession) -> list:
    """Fetch new BTC news (deduped)."""
    items = await async_with_retries(_fetch_cryptopanic, session)
    fresh = []
    for post in items:
        pid = str(post.get("id", ""))
        if not pid:
            continue
        if STATE.add_news_id(pid):
            title = post.get("title", "")
            url = post.get("url", "")
            published_at = post.get("published_at", "")
            try:
                # Convert to IST if available
                ts = pd.to_datetime(published_at, utc=True).to_pydatetime().astimezone(timezone(IST_OFFSET))
                ts_str = ts.strftime("%Y-%m-%d %H:%M")
            except Exception:
                ts_str = published_at
            # CryptoPanic sentiment may appear under "vote" aggregates or "sentiment", keep generic
            sentiment = post.get("sentiment", "neutral")
            fresh.append(f"{ts_str} IST | {title} | Sentiment: {sentiment}\n{url}")
    return fresh


# ============== DATA FETCH (ccxt) ==============
def fetch_ohlcv_sync(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    """Sync ccxt fetch with retries & basic validation."""
    ohlcv = sync_with_retries(EXCHANGE.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
    if not ohlcv:
        raise RuntimeError("Empty OHLCV from exchange")
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    # Require basic columns and sufficient rows
    if any(col not in df.columns for col in ("open", "close")) or len(df) < 50:
        raise RuntimeError("Insufficient or invalid OHLCV data")

    # Timestamps to IST for consistency (used only for reference)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp_ist"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")
    return df


# ============== INDICATORS (MACD 8,15,9) ==============
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def macd_8159(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Return (macd, signal, hist) with 8,15,9."""
    ema_fast = ema(series, 8)
    ema_slow = ema(series, 15)
    macd = ema_fast - ema_slow
    signal = ema(macd, 9)
    hist = macd - signal
    return macd, signal, hist


# ============== SIGNALS (Early vs Confirm) ==============
def detect_macd_signals(df: pd.DataFrame) -> list:
    """
    Early warning (intracandle): compare last closed vs current forming candle.
    Confirmation: based on the last CLOSED candle only.
    """
    messages = []

    # Validate df
    if len(df) < 30 or "close" not in df.columns:
        return messages

    close = df["close"].astype(float)
    macd, signal, hist = macd_8159(close)
    df = df.copy()
    df["macd"] = macd
    df["signal"] = signal

    # ---- Early (intracandle) ----
    # last closed candle index: -2, current forming candle index: -1
    macd_prev_closed = df["macd"].iloc[-2]
    macd_live = df["macd"].iloc[-1]

    early_state_prev = STATE.get_early_state()
    if macd_prev_closed < 0 <= macd_live:
        # crossed up intrabar
        if early_state_prev != "bullish":
            messages.append("⚠️ Early Warning: MACD crossing ABOVE 0 (intracandle, 15m).")
            STATE.set_early_state("bullish")
    elif macd_prev_closed > 0 >= macd_live:
        # crossed down intrabar
        if early_state_prev != "bearish":
            messages.append("⚠️ Early Warning: MACD crossing BELOW 0 (intracandle, 15m).")
            STATE.set_early_state("bearish")

    # ---- Confirmation (closed) ----
    # check the last two CLOSED candles: -3 -> -2
    macd_closed_prev = df["macd"].iloc[-3]
    macd_closed_last = df["macd"].iloc[-2]

    confirm_state_prev = STATE.get_confirm_state()
    if macd_closed_prev < 0 <= macd_closed_last:
        if confirm_state_prev != "bullish":
            messages.append("✅ Confirmation: MACD CLOSED above 0 (15m).")
            STATE.set_confirm_state("bullish")
    elif macd_closed_prev > 0 >= macd_closed_last:
        if confirm_state_prev != "bearish":
            messages.append("✅ Confirmation: MACD CLOSED below 0 (15m).")
            STATE.set_confirm_state("bearish")

    return messages


# ============== STARTUP SELF-TESTS (Suggestion #6) ==============
async def startup_self_tests():
    """Run once after boot to verify pipeline end-to-end without waiting."""
    if STATE.startup_tests_done():
        return
    async with aiohttp.ClientSession() as session:
        # 1) Test Discord
        await send_discord_message(f"🚀 Bot started at {now_ist().strftime('%Y-%m-%d %H:%M:%S')} IST — self-test ping.", session=session)

        # 2) Test price fetch + MACD compute quickly
        ok_msg = "✅ OHLCV fetch + MACD compute OK."
        try:
            df = fetch_ohlcv_sync(SYMBOL, TIMEFRAME, LOOKBACK)
            # compute to assert no exception
            _ = macd_8159(df["close"].astype(float))
        except Exception as e:
            ok_msg = f"❌ OHLCV/MACD test failed: {type(e).__name__}: {e}"
        await send_discord_message(ok_msg, session=session)

        # 3) Test News (send a small digest once)
        try:
            if CRYPTOPANIC_API_KEY:
                news = await fetch_news_new(session)
                if news:
                    preview = "\n".join(news[:3])
                    await send_discord_message(f"📰 Startup News Preview (BTC):\n{preview}", session=session)
                else:
                    await send_discord_message("📰 Startup News Preview: No fresh BTC news found.", session=session)
            else:
                await send_discord_message("📰 Startup News Preview skipped (no CRYPTOPANIC_API_KEY).", session=session)
        except Exception as e:
            await send_discord_message(f"❌ News fetch test failed: {type(e).__name__}: {e}", session=session)

    STATE.mark_startup_tests()


# ============== MAIN MONITOR LOOP ==============
async def monitor():
    # Run startup tests first
    await startup_self_tests()

    while True:
        try:
            # Fetch OHLCV (15m)
            df = fetch_ohlcv_sync(SYMBOL, TIMEFRAME, LOOKBACK)

            # Validate DF before signals
            if df is not None and len(df) >= 30 and {"open", "close"}.issubset(df.columns):
                msgs = detect_macd_signals(df)
            else:
                msgs = []

            # Send MACD alerts
            if msgs:
                ist_ts = now_ist().strftime("%Y-%m-%d %H:%M:%S")
                to_send = "\n".join(f"[{ist_ts}] {m}" for m in msgs)
                await send_discord_message(to_send)

            # Fetch a few new BTC news posts each loop (deduped)
            async with aiohttp.ClientSession() as session:
                fresh_news = await fetch_news_new(session)
                if fresh_news:
                    # Limit each loop to avoid spam
                    bundle = "\n".join(fresh_news[:MAX_NEWS_PER_LOOP])
                    await send_discord_message(f"📰 BTC News (new):\n{bundle}")

        except ccxt.BaseError as e:
            # Specific ccxt errors
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] ccxt error: {type(e).__name__}: {e}")
        except Exception as e:
            print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] Unexpected error: {type(e).__name__}: {e}")

        # loop cadence
        await asyncio.sleep(60)


# ============== ENTRYPOINT ==============
if __name__ == "__main__":
    # Start keep-alive server
    threading.Thread(target=run_flask, daemon=True).start()
    # Run async monitor
    asyncio.run(monitor())
