import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pytz import timezone
from flask import Flask
import threading
import mplfinance as mpf
import matplotlib.pyplot as plt
import io
import numpy as np
from config import DISCORD_WEBHOOK, CRYPTOPANIC_API_KEY, COINMARKETCAL_API_KEY

import requests

# === CONFIG ===
TICKER = "BTCUSDT"  # Binance Spot
FAST, SLOW, SIGNAL = 8, 15, 9
CHECK_DELAY = 60  # seconds between checks
NEWS_CHECK_INTERVAL = 3600  # seconds for hourly emergency news
NEWS_DIGEST_HOUR = 8  # IST 8:00am
DRAW_SR = False  # optional support/resistance plotting

# === FLASK KEEP-ALIVE SERVER ===
app = Flask("")

@app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    app.run(host="0.0.0.0", port=8080)

threading.Thread(target=run_flask, daemon=True).start()

# === GLOBAL STATES ===
last_15m_state = None
last_1d_state = None
engulfing_triggered = False
last_news_date = None
sent_news_ids = set()

# === FUNCTIONS ===
def send_discord(message: str, chunk_size=1900):
    # chunk if message exceeds Discord limit
    for i in range(0, len(message), chunk_size):
        chunk = message[i:i+chunk_size]
        try:
            requests.post(DISCORD_WEBHOOK, json={"content": chunk})
        except Exception as e:
            print("Discord error:", e)

def get_binance_klines(symbol, interval, limit=500):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        data = requests.get(url, timeout=10).json()
        df = pd.DataFrame(data, columns=["OpenTime","Open","High","Low","Close","Volume","CloseTime",
                                         "QuoteAssetVolume","NumTrades","TBBaseAV","TBQuoteAV","Ignore"])
        df["Open"] = df["Open"].astype(float)
        df["High"] = df["High"].astype(float)
        df["Low"] = df["Low"].astype(float)
        df["Close"] = df["Close"].astype(float)
        df["Volume"] = df["Volume"].astype(float)
        df.index = pd.to_datetime(df["CloseTime"], unit='ms')
        return df
    except Exception as e:
        print("Binance fetch error:", e)
        return pd.DataFrame()

def add_macd(df):
    df["ema_fast"] = df["Close"].ewm(span=FAST, adjust=False).mean()
    df["ema_slow"] = df["Close"].ewm(span=SLOW, adjust=False).mean()
    df["macd"] = df["ema_fast"] - df["ema_slow"]
    df["macd_signal"] = df["macd"].ewm(span=SIGNAL, adjust=False).mean()
    df["ema200"] = df["Close"].ewm(span=200, adjust=False).mean()
    return df

def plot_chart(df, title="Chart"):
    fig, ax = plt.subplots(figsize=(10,5))
    mpf.plot(df[-100:], type='candle', style='charles', ax=ax)
    ax.set_title(title)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf

def detect_engulfing(df):
    # returns True if last candle is engulfing previous
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    # Bullish engulfing
    if curr['Close'] > curr['Open'] and prev['Close'] < prev['Open']:
        if curr['Open'] < prev['Close'] and curr['Close'] > prev['Open']:
            return True
    # Bearish engulfing
    if curr['Close'] < curr['Open'] and prev['Close'] > prev['Open']:
        if curr['Open'] > prev['Close'] and curr['Close'] < prev['Open']:
            return True
    return False

# === News Functions ===
def fetch_crypto_news():
    global sent_news_ids
    url = f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}&currencies=BTC&kind=news"
    try:
        resp = requests.get(url, timeout=10).json()
        news_items = []
        for post in resp.get('results', []):
            if post['id'] in sent_news_ids:
                continue
            title = post['title']
            time_event = pd.to_datetime(post['published_at']).tz_convert('Asia/Kolkata')
            sentiment = post.get('sentiment', 'neutral')
            news_items.append(f"{time_event.strftime('%H:%M')} IST | {title} | Sentiment: {sentiment}")
            sent_news_ids.add(post['id'])
        return news_items
    except Exception as e:
        print("CryptoPanic fetch error:", e)
        return []

def send_daily_news_digest():
    df = get_binance_klines(TICKER, '1d', 2)
    pct_change = round((df['Close'].iloc[-1]-df['Close'].iloc[-2])/df['Close'].iloc[-2]*100,2) if len(df)>=2 else 0
    news_list = fetch_crypto_news()
    message = f"📰 BTC Morning Briefing (8:00 IST)\nPrice Change: {pct_change}%\n"
    if news_list:
        message += "\n".join(news_list)
    else:
        message += "No major news today."
    send_discord(message)

# === MAIN LOOP ===
print("🔄 Bot started... monitoring MACD zero-cross and engulfings")
send_discord(f"✅ Bot started and is monitoring {TICKER}")

while True:
    try:
        now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
        # 8:00 AM IST news digest
        if now_ist.hour == NEWS_DIGEST_HOUR and (last_news_date != now_ist.date()):
            send_daily_news_digest()
            last_news_date = now_ist.date()

        # Fetch 15m and 1d candles
        df_15m = get_binance_klines(TICKER, '15m', 500)
        df_1d = get_binance_klines(TICKER, '1d', 400)

        df_15m = add_macd(df_15m)
        df_1d = add_macd(df_1d)

        # 15m MACD
        macd_val = df_15m["macd"].iloc[-1]
        state_15m = "above" if macd_val > 0 else "below"
        if last_15m_state and last_15m_state != state_15m:
            buf = plot_chart(df_15m, title="BTC 15m MACD Cross")
            send_discord(f"⚡ MACD crossed {state_15m.upper()} zero on {TICKER} (15m)", chunk_size=1900)
        last_15m_state = state_15m

        # 1d MACD
        macd_val_1d = df_1d["macd"].iloc[-1]
        state_1d = "above" if macd_val_1d > 0 else "below"
        if last_1d_state and last_1d_state != state_1d:
            buf1 = plot_chart(df_1d, title="BTC 1D MACD Cross")
            send_discord(f"⚡ MACD crossed {state_1d.upper()} zero on {TICKER} (1D)", chunk_size=1900)
        last_1d_state = state_1d

        # Engulfing after 15m MACD cross
        if not engulfing_triggered and detect_engulfing(df_15m):
            buf_eng = plot_chart(df_15m, title="BTC 15m Engulfing after MACD Cross")
            send_discord(f"🔥 First Engulfing detected after 15m MACD zero cross on {TICKER}", chunk_size=1900)
            engulfing_triggered = True

        # Reset engulfing when MACD crosses back
        if engulfing_triggered and last_15m_state != state_15m:
            engulfing_triggered = False

    except Exception as e:
        print("Error:", e)

    time.sleep(CHECK_DELAY)
