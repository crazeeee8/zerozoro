import yfinance as yf

import pandas as pd

import requests

import time

from datetime import datetime

from flask import Flask

import threading



# === CONFIG ===

TICKER = "BTC-USD"

INTERVAL = "15m"

DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1374711617127841922/U8kaZV_I_l1P6H6CFnBg6oWAFLnEUMLfiFpzq-DGM4GJrraRlYvHSHifboWqnYjkUYNR"

FAST, SLOW, SIGNAL = 8, 15, 9

CHECK_DELAY = 60 # seconds between checks



# === FLASK KEEP-ALIVE SERVER ===

app = Flask("")



@app.route("/")

def home():

    return "Bot is running!"



def run_flask():

    app.run(host="0.0.0.0", port=8080)



threading.Thread(target=run_flask, daemon=True).start()



# === FUNCTIONS ===

def send_alert(message: str):

    try:

        requests.post(DISCORD_WEBHOOK, json={"content": message})

    except Exception as e:

        print("Discord error:", e)



def send_startup_ping():

    msg = f"✅ Bot started and is monitoring {TICKER} ({INTERVAL}) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    print(msg)

    send_alert(msg)



def get_data():

    df = yf.download(tickers=TICKER, interval=INTERVAL, period="2d")

    df.dropna(inplace=True)

    return df



def macd(df, fast=FAST, slow=SLOW, signal=SIGNAL):

    df["ema_fast"] = df["Close"].ewm(span=fast, adjust=False).mean()

    df["ema_slow"] = df["Close"].ewm(span=slow, adjust=False).mean()

    df["macd"] = df["ema_fast"] - df["ema_slow"]

    df["signal"] = df["macd"].ewm(span=signal, adjust=False).mean()

    return df



# === MAIN LOOP ===

last_state = None

print("🔄 Bot started... monitoring MACD zero-cross on", TICKER)

send_startup_ping()



while True:

    try:

        df = get_data()

        df = macd(df)

        macd_val = df["macd"].iloc[-1]



        state = "above" if macd_val > 0 else "below"



        if last_state and last_state != state:

            msg = f"⚡ MACD crossed {state.upper()} zero on {TICKER} ({INTERVAL}) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            print(msg)

            send_alert(msg)



        last_state = state

    except Exception as e:

        print("Error:", e)



    time.sleep(CHECK_DELAY)

