"""
Pocket Option COIN_otc Candle Streamer
Runs on Railway, serves candles via HTTP + WebSocket to the frontend.
"""

import os
import json
import asyncio
import threading
import time
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from dotenv import load_dotenv

# Pocket Option library
from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# ============================================================
# CONFIGURATION
# ============================================================
SSID = os.getenv("POCKET_OPTION_SSID")
ASSET = "COIN_otc"
TIMEFRAME_SECONDS = 10          # 10-second candles
MAX_CANDLES = 2000               # Keep last 2000 candles in memory

# ============================================================
# SHARED STATE
# ============================================================
class CandleStore:
    def __init__(self):
        self.candles = deque(maxlen=MAX_CANDLES)   # list of dicts
        self.forming = None                        # current forming candle
        self.lock = threading.Lock()
        self.connected = False
        self.last_tick = None
        self.tick_count = 0

store = CandleStore()

# ============================================================
# POCKET OPTION STREAM (runs in background thread)
# ============================================================
def run_po_stream():
    """Connect to Pocket Option and stream candles into the shared store."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_po_stream_async())


async def _po_stream_async():
    if not SSID:
        print("ERROR: POCKET_OPTION_SSID not set")
        return

    config = Config(timeout_secs=30, terminal_logging=False)
    client = PocketOptionAsync(SSID, config=config)

    try:
        balance = await client.balance()
        print(f"Connected. Balance: {balance} | Demo: {client.is_demo()}")
        store.connected = True
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Subscribe to raw ticks
    stream = await client.subscribe_symbol(ASSET)
    last_bucket = None
    forming = None

    async for tick in stream:
        try:
            price = float(tick.get("close") or tick.get("price") or 0)
            ts = int(tick.get("timestamp") or tick.get("time") or time.time())
            if price <= 0:
                continue

            bucket = (ts // TIMEFRAME_SECONDS) * TIMEFRAME_SECONDS

            with store.lock:
                store.tick_count += 1
                store.last_tick = price

                if last_bucket is None:
                    last_bucket = bucket
                    forming = {
                        "time": bucket,
                        "open": price, "high": price, "low": price, "close": price,
                    }
                elif bucket > last_bucket:
                    # Candle closed
                    store.candles.append(forming)
                    last_bucket = bucket
                    forming = {
                        "time": bucket,
                        "open": price, "high": price, "low": price, "close": price,
                    }
                else:
                    forming["high"] = max(forming["high"], price)
                    forming["low"] = min(forming["low"], price)
                    forming["close"] = price

                store.forming = forming

        except Exception as e:
            print(f"Tick error: {e}")
            continue


# ============================================================
# HTTP ROUTES
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def get_candles():
    """Return all stored closed candles + current forming candle."""
    with store.lock:
        return jsonify({
            "candles": list(store.candles),
            "forming": store.forming,
            "connected": store.connected,
            "tick_count": store.tick_count,
            "last_tick": store.last_tick,
        })


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "connected": store.connected,
        "candles_held": len(store.candles),
        "ticks": store.tick_count,
    })


# ============================================================
# STARTUP
# ============================================================
if __name__ == "__main__":
    # Start PO stream in a background thread
    thread = threading.Thread(target=run_po_stream, daemon=True)
    thread.start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
else:
    # When run under gunicorn, still start the background thread
    thread = threading.Thread(target=run_po_stream, daemon=True)
    thread.start()
