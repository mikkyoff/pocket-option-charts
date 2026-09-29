"""
Pocket Option BITB_otc 10s Candle + Indicator Streamer
- Real-time RSI(14), Bollinger Bands(20,2), EMA(6)
- Serves candles + indicator values to the frontend via HTTP
- Designed for Railway (gunicorn + background thread)
"""

import os
import json
import asyncio
import threading
import time
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv

from BinaryOptionsToolsV2 import PocketOptionAsync
from BinaryOptionsToolsV2.config import Config

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='')
CORS(app)

# ============================================================
# CONFIGURATION
# ============================================================
SSID = os.getenv("POCKET_OPTION_SSID")
ASSET = "BITB_otc"
TIMEFRAME_SECONDS = 10           # 10 second candles
HISTORY_CANDLES = 150            # Bootstrap history for indicators
MAX_CANDLES = 2000               # Rolling buffer for chart

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD_MULT = 2.0
EMA_PERIOD = 6

# ============================================================
# SHARED STATE
# ============================================================
class StreamState:
    def __init__(self):
        self.candles = deque(maxlen=MAX_CANDLES)  # closed candles (dicts)
        self.forming = None                        # current forming candle
        self.lock = threading.Lock()
        self.connected = False
        self.initialized = False                   # true once history + first boundary hit
        self.last_tick_price = None
        self.last_tick_ts = None
        self.tick_count = 0
        self.last_candle_boundary = None
        self.error = None

        # Indicator caches (recomputed every tick)
        self.rsi = None
        self.bb_upper = None
        self.bb_middle = None
        self.bb_lower = None
        self.bb_pct_to_upper = None  # % headspace between price and upper band
        self.bb_pct_to_lower = None  # % headspace between price and lower band
        self.ema = None
        self.ema_signal = None       # "ABOVE" or "BELOW"

state = StreamState()

# ============================================================
# INDICATOR MATH (pure Python, no numpy dependency)
# ============================================================
def compute_rsi(closes, period=14):
    """
    Wilder's RSI. `closes` is a list of floats (oldest → newest).
    Returns the RSI value for the latest close, or None if not enough data.
    """
    if len(closes) < period + 1:
        return None

    gains = 0.0
    losses = 0.0
    # Seed: simple average of first `period` deltas
    for i in range(1, period + 1):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses += -delta
    avg_gain = gains / period
    avg_loss = losses / period

    # Wilder smoothing for the rest
    for i in range(period + 1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gain = delta if delta > 0 else 0.0
        loss = -delta if delta < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_bollinger(closes, period=20, mult=2.0):
    """
    Returns (upper, middle, lower) using population stddev.
    """
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    stddev = variance ** 0.5
    upper = middle + mult * stddev
    lower = middle - mult * stddev
    return upper, middle, lower


def compute_ema(closes, period=6):
    """
    Exponential Moving Average (standard SMA seed).
    """
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    # Seed with SMA of first `period`
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def recompute_indicators(closes):
    """
    Given a list of closes (closed candles + current forming close),
    returns a dict with all indicator values.
    """
    rsi = compute_rsi(closes, RSI_PERIOD)
    bb_u, bb_m, bb_l = compute_bollinger(closes, BB_PERIOD, BB_STD_MULT)
    ema = compute_ema(closes, EMA_PERIOD)
    price = closes[-1] if closes else None

    bb_pct_to_upper = None
    bb_pct_to_lower = None
    if bb_u is not None and bb_l is not None and price is not None:
        # % of the upper band that the price could still travel upward
        if bb_u > 0:
            bb_pct_to_upper = (bb_u - price) / bb_u * 100.0
        # % of the price that the price sits above the lower band
        if price > 0:
            bb_pct_to_lower = (price - bb_l) / price * 100.0

    ema_signal = None
    if ema is not None and price is not None:
        ema_signal = "ABOVE" if price > ema else "BELOW"

    return {
        "rsi": rsi,
        "bb_upper": bb_u,
        "bb_middle": bb_m,
        "bb_lower": bb_l,
        "bb_pct_to_upper": bb_pct_to_upper,
        "bb_pct_to_lower": bb_pct_to_lower,
        "ema": ema,
        "ema_signal": ema_signal,
    }


# ============================================================
# POCKET OPTION STREAM
# ============================================================
def run_po_stream():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_po_stream_async())
    except Exception as e:
        print(f"[stream] fatal: {e}")
        with state.lock:
            state.error = str(e)


async def _po_stream_async():
    if not SSID:
        print("ERROR: POCKET_OPTION_SSID not set")
        with state.lock:
            state.error = "POCKET_OPTION_SSID not set"
        return

    config = Config(timeout_secs=30, terminal_logging=False)
    client = PocketOptionAsync(SSID, config=config)

    # Wait for assets to load (context manager does this too)
    try:
        await client.wait_for_assets(timeout=60.0)
        balance = await client.balance()
        print(f"[stream] Connected. Balance: {balance} | Demo: {client.is_demo()}")
        with state.lock:
            state.connected = True
    except Exception as e:
        print(f"[stream] Connection failed: {e}")
        with state.lock:
            state.connected = False
            state.error = f"Connection failed: {e}"
        return

    # ---------------------------------------------------------
    # STEP 1: Fetch 150 historical closed candles (10s timeframe)
    # ---------------------------------------------------------
    # `get_candles_live` yields (closed_candles, forming_candle).
    # We call it once and take the first yield to seed history.
    # Alternatively `get_candles` is deprecated but works; we use
    # `get_candles_live` because it is the supported path.
    history_closes = []
    try:
        gen = client.get_candles_live(
            asset=ASSET,
            period=TIMEFRAME_SECONDS,
            hours=1.0,
            max_rows=HISTORY_CANDLES,
        )
        closed, forming = await gen.__anext__()
        # `closed` is list of dicts with keys time/open/high/low/close
        for c in closed[-HISTORY_CANDLES:]:
            with state.lock:
                state.candles.append({
                    "time": int(c["time"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                })
            history_closes.append(float(c["close"]))
        print(f"[stream] Seeded {len(history_closes)} historical candles")

        # Also seed forming if provided
        if forming:
            with state.lock:
                state.forming = {
                    "time": int(forming["time"]),
                    "open": float(forming["open"]),
                    "high": float(forming["high"]),
                    "low": float(forming["low"]),
                    "close": float(forming["close"]),
                }
                state.last_candle_boundary = state.forming["time"]
        # Close the generator (we don't need its live loop; we run our own)
        await gen.aclose()
    except Exception as e:
        print(f"[stream] History fetch failed (will still stream live): {e}")

    # ---------------------------------------------------------
    # STEP 2: Subscribe to raw ticks
    # ---------------------------------------------------------
    stream = await client.subscribe_symbol(ASSET)

    print(f"[stream] Streaming ticks for {ASSET} @ {TIMEFRAME_SECONDS}s")
    with state.lock:
        state.initialized = True

    # ---------------------------------------------------------
    # STEP 3: Process ticks → build forming candle → indicators
    # ---------------------------------------------------------
    async for tick in stream:
        try:
            price = float(tick.get("close") or tick.get("price") or 0)
            ts = int(tick.get("timestamp") or tick.get("time") or time.time())
            if price <= 0:
                continue

            bucket = (ts // TIMEFRAME_SECONDS) * TIMEFRAME_SECONDS

            with state.lock:
                state.tick_count += 1
                state.last_tick_price = price
                state.last_tick_ts = ts

                if state.forming is None:
                    state.forming = {
                        "time": bucket,
                        "open": price, "high": price, "low": price, "close": price,
                    }
                    state.last_candle_boundary = bucket
                elif bucket > state.last_candle_boundary:
                    # Close the previous forming candle
                    state.candles.append(dict(state.forming))
                    state.last_candle_boundary = bucket
                    state.forming = {
                        "time": bucket,
                        "open": price, "high": price, "low": price, "close": price,
                    }
                else:
                    state.forming["high"] = max(state.forming["high"], price)
                    state.forming["low"] = min(state.forming["low"], price)
                    state.forming["close"] = price

                # --- Indicator recompute (on every tick) ---
                closes = [c["close"] for c in state.candles]
                closes.append(state.forming["close"])
                ind = recompute_indicators(closes)

                state.rsi = ind["rsi"]
                state.bb_upper = ind["bb_upper"]
                state.bb_middle = ind["bb_middle"]
                state.bb_lower = ind["bb_lower"]
                state.bb_pct_to_upper = ind["bb_pct_to_upper"]
                state.bb_pct_to_lower = ind["bb_pct_to_lower"]
                state.ema = ind["ema"]
                state.ema_signal = ind["ema_signal"]

        except Exception as e:
            print(f"[stream] Tick error: {e}")
            continue


# ============================================================
# HTTP ROUTES
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/candles")
def get_candles():
    with state.lock:
        return jsonify({
            "asset": ASSET,
            "timeframe": TIMEFRAME_SECONDS,
            "candles": list(state.candles),
            "forming": state.forming,
            "connected": state.connected,
            "initialized": state.initialized,
            "tick_count": state.tick_count,
            "last_tick_price": state.last_tick_price,
            "last_tick_ts": state.last_tick_ts,
            "indicators": {
                "rsi": state.rsi,
                "rsi_period": RSI_PERIOD,
                "bb_upper": state.bb_upper,
                "bb_middle": state.bb_middle,
                "bb_lower": state.bb_lower,
                "bb_period": BB_PERIOD,
                "bb_std": BB_STD_MULT,
                "bb_pct_to_upper": state.bb_pct_to_upper,
                "bb_pct_to_lower": state.bb_pct_to_lower,
                "ema": state.ema,
                "ema_period": EMA_PERIOD,
                "ema_signal": state.ema_signal,
            },
            "error": state.error,
        })


@app.route("/api/health")
def health():
    with state.lock:
        return jsonify({
            "status": "ok",
            "connected": state.connected,
            "initialized": state.initialized,
            "candles_held": len(state.candles),
            "ticks": state.tick_count,
            "error": state.error,
        })


# ============================================================
# STARTUP
# ============================================================
def _start_background():
    t = threading.Thread(target=run_po_stream, daemon=True)
    t.start()

_start_background()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
