"""
Pocket Option EURGBP_otc Candle + Indicator Streamer
- Candles built 100% from live ticks (bypasses stale API candles)
- Real-time RSI(14), Bollinger Bands(20,2), EMA(6)
- 1-minute timeframe
"""

import os
import asyncio
import threading
import time
from collections import deque
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
ASSET = "EURGBP_otc"
TIMEFRAME_SECONDS = 10           # 1 minute candles
HISTORY_CANDLES = 150            # Bootstrap history for indicators
MAX_CANDLES = 2000

RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD_MULT = 2.0
EMA_PERIOD = 6

# ============================================================
# SHARED STATE
# ============================================================
class StreamState:
    def __init__(self):
        self.candles = deque(maxlen=MAX_CANDLES)   # closed candles
        self.forming = None                        # current forming candle
        self.lock = threading.Lock()
        self.connected = False
        self.initialized = False
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
        self.bb_bandwidth = None      # total width as % of middle
        self.bb_pct_to_upper = None   # % of band width above price
        self.bb_pct_to_lower = None   # % of band width below price
        self.ema = None
        self.ema_signal = None        # "ABOVE" or "BELOW"

state = StreamState()

# ============================================================
# INDICATOR MATH
# ============================================================
def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses += -delta
    avg_gain = gains / period
    avg_loss = losses / period
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
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    stddev = variance ** 0.5
    return middle + mult * stddev, middle, middle - mult * stddev


def compute_ema(closes, period=6):
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def recompute_indicators(closes):
    """
    Returns indicator dict. Bollinger % calcs use the FULL band width
    (upper - lower) as the denominator, so pct_to_upper + pct_to_lower = 100.
    """
    rsi = compute_rsi(closes, RSI_PERIOD)
    bb_u, bb_m, bb_l = compute_bollinger(closes, BB_PERIOD, BB_STD_MULT)
    ema = compute_ema(closes, EMA_PERIOD)
    price = closes[-1] if closes else None

    bb_bandwidth = None
    bb_pct_to_upper = None
    bb_pct_to_lower = None
    if bb_u is not None and bb_l is not None and bb_m and bb_m > 0 and price is not None:
        bb_bandwidth = (bb_u - bb_l) / bb_m * 100.0
        full_width = bb_u - bb_l
        if full_width > 0:
            bb_pct_to_upper = (bb_u - price) / full_width * 100.0
            bb_pct_to_lower = (price - bb_l) / full_width * 100.0
            # Clamp to [0, 100] in case price briefly breaks outside the band
            bb_pct_to_upper = max(0.0, min(100.0, bb_pct_to_upper))
            bb_pct_to_lower = max(0.0, min(100.0, bb_pct_to_lower))

    ema_signal = None
    if ema is not None and price is not None:
        ema_signal = "ABOVE" if price > ema else "BELOW"

    return {
        "rsi": rsi,
        "bb_upper": bb_u,
        "bb_middle": bb_m,
        "bb_lower": bb_l,
        "bb_bandwidth": bb_bandwidth,
        "bb_pct_to_upper": bb_pct_to_upper,
        "bb_pct_to_lower": bb_pct_to_lower,
        "ema": ema,
        "ema_signal": ema_signal,
    }


# ============================================================
# POCKET OPTION STREAM (ticks → candles → indicators)
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
        with state.lock:
            state.error = "POCKET_OPTION_SSID not set"
        return

    config = Config(timeout_secs=30, terminal_logging=False)
    client = PocketOptionAsync(SSID, config=config)

    try:
        await client.wait_for_assets(timeout=60.0)
        balance = await client.balance()
        print(f"[stream] Connected. Balance: {balance} | Demo: {client.is_demo()}")
        with state.lock:
            state.connected = True
    except Exception as e:
        with state.lock:
            state.connected = False
            state.error = f"Connection failed: {e}"
        return

    # ---------------------------------------------------------
    # STEP 1: Fetch 150 historical CLOSED candles to seed
    # the indicator math. These are NOT used for display — only
    # as the seed so RSI/BB/EMA are immediately accurate.
    # ---------------------------------------------------------
    seed_closes = []
    try:
        # get_candles_live returns an async generator; we take the
        # first yield (historical backfill + forming) and then close it.
        gen = client.get_candles_live(
            asset=ASSET,
            period=TIMEFRAME_SECONDS,
            hours=3.0,
            max_rows=HISTORY_CANDLES,
        )
        closed, _forming = await gen.__anext__()
        for c in closed[-HISTORY_CANDLES:]:
            with state.lock:
                state.candles.append({
                    "time": int(c["time"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                })
            seed_closes.append(float(c["close"]))
        print(f"[stream] Seeded {len(seed_closes)} historical candles")
        await gen.aclose()
    except Exception as e:
        print(f"[stream] History seed failed (indicators will warm up live): {e}")

    # ---------------------------------------------------------
    # STEP 2: Subscribe to raw ticks. EVERY candle is built
    # from these ticks. We never trust the API's candle feed
    # again after this point.
    # ---------------------------------------------------------
    stream = await client.subscribe_symbol(ASSET)
    print(f"[stream] Streaming ticks for {ASSET} @ {TIMEFRAME_SECONDS}s")
    with state.lock:
        state.initialized = True

    # Track the last accepted tick timestamp to reject out-of-order ticks
    last_accepted_ts = 0
    # Tolerance: reject ticks older than 2× timeframe (guards against replay)
    stale_cutoff = TIMEFRAME_SECONDS * 2

    async for tick in stream:
        try:
            price = float(tick.get("close") or tick.get("price") or 0)
            ts = int(tick.get("timestamp") or tick.get("time") or time.time())
            if price <= 0:
                continue

            # Reject stale ticks
            now = int(time.time())
            if ts < now - stale_cutoff:
                continue
            if ts < last_accepted_ts:
                continue
            last_accepted_ts = ts

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
                    # Previous candle closes
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

                # Recompute indicators on every tick
                closes = [c["close"] for c in state.candles]
                closes.append(state.forming["close"])
                ind = recompute_indicators(closes)

                state.rsi = ind["rsi"]
                state.bb_upper = ind["bb_upper"]
                state.bb_middle = ind["bb_middle"]
                state.bb_lower = ind["bb_lower"]
                state.bb_bandwidth = ind["bb_bandwidth"]
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
                "bb_bandwidth": state.bb_bandwidth,
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
