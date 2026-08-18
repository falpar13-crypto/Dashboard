"""
BingX Perpetual - "Élő Gyertya" Skalp Felhalmozás-figyelő (v5)
====================================================================
Kibővítve Killzone logikával, Funding Rate (Squeeze) figyeléssel és 
Független EMA Squeeze riasztásokkal. A spam-visszaigazolások eltávolítva.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import pandas as pd
import requests

# ----------------------------------------------------------------------------
# 1) SKALP PARAMÉTEREK - fix (hardkódolt) globális változók
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "5m"      
MAX_PRICE_CHANGE = 3.0      
MIN_OI_INCREASE = 1.5       # Módosítva 2.5-ről 1.5-re
MIN_CANDLE_VOL_USDT = 15_000  

VOLUME_MA_PERIOD = 10       
MIN_VOL_MULTIPLIER = 2.0    

RANGE_LOOKBACK_PERIOD = 8          
RANGE_COMPRESSION_THRESHOLD_PCT = 1.5  

# --- ÚJ: KILLZONE IDŐABLAKOK (UTC) ---
LONDON_KILLZONE = ("07:00", "10:00")
NY_KILLZONE = ("13:30", "16:00")

TOTAL_RUN_BUDGET_SECONDS = 270   
PASS_INTERVAL_SECONDS = 30       

# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"
PREMIUM_INDEX_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/premiumIndex" # ÚJ: Funding Rate végpont

STATE_FILE = Path(__file__).parent / "alert_state.json"

MIN_VOLUME_USDT = 500_000
MAX_VOLUME_USDT = 15_000_000
NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

OI_TARGET_WINDOW_MINUTES = 5
OI_MIN_WINDOW_MINUTES = 2
OI_MAX_WINDOW_MINUTES = 20
MAX_HISTORY_AGE_MINUTES = 60
ALERT_COOLDOWN_MINUTES = 30

HIGHER_TIMEFRAME = "1h"       
HTF_EMA_PERIOD = 50           
HTF_KLINES_LIMIT = 100        
REQUIRE_HTF_ALIGNMENT = True  

MAX_CONCURRENT_REQUESTS = 12   
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

# Módosítva 65-ről 100-ra, hogy az EMA50 stabilan beálljon az 5 perceseken is
KLINES_LIMIT = 100 

RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ----------------------------------------------------------------------------
# ÁLLAPOT (JSON fájl) KEZELÉSE
# ----------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# ----------------------------------------------------------------------------
# BINGX API HÍVÁSOK
# ----------------------------------------------------------------------------

async def _get_json(session, url, params=None):
    for attempt in range(RETRY_COUNT):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 429:
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    return None

async def fetch_all_tickers(session):
    data = await _get_json(session, TICKER_ENDPOINT)
    if not data or "data" not in data:
        return {}
    result = {}
    for t in data["data"]:
        symbol = t.get("symbol", "")
        if not symbol.endswith("-USDT"):
            continue
        try:
            result[symbol] = {
                "last_price": float(t.get("lastPrice", 0) or 0),
                "quote_volume_24h": float(t.get("quoteVolume", 0) or 0),
            }
        except (TypeError, ValueError):
            continue
    return result

async def fetch_valid_contract_symbols(session):
    data = await _get_json(session, CONTRACTS_ENDPOINT)
    if not data or "data" not in data:
        return None
    return {
        c["symbol"] for c in data["data"]
        if c.get("symbol", "").endswith("-USDT") and c.get("status", 1) == 1
    }

async def fetch_open_interest(session, semaphore, symbol):
    async with semaphore:
        data = await _get_json(session, OI_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        try:
            return symbol, float(data["data"].get("openInterest", 0))
        except (TypeError, ValueError):
            return symbol, None

async def fetch_funding_rate(session, semaphore, symbol):
    """ÚJ: Funding Rate párhuzamos lekérése"""
    async with semaphore:
        data = await _get_json(session, PREMIUM_INDEX_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.03)
        if not data or "data" not in data:
            return symbol, None
        try:
            d = data["data"]
            if isinstance(d, list) and len(d) > 0:
                fr = float(d[0].get("lastFundingRate", 0))
            elif isinstance(d, dict):
                fr = float(d.get("lastFundingRate", 0))
            else:
                return symbol, None
            return symbol, fr * 100  # Százalékos formára alakítva
        except (TypeError, ValueError, KeyError, IndexError):
            return symbol, None

SR_LOOKBACK_PERIOD = 60     
SR_PROXIMITY_PCT = 0.5      

async def fetch_htf_trend(session, semaphore, symbol):
    async with semaphore:
        params = {"symbol": symbol, "interval": HIGHER_TIMEFRAME, "limit": HTF_KLINES_LIMIT}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.03)
        empty_result = {"trend": None, "support": None, "resistance": None}
        if not data or "data" not in data or not data["data"]:
            return symbol, empty_result
        df = pd.DataFrame(data["data"])
        required_cols = {"close", "high", "low", "time"}
        if not required_cols.issubset(df.columns):
            return symbol, empty_result
        for col in ["close", "high", "low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["time"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)

        closed = df.iloc[:-1] 
        if len(closed) < HTF_EMA_PERIOD:
            return symbol, empty_result  

        ema = closed["close"].ewm(span=HTF_EMA_PERIOD, adjust=False).mean()
        last_close = closed["close"].iloc[-1]
        last_ema = ema.iloc[-1]
        trend = None
        if not pd.isna(last_ema):
            if last_close > last_ema:
                trend = "UP"
            elif last_close < last_ema:
                trend = "DOWN"
            else:
                trend = "NEUTRAL"

        support = resistance = None
        sr_window = closed.iloc[-SR_LOOKBACK_PERIOD:]
        if len(sr_window) >= SR_LOOKBACK_PERIOD:
            support = float(sr_window["low"].min())
            resistance = float(sr_window["high"].max())

        return symbol, {"trend": trend, "support": support, "resistance": resistance}

async def fetch_klines(session, semaphore, symbol, interval, limit=KLINES_LIMIT):
    async with semaphore:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        df = pd.DataFrame(data["data"])
        expected = {"open", "close", "high", "low", "volume", "time"}
        if not expected.issubset(df.columns):
            return symbol, None
        df = df.rename(columns={"time": "timestamp"})
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)
        return symbol, df

# ----------------------------------------------------------------------------
# TELEGRAM ÉRTESÍTÉS ÉS FORMÁZÁS
# ----------------------------------------------------------------------------

def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"Telegram küldési hiba: {e}")

DIRECTION_LABELS = {"LONG": "PUMP", "SHORT": "DUMP"}  

def get_killzone_string(current_utc: datetime) -> str:
    """ÚJ: Ellenőrzi, hogy Killzone időszakban vagyunk-e."""
    t_str = current_utc.strftime("%H:%M")
    if LONDON_KILLZONE[0] <= t_str <= LONDON_KILLZONE[1]:
        return "\n⏰ Időszak: London Killzone"
    elif NY_KILLZONE[0] <= t_str <= NY_KILLZONE[1]:
        return "\n⏰ Időszak: New York Killzone"
    return ""

def format_scalp_message(symbol, direction, price, price_change_pct,
                          candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct,
                          htf_trend=None, bounce_confluence=False, near_level_risk=False,
                          rsi=None, macd_status=None, signal_type="STANDARD", 
                          funding_rate=None, current_utc=None):
    
    action = DIRECTION_LABELS.get(direction, direction)
    
    if signal_type == "EMA_SQUEEZE":
        header = f"🗜️ EMA SQUEEZE KITÖRÉS ({action}): <b>{symbol}</b>"
    elif signal_type == "RANGE_BREAKOUT":
        header = f"🎯 SÁV KITÖRÉS ({action}): <b>{symbol}</b>"
    else:
        header = f"⚡ STANDARD {action}: <b>{symbol}</b>"

    warning_line = ""
    against_trend = (
        (direction == "LONG" and htf_trend == "DOWN")
        or (direction == "SHORT" and htf_trend == "UP")
    )
    if against_trend:
        warning_line = f"\n⚠️ Trenddel szemben (1h: {htf_trend})"

    bounce_line = ""
    if bounce_confluence:
        level_type = "támaszról" if direction == "LONG" else "ellenállásról"
        bounce_line = f"\n🎯 Szint-visszapattanás ({level_type})"

    risk_line = ""
    if near_level_risk:
        level_type = "ellenállás" if direction == "LONG" else "támasz"
        risk_line = f"\n⚠️ Közeli {level_type} - onnan visszapattanhat!"

    # ÚJ: Funding Rate és Squeeze indikátor
    funding_str = ""
    if funding_rate is not None:
        squeeze_warn = ""
        if direction == "LONG" and funding_rate <= -0.01:
            squeeze_warn = " 💥 SHORT SQUEEZE (Túl sok a shortos!)"
        elif direction == "SHORT" and funding_rate >= 0.01:
            squeeze_warn = " 💥 LONG SQUEEZE (Túl sok a longos!)"
        funding_str = f"\n💸 Funding: {funding_rate:.4f}%{squeeze_warn}"

    indicator_line = ""
    parts = []
    if rsi is not None:
        rsi_note = " (túlvett)" if rsi >= RSI_OVERBOUGHT else " (túladott)" if rsi <= RSI_OVERSOLD else ""
        parts.append(f"RSI: {rsi:.1f}{rsi_note}")
    if macd_status is not None:
        parts.append(f"MACD: {macd_status}")
    
    if parts:
        indicator_line = f"\n📐 {' | '.join(parts)}"
    
    indicator_line += funding_str

    killzone_line = get_killzone_string(current_utc) if current_utc else ""

    # ÚJ: Szellős dizájn - extra sortörés az elején és a végén
    return (
        f"\n{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{indicator_line}"
        f"{warning_line}"
        f"{bounce_line}"
        f"{risk_line}"
        f"{killzone_line}\n"
    )

def find_oi_baseline(history_without_current, now):
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if OI_MIN_WINDOW_MINUTES <= age_min <= OI_MAX_WINDOW_MINUTES:
            diff = abs(age_min - OI_TARGET_WINDOW_MINUTES)
            if best_diff is None or diff < best_diff:
                best, best_diff = h, diff
    return best

def compute_rsi_macd(close_series: pd.Series):
    if len(close_series) < 35:
        return None, None
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi_series = 100 - (100 / (1 + rs))
    rsi_series = rsi_series.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    rsi_val = rsi_series.iloc[-1]
    rsi_val = round(float(rsi_val), 1) if pd.notna(rsi_val) else None

    ema12 = close_series.ewm(span=12, adjust=False).mean()
    ema26 = close_series.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()

    macd_status = None
    if len(macd_line) >= 2 and not macd_line.iloc[-2:].isna().any():
        prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
        curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
        if prev_diff < 0 and curr_diff > 0:
            macd_status = "Bullish Cross"
        elif prev_diff > 0 and curr_diff < 0:
            macd_status = "Bearish Cross"
        elif curr_diff > 0:
            macd_status = "Bullish"
        else:
            macd_status = "Bearish"

    return rsi_val, macd_status

def evaluate_candle(kdf: pd.DataFrame):
    if kdf is None or len(kdf) < max(VOLUME_MA_PERIOD + 1, 50):
        return None

    live = kdf.iloc[-1]                      
    closed = kdf.iloc[:-1]                    
    baseline_window = closed.iloc[-VOLUME_MA_PERIOD:]
    if len(baseline_window) < VOLUME_MA_PERIOD:
        return None

    prev_close = closed.iloc[-1]["close"]     
    if prev_close <= 0 or live["open"] <= 0:
        return None

    avg_vol = baseline_window["volume"].mean()
    if pd.isna(avg_vol) or avg_vol <= 0:
        return None

    current_price = float(live["close"])      
    price_change_pct = (current_price - prev_close) / prev_close * 100
    vol_multiplier = live["volume"] / avg_vol
    candle_vol_usdt = float(live["volume"] * current_price)
    direction = "LONG" if current_price >= live["open"] else "SHORT"

    rsi_val, macd_status = compute_rsi_macd(kdf["close"])

    # 1. SÁV KITÖRÉS LOGIKA
    range_window = closed.iloc[-RANGE_LOOKBACK_PERIOD:]
    signal_type = "STANDARD"
    
    if len(range_window) >= RANGE_LOOKBACK_PERIOD:
        range_high = float(range_window["high"].max())
        range_low = float(range_window["low"].min())
        if range_low > 0:
            range_width_pct = (range_high - range_low) / range_low * 100
            is_tight_range = range_width_pct <= RANGE_COMPRESSION_THRESHOLD_PCT
            is_breakout = (
                (direction == "LONG" and current_price > range_high)
                or (direction == "SHORT" and current_price < range_low)
            )
            if is_tight_range and is_breakout:
                signal_type = "RANGE_BREAKOUT"

    # 2. ÚJ: EMA SQUEEZE LOGIKA (Felülírja a RANGE_BREAKOUT-ot, ha teljesül, mert ritkább és erősebb)
    ema20 = kdf["close"].ewm(span=20, adjust=False).mean()
    ema50 = kdf["close"].ewm(span=50, adjust=False).mean()
    
    last_4_closed = closed.iloc[-4:]
    ema20_closed = ema20.iloc[-5:-1] 
    ema50_closed = ema50.iloc[-5:-1]

    if len(last_4_closed) == 4:
        e20_live = ema20.iloc[-1]
        e50_live = ema50.iloc[-1]
        dist_pct = abs(e20_live - e50_live) / min(e20_live, e50_live) * 100

        if dist_pct <= 1.5:
            touching = True
            for i in range(4):
                high = last_4_closed.iloc[i]["high"]
                low = last_4_closed.iloc[i]["low"]
                e20_c = ema20_closed.iloc[i]
                e50_c = ema50_closed.iloc[i]
                
                max_e = max(e20_c, e50_c)
                min_e = min(e20_c, e50_c)
                
                # Ellenőrizzük, hogy a gyertya érinti-e a két EMA közötti területet
                if not (low <= max_e and high >= min_e):
                    touching = False
                    break
                    
            if touching:
                max_high_4 = last_4_closed["high"].max()
                min_low_4 = last_4_closed["low"].min()

                if direction == "LONG" and current_price > max(e20_live, e50_live) and current_price > max_high_4:
                    signal_type = "EMA_SQUEEZE"
                elif direction == "SHORT" and current_price < min(e20_live, e50_live) and current_price < min_low_4:
                    signal_type = "EMA_SQUEEZE"

    return {
        "price": current_price,
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
        "direction": direction,
        "candle_open_ts": live["timestamp"].isoformat(),
        "rsi": rsi_val,
        "macd_status": macd_status,
        "signal_type": signal_type
    }

# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR
# ----------------------------------------------------------------------------

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            return 0, 0, valid_contracts, htf_cache

        if valid_contracts is None:
            valid_contracts = await fetch_valid_contract_symbols(session)

        candidates = []
        for s, info in tickers.items():
            if not (MIN_VOLUME_USDT <= info["quote_volume_24h"] <= MAX_VOLUME_USDT):
                continue
            if not is_probably_crypto(s):
                continue
            if valid_contracts is not None and s not in valid_contracts:
                continue
            candidates.append(s)

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        missing_htf = [s for s in candidates if s not in htf_cache]

        # ÚJ: A Funding Rate API hívása is bekerült a párhuzamos gather blokkba
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, semaphore, s) for s in missing_htf]
        fr_tasks = [fetch_funding_rate(session, semaphore, s) for s in candidates]

        oi_results, kline_results, htf_results, fr_results = await asyncio.gather(
            asyncio.gather(*oi_tasks),
            asyncio.gather(*kline_tasks),
            asyncio.gather(*htf_tasks),
            asyncio.gather(*fr_tasks)
        )

        if htf_results:
            for s, htf_data in htf_results:
                if htf_data is not None and htf_data.get("trend") is not None:
                    htf_cache[s] = htf_data

    oi_map = {s: oi for s, oi in oi_results if oi is not None}
    klines_map = {s: df for s, df in kline_results if df is not None}
    fr_map = {s: fr for s, fr in fr_results if fr is not None}

    alerts_sent = 0
    evaluated = 0

    for symbol in candidates:
        candle = evaluate_candle(klines_map.get(symbol))
        oi_now = oi_map.get(symbol)
        funding_rate = fr_map.get(symbol)
        
        if candle is None or oi_now is None:
            continue
        evaluated += 1

        entry = state.setdefault(symbol, {"oi_history": [], "last_alert_ts": None})
        entry["oi_history"].append({"ts": now.isoformat(), "oi": oi_now})
        cutoff = now - timedelta(minutes=MAX_HISTORY_AGE_MINUTES)
        entry["oi_history"] = [
            h for h in entry["oi_history"] if datetime.fromisoformat(h["ts"]) >= cutoff
        ]

        oi_baseline = find_oi_baseline(entry["oi_history"][:-1], now)
        if oi_baseline is None or oi_baseline["oi"] <= 0:
            continue

        oi_change_pct = (oi_now - oi_baseline["oi"]) / oi_baseline["oi"] * 100

        htf_data = htf_cache.get(symbol, {})
        htf_trend = htf_data.get("trend")
        support = htf_data.get("support")
        resistance = htf_data.get("resistance")

        against_trend = REQUIRE_HTF_ALIGNMENT and (
            (candle["direction"] == "LONG" and htf_trend == "DOWN")
            or (candle["direction"] == "SHORT" and htf_trend == "UP")
        )

        price = candle["price"]
        near_support = support is not None and support > 0 and abs(price - support) / support * 100 <= SR_PROXIMITY_PCT
        near_resistance = resistance is not None and resistance > 0 and abs(price - resistance) / resistance * 100 <= SR_PROXIMITY_PCT

        near_level_risk = (
            (candle["direction"] == "LONG" and near_resistance)
            or (candle["direction"] == "SHORT" and near_support)
        )
        bounce_confluence = (
            (candle["direction"] == "LONG" and near_support)
            or (candle["direction"] == "SHORT" and near_resistance)
        )

        # ÚJ: Lazább feltételek az EMA SQUEEZE esetében, mivel a setup vizuálisan nagyon erős
        is_ema_squeeze = (candle["signal_type"] == "EMA_SQUEEZE")
        req_oi = MIN_OI_INCREASE * 0.6 if is_ema_squeeze else MIN_OI_INCREASE
        req_vol = MIN_VOL_MULTIPLIER * 0.7 if is_ema_squeeze else MIN_VOL_MULTIPLIER

        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= req_oi
            and candle["vol_multiplier"] >= req_vol
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        if is_setup and cooldown_ok:
            msg = format_scalp_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                oi_now, oi_change_pct, htf_trend=htf_trend,
                bounce_confluence=bounce_confluence, near_level_risk=near_level_risk,
                rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                signal_type=candle.get("signal_type", "STANDARD"),
                funding_rate=funding_rate, current_utc=now
            )
            send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1

    return alerts_sent, evaluated, valid_contracts, htf_cache

# ----------------------------------------------------------------------------
# FŐ CIKLUS
# ----------------------------------------------------------------------------

RUN_LOCK_STALE_MINUTES = (TOTAL_RUN_BUDGET_SECONDS / 60) + 2  

async def main():
    state = load_state()
    now_start = datetime.now(timezone.utc)

    existing_lock = state.get("_run_lock")
    if existing_lock:
        try:
            lock_age_minutes = (now_start - datetime.fromisoformat(existing_lock)).total_seconds() / 60
        except (ValueError, TypeError):
            lock_age_minutes = None
        if lock_age_minutes is not None and lock_age_minutes < RUN_LOCK_STALE_MINUTES:
            return

    state["_run_lock"] = now_start.isoformat()
    save_state(state) 

    try:
        await _run_main_loop(state)
    finally:
        state["_run_lock"] = None
        save_state(state)

async def _run_main_loop(state: dict):
    loop_start = time.monotonic()
    valid_contracts = None
    htf_cache = {}   
    pass_num = 0
    total_alerts = 0

    while True:
        elapsed_total = time.monotonic() - loop_start
        if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
            break

        pass_num += 1
        pass_start = time.monotonic()
        now = datetime.now(timezone.utc)

        remaining_budget = max(30.0, TOTAL_RUN_BUDGET_SECONDS - elapsed_total)
        try:
            alerts, evaluated, valid_contracts, htf_cache = await asyncio.wait_for(
                run_single_pass(state, valid_contracts, htf_cache, now),
                timeout=remaining_budget,
            )
        except asyncio.TimeoutError:
            save_state(state)
            break

        total_alerts += alerts
        save_state(state)  

        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break

        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva.")
    asyncio.run(main())

