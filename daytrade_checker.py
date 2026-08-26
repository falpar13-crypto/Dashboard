"""
BingX Perpetual - Daytrade Felhalmozás-figyelő (1h idősík)
====================================================================
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional, TypedDict

import aiohttp
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daytrade_checker")

# ----------------------------------------------------------------------------
# 1) DAYTRADE PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1h"      
CANDLE_DURATION_SECONDS = 3600  
# SZIGORÍTVA: 7.0 -> 5.0 - kevésbé engedjük be a már jócskán kifutott mozgásokat
MAX_PRICE_CHANGE = 5.0      
# SZIGORÍTVA: 3.0 -> 4.0 - nagyobb OI-elmozdulás kell a valódi felhalmozáshoz
MIN_OI_INCREASE = 4.0       
# SZIGORÍTVA: 50 000 -> 75 000 - arányosan a MIN_VOL_MULTIPLIER emeléséhez
MIN_CANDLE_VOL_USDT = 75_000  

VOLUME_MA_PERIOD = 12       
# SZIGORÍTVA: 1.8 -> 2.5 - ez volt a leggyengébb pont; a scalp botban is 2.5x
# a bevált, szigorú küszöb, és semmi nem indokolta, hogy itt lazább legyen
MIN_VOL_MULTIPLIER = 2.5    

# SZIGORÍTVA: 3.5 -> 5.0 - az EARLY egy VETÍTETT (extrapolált) szám, tehát
# eleve zajosabb, mint a STANDARD - alacsony küszöbbel könnyen "belövi"
# magát egy random kilengés is egy 1 órás gyertya elején. A scalp botban is
# 5.0x a bevált érték ugyanerre a célra.
EARLY_MIN_PACE_VOL_MULT = 5.0    
EARLY_MIN_ELAPSED_FRACTION = 0.1  
EARLY_MAX_ELAPSED_FRACTION = 0.5   
# SZIGORÍTVA: 20 000 -> 35 000 - arányosan a MIN_CANDLE_VOL_USDT emeléséhez
EARLY_MIN_CANDLE_VOL_USDT = 35_000  

OI_FAST_TARGET_WINDOW_MINUTES = 15
OI_FAST_MIN_WINDOW_MINUTES = 5
OI_FAST_MAX_WINDOW_MINUTES = 30
# SZIGORÍTVA: 1.5 -> 2.0
EARLY_MIN_OI_FAST_INCREASE = 2.0   

FUNDING_SQUEEZE_THRESHOLD_PCT = 0.01

TOTAL_RUN_BUDGET_SECONDS = 520   
PASS_INTERVAL_SECONDS = 30       

# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"
FUNDING_RATE_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/premiumIndex"

STATE_FILE = Path(__file__).parent / "daytrade_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "daytrade_alert_log.jsonl"

MIN_VOLUME_USDT = 1_000_000
MAX_VOLUME_USDT = 150_000_000

NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

OI_TARGET_WINDOW_MINUTES = 60
OI_MIN_WINDOW_MINUTES = 30
OI_MAX_WINDOW_MINUTES = 120
MAX_HISTORY_AGE_MINUTES = 360

# SZIGORÍTVA: 120 -> 240 perc (4 óra) - egy napon belüli mozgás sokáig
# tarthat, nem akarunk 2 óránként újra jelzést kapni ugyanarra a folytatódó
# trendre (ez is hozzájárulhatott a "sok jelzés" érzethez)
ALERT_COOLDOWN_MINUTES = 240

HIGHER_TIMEFRAME = "4h"       
HTF_KLINES_LIMIT = 100        
REQUIRE_HTF_ALIGNMENT = True  

# ÚJ: HH/HL/LH/LL (swing-struktúra) alapú trendfelismerés - LECSERÉLI az
# EMA(50)-alapú módszert (ugyanaz a csere, mint a scalp botban/alert_checker.py-
# ban). Az EMA(50) 4h gyertyákon LASSÚ: 50*4h = 200 óra (kb. 8.3 nap) kell,
# mire stabilizálódik, és egy trendváltás után is hosszan "elmarad" a valós
# ártól. A price-action (swing-struktúra) megközelítés a tényleges
# csúcsokat/mélypontokat nézi:
#   - UP: az utolsó két swing csúcs egyre magasabb (Higher High) ÉS az
#     utolsó két swing mélypont egyre magasabb (Higher Low).
#   - DOWN: az utolsó két swing csúcs egyre alacsonyabb (Lower High) ÉS az
#     utolsó két swing mélypont egyre alacsonyabb (Lower Low).
#   - Minden más eset (pl. HH+LL vagy LH+HL - vegyes szerkezet) NEUTRAL.
SWING_FRACTAL_LEGS = 2   # ennyi gyertyát nézünk MINDKÉT oldalon egy swing
                           # csúcs/mélypont azonosításához ("5 gyertyás fraktál")

MAX_CONCURRENT_REQUESTS = 16   
KLINES_MAX_CONCURRENT_REQUESTS = 4
KLINES_REQUEST_PACING_SECONDS = 0.2  

REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

HTF_FETCH_BATCH_SIZE = 20

_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150  

KLINES_LIMIT = 120
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ÚJ: CVD (Cumulative Volume Delta) - lásd az alert_checker.py-ban lévő
# fetch_cvd_confirmation() blokk-kommentjét, ide azonos logikával került át.
TRADES_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/trades"
CVD_LOOKBACK_TRADES = 500
CVD_CONFIRM_RATIO = 0.55
CVD_DIVERGENCE_RATIO = 0.55
CVD_TREND_DELTA_THRESHOLD = 0.03

# ÚJ: MEGBÍZHATÓSÁGI (confluence) pontszám - lásd az alert_checker.py azonos
# blokk-kommentjét. NEM szűr, NEM blokkol, csak összegzi a fejléc alá.
FUNDING_MOMENTUM_THRESHOLD_PCT = 0.002

# ÚJ: FELHALMOZÁS (accumulation-only) figyelmeztető jelzés - lásd az
# alert_checker.py azonos blokk-kommentjét. A daytrade (1h) idősíkhoz
# igazított, szélesebb küszöbökkel.
ACCUM_MAX_PRICE_CHANGE = 1.0        # 1h gyertyán belül ennél kevesebb mozgás
ACCUM_MIN_OI_INCREASE = 5.0         # magasabb, mint a MIN_OI_INCREASE, mert
                                       # itt nincs ár-/volumen-megerősítés
ACCUM_MIN_CANDLE_VOL_USDT = 20_000
ACCUM_ALERT_COOLDOWN_MINUTES = 180  # 3 óra - napon belüli mozgás lassabb

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

# ----------------------------------------------------------------------------
# KIÉRTÉKELÉS (NAPI ÖSSZESÍTŐ)
# ----------------------------------------------------------------------------
OUTCOME_EVAL_WINDOW_MINUTES = 1440      
OUTCOME_FIXED_SL_PCT = 4.0            
OUTCOME_PROFIT_LEVELS_PCT = [2.0, 5.0, 8.0, 12.0]  
OUTCOME_MAX_STALE_MINUTES = 120        

DAILY_SUMMARY_MIN_DELAY_MINUTES = 35  

class CandleEval(TypedDict):
    price: float
    price_change_pct: float
    vol_multiplier: float
    candle_vol_usdt: float
    direction: str          
    rsi: Optional[float]
    macd_status: Optional[str]
    signal_type: str        
    elapsed_fraction: Optional[float]      
    pace_vol_multiplier: Optional[float]   

class OiBaseline(TypedDict):
    ts: str
    oi: float

def _rotate_signal_log(before_date_str: str) -> None:
    if not SIGNAL_LOG_FILE.exists():
        return
    try:
        keep_lines = []
        archive_by_month: dict[str, list[str]] = {}
        with SIGNAL_LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    rec = json.loads(stripped)
                except json.JSONDecodeError:
                    keep_lines.append(line)  
                    continue
                entry_date = rec.get("entry_date")
                if entry_date and entry_date < before_date_str:
                    month_key = entry_date[:7]  
                    archive_by_month.setdefault(month_key, []).append(line)
                else:
                    keep_lines.append(line)

        if not archive_by_month:
            return  

        for month_key, lines in archive_by_month.items():
            archive_path = SIGNAL_LOG_FILE.parent / f"daytrade_alert_log_{month_key}.jsonl.bak"
            with archive_path.open("a", encoding="utf-8") as f:
                f.writelines(lines)

        tmp_path = SIGNAL_LOG_FILE.with_suffix(SIGNAL_LOG_FILE.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            f.writelines(keep_lines)
        os.replace(tmp_path, SIGNAL_LOG_FILE)
    except OSError as e:
        logger.error("Napló-rotáció sikertelen: %s", e)

def _log_signal_outcome(record: dict) -> None:
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error("Nem sikerült írni a jelzés-naplóba: %s", e)

def register_pending_signal(state: dict, symbol: str, signal_type: str, direction: str, entry_price: float, now: datetime,
                             confluence_earned: int = None, confluence_available: int = None) -> None:
    pending = state.setdefault("pending_outcomes", [])
    pending.append({
        "id": f"{symbol}_{signal_type}_{now.strftime('%Y%m%dT%H%M%S')}",
        "symbol": symbol,
        "signal_type": signal_type,
        "direction": direction,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "entry_date": now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d"), 
        "window_end_ts": (now + timedelta(minutes=OUTCOME_EVAL_WINDOW_MINUTES)).isoformat(),
        "confluence_earned": confluence_earned,
        "confluence_available": confluence_available,
    })

def _simulate_trade_outcome(direction: str, entry_price: float, candles: pd.DataFrame) -> dict:
    levels_reached = {lvl: False for lvl in OUTCOME_PROFIT_LEVELS_PCT}
    max_favorable_pct = 0.0
    sl_hit = False

    if direction == "LONG":
        sl_price = entry_price * (1 - OUTCOME_FIXED_SL_PCT / 100)
    else:  
        sl_price = entry_price * (1 + OUTCOME_FIXED_SL_PCT / 100)

    for _, row in candles.iterrows():
        high = float(row["high"])
        low = float(row["low"])

        if direction == "LONG":
            favorable_extreme = high
            adverse_extreme = low
            favorable_pct = (favorable_extreme - entry_price) / entry_price * 100
        else:  
            favorable_extreme = low
            adverse_extreme = high
            favorable_pct = (entry_price - favorable_extreme) / entry_price * 100

        if favorable_pct > max_favorable_pct:
            max_favorable_pct = favorable_pct
        for lvl in OUTCOME_PROFIT_LEVELS_PCT:
            if max_favorable_pct >= lvl:
                levels_reached[lvl] = True

        hit = (adverse_extreme <= sl_price) if direction == "LONG" else (adverse_extreme >= sl_price)
        if hit:
            sl_hit = True
            break

    return {
        "sl_hit": sl_hit,
        "max_favorable_pct": round(max_favorable_pct, 3),
        "levels_reached": {f"level_{lvl}pct": levels_reached[lvl] for lvl in OUTCOME_PROFIT_LEVELS_PCT},
    }

async def resolve_pending_signals(state: dict, session, klines_semaphore, now: datetime) -> None:
    pending = state.get("pending_outcomes", [])
    if not pending:
        return

    due, still_pending = [], []
    for item in pending:
        try:
            window_end = datetime.fromisoformat(item["window_end_ts"])
        except (KeyError, ValueError):
            continue  
        (due if now >= window_end else still_pending).append(item)

    if not due:
        return

    async def _resolve_one(item):
        entry_dt = datetime.fromisoformat(item["entry_ts"])
        window_end_dt = datetime.fromisoformat(item["window_end_ts"])
        symbol, kdf = await fetch_klines(session, klines_semaphore, item["symbol"], "1h", limit=48)
        if kdf is None or kdf.empty:
            return item, None

        entry_naive = entry_dt.astimezone(timezone.utc).replace(tzinfo=None)
        window_end_naive = window_end_dt.astimezone(timezone.utc).replace(tzinfo=None)
        window_candles = kdf[(kdf["timestamp"] >= entry_naive) & (kdf["timestamp"] <= window_end_naive)]
        if window_candles.empty:
            return item, None

        result = _simulate_trade_outcome(item["direction"], item["entry_price"], window_candles)
        return item, result

    results = await asyncio.gather(*[_resolve_one(item) for item in due], return_exceptions=True)

    for outcome_pair in results:
        if isinstance(outcome_pair, Exception):
            logger.error("Hiba a jelzés kiértékelése közben: %s", outcome_pair)
            continue
        item, result = outcome_pair
        if result is None:
            entry_dt = datetime.fromisoformat(item["entry_ts"])
            age_minutes = (now - entry_dt).total_seconds() / 60
            if age_minutes >= OUTCOME_EVAL_WINDOW_MINUTES + OUTCOME_MAX_STALE_MINUTES:
                _log_signal_outcome({**item, "outcome": "UNKNOWN", "sl_hit": None,
                                      "max_favorable_pct": None, "levels_reached": None,
                                      "resolved_ts": now.isoformat()})
            else:
                still_pending.append(item)  
            continue
        outcome_label = "LOSS" if result["sl_hit"] else "NEUTRAL"
        _log_signal_outcome({**item, **result, "outcome": outcome_label, "resolved_ts": now.isoformat()})

    state["pending_outcomes"] = still_pending

def _load_log_entries_for_date(date_str: str) -> list:
    if not SIGNAL_LOG_FILE.exists():
        return []
    entries = []
    try:
        with SIGNAL_LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue  
                if rec.get("entry_date") == date_str:
                    entries.append(rec)
    except OSError as e:
        logger.error("Nem sikerült beolvasni a jelzés-naplót: %s", e)
    return entries

def _format_daily_summary(date_str: str, entries: list) -> str:
    SIGNAL_TYPE_ORDER = ["STANDARD", "EARLY"]
    SIGNAL_TYPE_LABELS = {"STANDARD": "🦅 DAYTRADE STD", "EARLY": "🌅 DAYTRADE EARLY"}

    lines = [
        f"📊 <b>[DAYTRADE] Összesítő</b> ({date_str})",
        "(SL = -4.0%-os fix stop-loss 24 órán belül; a %-ok",
        "azt mutatják, hány jelzés érte el a profitszintet SL ELŐTT)",
        "━━━━━━━━━━━━━",
    ]

    if not entries:
        lines.append("Nem volt jelzés.")
        return f"\n{chr(10).join(lines)}\n"

    types_present = sorted(
        {r.get("signal_type", "STANDARD") for r in entries},
        key=lambda t: SIGNAL_TYPE_ORDER.index(t) if t in SIGNAL_TYPE_ORDER else 99,
    )

    for sig_type in types_present:
        type_entries = [r for r in entries if r.get("signal_type", "STANDARD") == sig_type]
        total = len(type_entries)
        resolved = [r for r in type_entries if r.get("outcome") != "UNKNOWN" and r.get("sl_hit") is not None]
        n = len(resolved)
        unknown = total - n
        label = SIGNAL_TYPE_LABELS.get(sig_type, sig_type)

        if n == 0:
            lines.append(f"{label}: {total} jelzés (nincs adat)")
            lines.append("━━━━━━━━━━━━━")
            continue

        sl_hits = sum(1 for r in resolved if r.get("sl_hit"))
        sl_pct = sl_hits / n * 100
        lines.append(f"{label}: {total} jelzés{f' ({unknown} n/a)' if unknown else ''}")
        lines.append(f"SL beütve: {sl_hits}/{n} ({sl_pct:.0f}%)")
        for lvl in OUTCOME_PROFIT_LEVELS_PCT:
            key = f"level_{lvl}pct"
            hit = sum(1 for r in resolved if (r.get("levels_reached") or {}).get(key))
            lines.append(f"+{lvl}% elérve SL előtt: {hit}/{n} ({hit / n * 100:.0f}%)")
        lines.append("━━━━━━━━━━━━━")

    return f"\n{chr(10).join(lines)}\n"

STATE_CLEANUP_STALE_DAYS = 14  

def _cleanup_stale_state_entries(state: dict, now: datetime) -> None:
    cutoff = now - timedelta(days=STATE_CLEANUP_STALE_DAYS)
    stale_symbols = []
    for key, entry in state.items():
        if key.startswith("_") or not isinstance(entry, dict):
            continue
        last_seen = entry.get("last_seen")
        if last_seen is None:
            continue  
        try:
            last_seen_dt = datetime.fromisoformat(last_seen)
        except ValueError:
            continue
        if last_seen_dt < cutoff:
            stale_symbols.append(key)

    for key in stale_symbols:
        del state[key]

async def maybe_send_daily_summary(state: dict, now: datetime) -> None:
    local_now = now.astimezone(SUMMARY_TIMEZONE)
    today_str = local_now.strftime("%Y-%m-%d")
    last_summary_date = state.get("_last_summary_date")

    if last_summary_date is None:
        state["_last_summary_date"] = today_str
        return

    if last_summary_date == today_str:
        return  

    local_midnight_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_local_midnight = (local_now - local_midnight_today).total_seconds() / 60
    if minutes_since_local_midnight < DAILY_SUMMARY_MIN_DELAY_MINUTES:
        return  

    yesterday_str = (local_now - timedelta(days=1)).strftime("%Y-%m-%d")
    entries = _load_log_entries_for_date(yesterday_str)
    if entries:
        summary_msg = _format_daily_summary(yesterday_str, entries)
        await send_telegram_message(summary_msg)

    state["_last_summary_date"] = today_str
    _rotate_signal_log(yesterday_str)
    _cleanup_stale_state_entries(state, now)

# ----------------------------------------------------------------------------
# ÁLLAPOT KEZELÉSE
# ----------------------------------------------------------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}

def save_state(state: dict) -> None:
    tmp_path = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, STATE_FILE)
    except OSError:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass

# ----------------------------------------------------------------------------
# API HÍVÁSOK
# ----------------------------------------------------------------------------
async def _get_json(session, url, params=None):
    endpoint_key = url
    cooldown_until = _ENDPOINT_COOLDOWN_UNTIL.get(endpoint_key)
    if cooldown_until is not None:
        now_mono = time.monotonic()
        if now_mono < cooldown_until:
            return None
        del _ENDPOINT_COOLDOWN_UNTIL[endpoint_key]

    last_error = None
    for attempt in range(RETRY_COUNT):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 429:
                    last_error = "HTTP 429 (rate limit)"
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, dict) and data.get("code") not in (None, 0):
                    code = data.get("code")
                    msg = data.get("msg", "")
                    last_error = f"API code={code} msg={msg}"
                    if code == 100410:
                        wait_seconds = ENDPOINT_COOLDOWN_MAX_SECONDS
                        m = re.search(r"after (\d+)", msg)
                        if m:
                            unblock_epoch_ms = int(m.group(1))
                            wait_seconds = max(0.0, unblock_epoch_ms / 1000 - time.time())
                            wait_seconds = min(wait_seconds, ENDPOINT_COOLDOWN_MAX_SECONDS)
                        _ENDPOINT_COOLDOWN_UNTIL[endpoint_key] = time.monotonic() + wait_seconds
                        logger.warning("Endpoint hűtésre kényszerítve %.0f mp-re (code 100410) - %s",
                                        wait_seconds, url)
                        return None
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                return data
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    if last_error is not None:
        logger.warning("Sikertelen API-hívás (%s próbálkozás után) - %s | url=%s params=%s",
                        RETRY_COUNT, last_error, url, params)
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
                "quote_volume_24h": float(t.get("quoteVolume", 0) or 0),
                "last_price": float(t.get("lastPrice", 0) or 0) or None,
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
    async with semaphore:
        data = await _get_json(session, FUNDING_RATE_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        payload = data["data"]
        if isinstance(payload, list):
            payload = payload[0] if payload else None
        if not isinstance(payload, dict):
            return symbol, None
        try:
            raw = payload.get("lastFundingRate")
            if raw is None:
                raw = payload.get("fundingRate")
            if raw is None:
                return symbol, None
            return symbol, float(raw) * 100  
        except (TypeError, ValueError, Exception):
            return symbol, None

SR_LOOKBACK_PERIOD = 60     
SR_PROXIMITY_PCT = 1.0      


async def fetch_cvd_confirmation(session, semaphore, symbol, direction: str):
    """CVD (Cumulative Volume Delta) megerősítés - az alert_checker.py-ban
    lévő azonos nevű függvény ide átemelt, változatlan logikával (lásd ott
    a részletes blokk-kommentet). Visszatér: {"status": "confirm"/"diverge"/
    "neutral", "delta": float|None} vagy None, ha a lekérés meghiúsul.
    A "delta" a lekért trade-ablak időrendi első/második felében mért
    taker-vétel arány különbsége - PLUSZ API-hívás nélkül, ugyanabból az
    egy lekérésből (erősödő/gyengülő nyomás jelzésére, csak a
    megbízhatósági pontszámhoz használjuk)."""
    try:
        async with semaphore:
            async with session.get(TRADES_ENDPOINT, params={"symbol": symbol, "limit": CVD_LOOKBACK_TRADES},
                                    timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                data = await resp.json()
    except Exception as e:
        logger.info("CVD: sikertelen lekérés (%s) - kihagyva. Ok: %s: %s", symbol, type(e).__name__, e)
        return None

    if not data or "data" not in data or not data["data"]:
        return None
    trades = data["data"]
    if isinstance(trades, dict):
        trades = trades.get("trades") or trades.get("list") or []
    if not isinstance(trades, list) or not trades:
        return None

    taker_buy_vol = 0.0
    taker_sell_vol = 0.0
    parsed_trades = []
    try:
        for t in trades:
            qty = t.get("sz")
            if qty is None:
                qty = t.get("qty")
            if qty is None:
                qty = t.get("q")
            if qty is None:
                qty = t.get("volume")
            if qty is None:
                continue
            qty = float(qty)

            ts_raw = t.get("ts") or t.get("time") or t.get("T")
            try:
                ts_val = int(ts_raw) if ts_raw is not None else None
            except (TypeError, ValueError):
                ts_val = None

            side = t.get("side")
            if side is not None:
                side_str = str(side).strip().lower()
                if side_str in ("buy", "bid", "1"):
                    taker_buy_vol += qty
                    parsed_trades.append((ts_val, True, qty))
                elif side_str in ("sell", "ask", "2"):
                    taker_sell_vol += qty
                    parsed_trades.append((ts_val, False, qty))
                continue

            is_buyer_maker = t.get("buyerMaker")
            if is_buyer_maker is None:
                is_buyer_maker = t.get("isBuyerMaker")
            if is_buyer_maker is None:
                is_buyer_maker = t.get("m")
            if is_buyer_maker is None:
                continue
            if is_buyer_maker:
                taker_sell_vol += qty
                parsed_trades.append((ts_val, False, qty))
            else:
                taker_buy_vol += qty
                parsed_trades.append((ts_val, True, qty))
    except (TypeError, ValueError) as e:
        logger.warning("CVD: hiba a trade-ek feldolgozása közben (%s): %s", symbol, e)
        return None

    total_vol = taker_buy_vol + taker_sell_vol
    if total_vol <= 0:
        return None

    buy_ratio = taker_buy_vol / total_vol

    if direction == "LONG":
        if buy_ratio >= CVD_CONFIRM_RATIO:
            status = "confirm"
        elif (1 - buy_ratio) >= CVD_DIVERGENCE_RATIO:
            status = "diverge"
        else:
            status = "neutral"
    else:
        if (1 - buy_ratio) >= CVD_CONFIRM_RATIO:
            status = "confirm"
        elif buy_ratio >= CVD_DIVERGENCE_RATIO:
            status = "diverge"
        else:
            status = "neutral"

    delta = None
    timestamped = [p for p in parsed_trades if p[0] is not None]
    if len(timestamped) >= 20 and len(timestamped) >= 0.8 * len(parsed_trades):
        timestamped.sort(key=lambda p: p[0])
        mid = len(timestamped) // 2
        older, newer = timestamped[:mid], timestamped[mid:]

        def _buy_ratio(chunk):
            buy = sum(q for _, is_buy, q in chunk if is_buy)
            sell = sum(q for _, is_buy, q in chunk if not is_buy)
            tot = buy + sell
            return (buy / tot) if tot > 0 else None

        older_ratio = _buy_ratio(older)
        newer_ratio = _buy_ratio(newer)
        if older_ratio is not None and newer_ratio is not None:
            delta = round(newer_ratio - older_ratio, 3)

    return {"status": status, "delta": delta}


def _find_swing_points(closed: pd.DataFrame, legs: int = SWING_FRACTAL_LEGS) -> list:
    """Fraktál-alapú swing csúcs/mélypont keresés: az i. gyertya akkor
    számít swing csúcsnak, ha a high-ja SZIGORÚAN a legmagasabb a
    [i-legs, i+legs] ablakban (hasonlóan a mélypontra a low-val). Egyedi
    (nem holtversenyes) szélsőértéket keresünk. Visszatér:
    [(index, ár, 'H'|'L'), ...] időrendben."""
    highs = closed["high"].to_numpy()
    lows = closed["low"].to_numpy()
    n = len(highs)
    points = []
    for i in range(legs, n - legs):
        h_window = highs[i - legs:i + legs + 1]
        if highs[i] == h_window.max() and (h_window == highs[i]).sum() == 1:
            points.append((i, float(highs[i]), "H"))
        l_window = lows[i - legs:i + legs + 1]
        if lows[i] == l_window.min() and (l_window == lows[i]).sum() == 1:
            points.append((i, float(lows[i]), "L"))
    points.sort(key=lambda p: p[0])
    return points


def _build_zigzag(swing_points: list) -> list:
    """A nyers swing-pontokból váltakozó (H, L, H, L, ...) zigzag-sorozatot
    épít: ha két egymást követő pont ugyanolyan típusú, csak a
    SZÉLSŐSÉGESEBBET tartjuk meg."""
    zigzag = []
    for idx, price, typ in swing_points:
        if zigzag and zigzag[-1][2] == typ:
            if typ == "H" and price > zigzag[-1][1]:
                zigzag[-1] = (idx, price, typ)
            elif typ == "L" and price < zigzag[-1][1]:
                zigzag[-1] = (idx, price, typ)
        else:
            zigzag.append((idx, price, typ))
    return zigzag


def _classify_structure_trend(zigzag: list) -> Optional[str]:
    """Az utolsó két swing csúcsot és az utolsó két swing mélypontot nézve
    dönti el a trendet."""
    swing_highs = [price for _, price, typ in zigzag if typ == "H"]
    swing_lows = [price for _, price, typ in zigzag if typ == "L"]
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return None  # nincs elég azonosított swing egy megbízható döntéshez

    higher_high = swing_highs[-1] > swing_highs[-2]
    higher_low = swing_lows[-1] > swing_lows[-2]
    lower_high = swing_highs[-1] < swing_highs[-2]
    lower_low = swing_lows[-1] < swing_lows[-2]

    if higher_high and higher_low:
        return "UP"
    if lower_high and lower_low:
        return "DOWN"
    return "NEUTRAL"  # vegyes szerkezet (pl. HH+LL vagy LH+HL) - oldalazás/átmenet


async def fetch_htf_trend(session, semaphore, symbol):
    async with semaphore:
        params = {"symbol": symbol, "interval": HIGHER_TIMEFRAME, "limit": HTF_KLINES_LIMIT}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(KLINES_REQUEST_PACING_SECONDS)
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
        min_candles_needed = SWING_FRACTAL_LEGS * 2 + 1
        trend = None
        if len(closed) >= min_candles_needed:
            swing_points = _find_swing_points(closed, legs=SWING_FRACTAL_LEGS)
            zigzag = _build_zigzag(swing_points)
            trend = _classify_structure_trend(zigzag)

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
        await asyncio.sleep(KLINES_REQUEST_PACING_SECONDS)
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

def _send_telegram_message_sync(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Hiányzik a TELEGRAM_BOT_TOKEN vagy TELEGRAM_CHAT_ID env változó.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code != 200:
            logger.error("Telegram hiba (%s): %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Telegram küldési hiba: %s", e)

async def send_telegram_message(text: str) -> None:
    await asyncio.to_thread(_send_telegram_message_sync, text)

DIRECTION_LABELS = {"LONG": "PUMP", "SHORT": "DUMP"}  

def format_daytrade_message(symbol, direction, price, price_change_pct, candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct, htf_trend=None, bounce_confluence=False, near_level_risk=False, rsi=None, macd_status=None, signal_type="STANDARD", funding_rate=None, pace_vol_multiplier=None, elapsed_fraction=None, cvd_status=None, confluence_line=""):
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "EARLY":
        header = f"🌅 <b>[DAYTRADE] {symbol}</b> {action} (KORAI 1H)"
    else:
        header = f"🦅 <b>[DAYTRADE] {symbol}</b> {action} (STANDARD 1H)"

    # ÚJ: megbízhatósági (confluence) pontszám - EGYETLEN plusz sor a fejléc
    # alatt, lásd compute_confluence_score()/format_confluence_line(). Minden
    # más sor (RSI/MACD/Vol/OI/funding/CVD/HTF/S-R) VÁLTOZATLAN marad.
    confluence_block = f"\n{confluence_line}" if confluence_line else ""

    early_line = ""
    if signal_type == "EARLY":
        pace_note = f", vetített ütem: {pace_vol_multiplier:.1f}x" if pace_vol_multiplier is not None else ""
        elapsed_note = f" (a gyertya ~{elapsed_fraction * 100:.0f}%-ánál)" if elapsed_fraction is not None else ""
        early_line = f"\n🔬 Korai jelzés{pace_note}{elapsed_note}"

    warning_line = ""
    against_trend = ((direction == "LONG" and htf_trend == "DOWN") or (direction == "SHORT" and htf_trend == "UP"))
    if against_trend:
        warning_line = f"\n⚠️ Trenddel szemben (4h: {htf_trend})"

    bounce_line = ""
    if bounce_confluence:
        level_type = "támaszról" if direction == "LONG" else "ellenállásról"
        bounce_line = f"\n🎯 Szint-visszapattanás ({level_type}, 4h-s csatorna)"

    risk_line = ""
    if near_level_risk:
        level_type = "ellenállás" if direction == "LONG" else "támasz"
        risk_line = f"\n⚠️ Közeli {level_type} (4h-s csatorna) - onnan visszapattanhat!"

    indicator_line = ""
    if rsi is not None or macd_status is not None:
        parts = []
        if rsi is not None:
            rsi_note = ""
            if rsi >= RSI_OVERBOUGHT:
                rsi_note = " (túlvett)"
            elif rsi <= RSI_OVERSOLD:
                rsi_note = " (túladott)"
            parts.append(f"RSI: {rsi:.1f}{rsi_note}")
        if macd_status is not None:
            parts.append(f"MACD: {macd_status}")
        indicator_line = f"\n📐 {' | '.join(parts)}"

    funding_line = ""
    if funding_rate is not None:
        funding_line = f"\n💸 Funding: {funding_rate:+.4f}%"
        if direction == "LONG" and funding_rate <= -FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 SHORT SQUEEZE"
        elif direction == "SHORT" and funding_rate >= FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 LONG SQUEEZE"

    # ÚJ: CVD (Cumulative Volume Delta) sor - lásd fetch_cvd_confirmation().
    cvd_line = ""
    if cvd_status == "diverge":
        divergence_note = "eladási" if direction == "LONG" else "vételi"
        cvd_line = f"\n⚠️ CVD divergál (rejtett {divergence_note} nyomás a felszín alatt)"
    elif cvd_status == "confirm":
        cvd_line = "\n✅ CVD megerősíti az irányt"

    body = (
        f"{header}"
        f"{confluence_block}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{early_line}"
        f"{indicator_line}"
        f"{funding_line}"
        f"{cvd_line}"
        f"{warning_line}"
        f"{bounce_line}"
        f"{risk_line}"
    )
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# ÚJ: MEGBÍZHATÓSÁGI (confluence) PONTSZÁM - lásd az alert_checker.py azonos
# nevű függvényének blokk-kommentjét, ide változatlan logikával átemelve.
# ----------------------------------------------------------------------------
def compute_confluence_score(direction, htf_trend=None, near_level_risk=False,
                              has_sr_data=False, cvd_status=None, cvd_delta=None,
                              rsi=None, funding_rate=None, funding_momentum=None):
    factors = []

    if htf_trend is not None:
        against_trend = (
            (direction == "LONG" and htf_trend == "DOWN")
            or (direction == "SHORT" and htf_trend == "UP")
        )
        factors.append(("HTF", not against_trend))

    if has_sr_data:
        factors.append(("S/R", not near_level_risk))

    if cvd_status is not None:
        factors.append(("CVD", cvd_status == "confirm"))

    if cvd_delta is not None:
        if direction == "LONG":
            trend_ok = cvd_delta >= CVD_TREND_DELTA_THRESHOLD
        else:
            trend_ok = cvd_delta <= -CVD_TREND_DELTA_THRESHOLD
        factors.append(("CVDΔ", trend_ok))

    if rsi is not None:
        rsi_ok = (
            (direction == "LONG" and rsi < RSI_OVERBOUGHT)
            or (direction == "SHORT" and rsi > RSI_OVERSOLD)
        )
        factors.append(("RSI", rsi_ok))

    if funding_rate is not None:
        funding_ok = (
            (direction == "LONG" and funding_rate <= FUNDING_SQUEEZE_THRESHOLD_PCT)
            or (direction == "SHORT" and funding_rate >= -FUNDING_SQUEEZE_THRESHOLD_PCT)
        )
        factors.append(("Fund", funding_ok))

    if funding_momentum is not None:
        momentum_ok = (
            (direction == "LONG" and funding_momentum <= -FUNDING_MOMENTUM_THRESHOLD_PCT)
            or (direction == "SHORT" and funding_momentum >= FUNDING_MOMENTUM_THRESHOLD_PCT)
        )
        factors.append(("FundΔ", momentum_ok))

    earned = sum(1 for _, ok in factors if ok)
    available = len(factors)
    return earned, available, factors


def format_confluence_line(earned: int, available: int, factors: list) -> str:
    if available == 0:
        return ""
    ratio = earned / available
    if ratio >= 0.8:
        emoji = "🔥"
    elif ratio >= 0.5:
        emoji = "⚖️"
    else:
        emoji = "⚠️"
    breakdown = " ".join(f"{label}{'✅' if ok else '❌'}" for label, ok in factors)
    return f"{emoji} Megbízhatóság: {earned}/{available} ({breakdown})"


# ----------------------------------------------------------------------------
# ÚJ: FELHALMOZÁS (accumulation-only) FIGYELMEZTETŐ ÜZENET - lásd az
# alert_checker.py azonos nevű függvényének blokk-kommentjét.
# ----------------------------------------------------------------------------
def format_accumulation_message(symbol, oi_value, oi_change_pct, price_change_pct,
                                 rsi=None, macd_status=None, funding_rate=None):
    indicator_line = ""
    if rsi is not None or macd_status is not None:
        parts = []
        if rsi is not None:
            parts.append(f"RSI: {rsi:.1f}")
        if macd_status is not None:
            parts.append(f"MACD: {macd_status}")
        indicator_line = f"\n📐 {' | '.join(parts)}"

    funding_line = f"\n💸 Funding: {funding_rate:+.4f}%" if funding_rate is not None else ""

    body = (
        f"👀 <b>[DAYTRADE] {symbol}</b> FELHALMOZÁS (1H)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%) - az ár szinte "
        f"változatlan ({price_change_pct:+.2f}%)"
        f"{indicator_line}"
        f"{funding_line}\n"
        f"ℹ️ Ez MÉG NEM kereskedési jelzés - valaki pozíciót épít, mielőtt "
        f"az ár elindulna. Ha ár + volumen is beindul, jön a rendes jelzés."
    )
    return f"\n{body}\n"



def find_oi_baseline(history_without_current: list, now: datetime, target_minutes: float = None, min_minutes: float = None, max_minutes: float = None) -> Optional["OiBaseline"]:
    target = OI_TARGET_WINDOW_MINUTES if target_minutes is None else target_minutes
    min_w = OI_MIN_WINDOW_MINUTES if min_minutes is None else min_minutes
    max_w = OI_MAX_WINDOW_MINUTES if max_minutes is None else max_minutes
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if min_w <= age_min <= max_w:
            diff = abs(age_min - target)
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

def evaluate_candle(kdf: pd.DataFrame, now: Optional[datetime] = None) -> Optional["CandleEval"]:
    if kdf is None or len(kdf) < VOLUME_MA_PERIOD + 1:
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
    if avg_vol is None or pd.isna(avg_vol) or avg_vol <= 0:
        return None

    current_price = float(live["close"])      
    price_change_pct = (current_price - prev_close) / prev_close * 100
    vol_multiplier = live["volume"] / avg_vol
    candle_vol_usdt = float(live["volume"] * current_price)
    direction = "LONG" if current_price >= live["open"] else "SHORT"

    rsi_val, macd_status = compute_rsi_macd(kdf["close"])

    elapsed_fraction = None
    pace_vol_multiplier = None
    if now is not None and "timestamp" in kdf.columns:
        try:
            live_open_ts = live["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc)
            now_utc = now.astimezone(timezone.utc)
            elapsed_seconds = (now_utc - live_open_ts).total_seconds()
            if elapsed_seconds >= 60:
                elapsed_fraction = min(1.0, elapsed_seconds / CANDLE_DURATION_SECONDS)
                pace_vol_multiplier = vol_multiplier / elapsed_fraction
        except (TypeError, ValueError, OverflowError):
            pass

    return {
        "price": current_price,
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
        "direction": direction,
        "rsi": rsi_val,
        "macd_status": macd_status,
        "signal_type": "STANDARD",
        "elapsed_fraction": round(elapsed_fraction, 3) if elapsed_fraction is not None else None,
        "pace_vol_multiplier": round(pace_vol_multiplier, 2) if pace_vol_multiplier is not None else None,
    }

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, funding_cache: dict, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + KLINES_MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        klines_semaphore = asyncio.Semaphore(KLINES_MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, htf_cache, funding_cache

        if valid_contracts is None:
            valid_contracts = await fetch_valid_contract_symbols(session)

        await resolve_pending_signals(state, session, klines_semaphore, now)

        candidates = []
        for s, info in tickers.items():
            if not (MIN_VOLUME_USDT <= info["quote_volume_24h"] <= MAX_VOLUME_USDT):
                continue
            if not is_probably_crypto(s):
                continue
            if valid_contracts is not None and s not in valid_contracts:
                continue
            candidates.append(s)

        missing_htf = [s for s in candidates if s not in htf_cache]
        if len(missing_htf) > HTF_FETCH_BATCH_SIZE:
            missing_htf = missing_htf[:HTF_FETCH_BATCH_SIZE]
        
        missing_funding = [s for s in candidates if s not in funding_cache]

        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, klines_semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, klines_semaphore, s) for s in missing_htf]
        funding_tasks = [fetch_funding_rate(session, semaphore, s) for s in missing_funding]

        oi_results, kline_results, htf_results, funding_results = await asyncio.gather(
            asyncio.gather(*oi_tasks, return_exceptions=True),
            asyncio.gather(*kline_tasks, return_exceptions=True),
            asyncio.gather(*htf_tasks, return_exceptions=True),
            asyncio.gather(*funding_tasks, return_exceptions=True),
        )

        if htf_results:
            for item in htf_results:
                if isinstance(item, BaseException):
                    continue
                s, htf_data = item
                if htf_data is not None and htf_data.get("trend") is not None:
                    htf_cache[s] = htf_data

        if funding_results:
            for item in funding_results:
                if isinstance(item, BaseException):
                    continue
                s, fr = item
                if fr is not None:
                    funding_cache[s] = fr

    oi_map = {item[0]: item[1] for item in oi_results if not isinstance(item, BaseException) and item[1] is not None}
    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0

    for symbol in candidates:
        candle = evaluate_candle(klines_map.get(symbol), now=now)
        oi_now = oi_map.get(symbol)
        if candle is None or oi_now is None:
            continue
        evaluated += 1

        entry = state.setdefault(symbol, {"oi_history": [], "last_alert_ts": None})
        entry["oi_history"].append({"ts": now.isoformat(), "oi": oi_now})
        cutoff = now - timedelta(minutes=MAX_HISTORY_AGE_MINUTES)
        entry["oi_history"] = [h for h in entry["oi_history"] if datetime.fromisoformat(h["ts"]) >= cutoff]
        entry["last_seen"] = now.isoformat()

        oi_baseline = find_oi_baseline(entry["oi_history"][:-1], now)
        if oi_baseline is None or oi_baseline["oi"] <= 0:
            continue

        oi_change_pct = (oi_now - oi_baseline["oi"]) / oi_baseline["oi"] * 100
        funding_rate = funding_cache.get(symbol)

        prev_funding_rate = entry.get("prev_funding_rate")
        funding_momentum = None
        if funding_rate is not None and prev_funding_rate is not None:
            funding_momentum = funding_rate - prev_funding_rate
        if funding_rate is not None:
            entry["prev_funding_rate"] = funding_rate

        htf_data = htf_cache.get(symbol, {})
        htf_trend = htf_data.get("trend")
        support = htf_data.get("support")
        resistance = htf_data.get("resistance")

        against_trend = REQUIRE_HTF_ALIGNMENT and ((candle["direction"] == "LONG" and htf_trend == "DOWN") or (candle["direction"] == "SHORT" and htf_trend == "UP"))
        
        price = candle["price"]
        near_support = support is not None and support > 0 and abs(price - support) / support * 100 <= SR_PROXIMITY_PCT
        near_resistance = resistance is not None and resistance > 0 and abs(price - resistance) / resistance * 100 <= SR_PROXIMITY_PCT

        near_level_risk = ((candle["direction"] == "LONG" and near_resistance) or (candle["direction"] == "SHORT" and near_support))
        bounce_confluence = ((candle["direction"] == "LONG" and near_support) or (candle["direction"] == "SHORT" and near_resistance))

        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= MIN_OI_INCREASE
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        is_setup_early = False
        oi_fast_change_pct = None
        if not is_setup:
            elapsed_fraction = candle.get("elapsed_fraction")
            pace_vol_multiplier = candle.get("pace_vol_multiplier")
            if (
                elapsed_fraction is not None
                and EARLY_MIN_ELAPSED_FRACTION <= elapsed_fraction <= EARLY_MAX_ELAPSED_FRACTION
                and pace_vol_multiplier is not None
                and pace_vol_multiplier >= EARLY_MIN_PACE_VOL_MULT
                and candle["candle_vol_usdt"] >= EARLY_MIN_CANDLE_VOL_USDT
                and abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            ):
                oi_fast_baseline = find_oi_baseline(
                    entry["oi_history"][:-1], now,
                    target_minutes=OI_FAST_TARGET_WINDOW_MINUTES,
                    min_minutes=OI_FAST_MIN_WINDOW_MINUTES,
                    max_minutes=OI_FAST_MAX_WINDOW_MINUTES,
                )
                if oi_fast_baseline is not None and oi_fast_baseline["oi"] > 0:
                    oi_fast_change_pct = (oi_now - oi_fast_baseline["oi"]) / oi_fast_baseline["oi"] * 100
                    is_setup_early = oi_fast_change_pct >= EARLY_MIN_OI_FAST_INCREASE

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        fired_signal_type = "STANDARD" if is_setup else ("EARLY" if is_setup_early else None)

        if fired_signal_type and cooldown_ok:
            display_oi_change_pct = oi_fast_change_pct if fired_signal_type == "EARLY" else oi_change_pct

            try:
                cvd_result = await fetch_cvd_confirmation(session, semaphore, symbol, candle["direction"])
            except Exception as e:
                logger.info("CVD: váratlan hiba (%s) - kihagyva. Ok: %s: %s", symbol, type(e).__name__, e)
                cvd_result = None
            cvd_status = cvd_result.get("status") if cvd_result else None
            cvd_delta = cvd_result.get("delta") if cvd_result else None

            conf_earned, conf_available, conf_factors = compute_confluence_score(
                direction=candle["direction"],
                htf_trend=htf_trend,
                near_level_risk=near_level_risk,
                has_sr_data=(support is not None and resistance is not None),
                cvd_status=cvd_status,
                cvd_delta=cvd_delta,
                rsi=candle.get("rsi"),
                funding_rate=funding_rate,
                funding_momentum=funding_momentum,
            )
            confluence_line = format_confluence_line(conf_earned, conf_available, conf_factors)

            msg = format_daytrade_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                oi_now, display_oi_change_pct, htf_trend=htf_trend,
                bounce_confluence=bounce_confluence, near_level_risk=near_level_risk,
                rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                signal_type=fired_signal_type,
                funding_rate=funding_rate,
                pace_vol_multiplier=candle.get("pace_vol_multiplier"),
                elapsed_fraction=candle.get("elapsed_fraction"),
                cvd_status=cvd_status,
                confluence_line=confluence_line,
            )
            await send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            register_pending_signal(
                state, symbol, fired_signal_type, candle["direction"], candle["price"], now,
                confluence_earned=conf_earned, confluence_available=conf_available,
            )

        # ÚJ: FELHALMOZÁS (accumulation-only) figyelmeztető jelzés - lásd az
        # alert_checker.py azonos blokk-kommentjét. Csak ha ebben a körben
        # sem STANDARD, sem EARLY nem tüzelt; saját, külön cooldown-nal.
        if fired_signal_type is None:
            is_accum_setup = (
                abs(candle["price_change_pct"]) <= ACCUM_MAX_PRICE_CHANGE
                and oi_change_pct >= ACCUM_MIN_OI_INCREASE
                and candle["candle_vol_usdt"] >= ACCUM_MIN_CANDLE_VOL_USDT
            )
            if is_accum_setup:
                accum_cooldown_ok = True
                if entry.get("last_accum_alert_ts"):
                    last_accum_dt = datetime.fromisoformat(entry["last_accum_alert_ts"])
                    if (now - last_accum_dt) < timedelta(minutes=ACCUM_ALERT_COOLDOWN_MINUTES):
                        accum_cooldown_ok = False
                if accum_cooldown_ok:
                    accum_msg = format_accumulation_message(
                        symbol, oi_now, oi_change_pct, candle["price_change_pct"],
                        rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                        funding_rate=funding_rate,
                    )
                    await send_telegram_message(accum_msg)
                    entry["last_accum_alert_ts"] = now.isoformat()
                    alerts_sent += 1
                    logger.info("FELHALMOZÁS jelzés küldve (daytrade): %s (OI %+.2f%%, ár %+.2f%%)",
                                symbol, oi_change_pct, candle["price_change_pct"])

    await maybe_send_daily_summary(state, now)
    return alerts_sent, evaluated, valid_contracts, htf_cache, funding_cache

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
            logger.warning("Egy másik futás már aktívnak tűnik (zár kora: %.1f perc) - "
                            "ez a példány csendben kilép.", lock_age_minutes)
            return
        else:
            logger.warning("A talált zár elavultnak (beragadtnak) tűnik - felülírjuk és folytatjuk.")
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
    funding_cache = {}

    while True:
        elapsed_total = time.monotonic() - loop_start
        if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
            break
        pass_start = time.monotonic()
        now = datetime.now(timezone.utc)
        remaining_budget = max(30.0, TOTAL_RUN_BUDGET_SECONDS - elapsed_total)
        try:
            alerts, evaluated, valid_contracts, htf_cache, funding_cache = await asyncio.wait_for(
                run_single_pass(state, valid_contracts, htf_cache, funding_cache, now),
                timeout=remaining_budget,
            )
        except asyncio.TimeoutError:
            logger.warning("Túllépte az időkeretet (%.0f mp), megszakítva.", remaining_budget)
            save_state(state)
            break
        save_state(state)
        logger.info("Kör kész: %d pár kiértékelve, %d riasztás.", evaluated, alerts)
        # JAVÍTÁS: a korábbi "pass_elapsed = time.monotonic() - (time.monotonic() - remaining_budget)"
        # matematikailag mindig kb. remaining_budget-tal volt egyenlő (a két
        # time.monotonic() hívás közti különbség elhanyagolható), ami mivel
        # remaining_budget sosem kisebb 30-nál, azt eredményezte, hogy
        # PASS_INTERVAL_SECONDS - pass_elapsed MINDIG <= 0 volt -> sleep_time
        # MINDIG 0 -> a ciklus SOHA nem várt, szünet nélkül pörgött, ami
        # azonnal rate-limitbe futtatta a botot. A helyes számítás a kör
        # TÉNYLEGES időtartamát nézi (pass_start-tól máig).
        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break
        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    asyncio.run(main())
