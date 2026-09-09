"""
BingX Perpetual - Csendes Felhalmozás (Accumulation) Figyelő (accum_checker.py)
====================================================================
Önálló, NAPOS idősíkon dolgozó bot - teljesen más időskála, mint a többi
(percek/órák helyett napok). A cél: alacsony árú/erősen visszaesett
("nagyon lowon lévő") meme-coinoknál elkapni a csendes felhalmozási
fázist, MIELŐTT a kitörés/pump megtörténne.

--------------------------------------------------------------------
A SETUP: CSENDES FELHALMOZÁS (Wyckoff "accumulation" koncepció)
--------------------------------------------------------------------
Klasszikus, jól dokumentált mintázat: valaki (vagy valakik) lassan épít
pozíciót egy erősen leértékelődött coinban, DE szándékosan nem engedi
elszállni az árat (hogy ne vonjon magára figyelmet, jobb átlagáron
tudjon vásárolni). Ez jellemzően MEGELŐZI egy nagyobb kitörést.

Három, tisztán OHLC+volumen alapú feltétel (nincs RSI/MACD):

  1. "NAGYON LOW": a jelenlegi ár az elmúlt ATH_LOOKBACK_DAYS nap
     csúcsához képest legalább MIN_DROP_FROM_PEAK_PCT %-kal lejjebb van.

  2. VOLUMEN-NÖVEKEDÉS: az utóbbi napok (a PATTERN_WINDOW_DAYS ablak
     második fele) átlag-volumenje legalább VOLUME_GROWTH_MIN_RATIO-
     szorosa az ablak első felének átlagához képest - VALÓDI, tartós
     növekedés, nem egyetlen kiugró nap.

  3. ÁR-SZŰKÖSSÉG: ugyanebben az ablakban az ár (high-low tartomány) az
     átlagárhoz képest MAX_PRICE_RANGE_PCT %-on belül maradt - vagyis
     az ár NEM tört még ki, még csendben van.

  PLUSZ: a mai (élő) nap még nem mutat nagy elmozdulást (MAX_TODAY_
  CHANGE_PCT alatt) - hogy még a kitörés ELŐTT kapjuk el, ne közben.

--------------------------------------------------------------------
FONTOS
--------------------------------------------------------------------
Ez egy MEGFIGYELÉSI jelzés, nem pontos időzítésű belépő - a mintázat
kialakulása után a tényleges kitörés napokkal/hetekkel később is
történhet. Az audit-rendszer ablakai ezért NAPOS skálán mérnek (1d,
2d, 3d, 5d, 7d), nem percben/órában, mint a többi botnál.
"""

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp
import numpy as np
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("accum_checker")

# ----------------------------------------------------------------------------
# PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1d"
HISTORY_CANDLES = 200          # fedi a 180 napos ATH-visszatekintést + puffer

ATH_LOOKBACK_DAYS = 180
MIN_DROP_FROM_PEAK_PCT = 70.0  # a csúcshoz képest legalább ennyivel lejjebb kell lennie

PATTERN_WINDOW_DAYS = 10       # a felhalmozási mintázat vizsgálati ablaka
VOLUME_GROWTH_MIN_RATIO = 1.5  # az ablak 2. fele / 1. fele volumen-arány min.
MAX_PRICE_RANGE_PCT = 18.0     # az ártartomány max. ennyi %-a lehet az átlagárnak
MAX_TODAY_CHANGE_PCT = 10.0    # a mai (élő) nap elmozdulása még ne legyen nagy

ALERT_COOLDOWN_DAYS = 7        # a mintázat maga is 10 napos, nem érdemes gyakrabban újra jelezni

# ÚJ: mivel kifejezetten KIS, alacsony árú meme-coinokat keresünk, a
# szokásos "shitcoin-szűrő" IRÁNYA IS MEGFORDUL - itt nem a nagy, hanem
# a kisebb kapitalizációjú, de még kereskedhető coinokra fókuszálunk.
MIN_VOLUME_USDT = 300_000       # laza minimum - legyen valódi kereskedhetőség
MAX_VOLUME_USDT = 80_000_000    # a nagy, ismert coinokat kizárjuk - nem "low" jellegűek

SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV, SL/TP-MENTES SIGNAL-AUDIT RENDSZER - NAPOS SKÁLÁN
# ----------------------------------------------------------------------------
AUDIT_SIGNALS_FILE = Path(__file__).parent / "accum_audit_signals.jsonl"
AUDIT_RESULTS_FILE = Path(__file__).parent / "accum_audit_results.jsonl"

# ÚJ: itt NEM 5m-4h, hanem NAPOS ablakok - lásd a fájl elején lévő
# blokk-kommentet arról, miért más ennek a botnak az időskálája.
AUDIT_WINDOWS_MINUTES = [("1d", 1440), ("2d", 2880), ("3d", 4320), ("5d", 7200), ("7d", 10080)]
AUDIT_MAX_WINDOW_MINUTES = AUDIT_WINDOWS_MINUTES[-1][1]
AUDIT_TIME_TO_MOVE_LEVELS_PCT = [2.0, 5.0, 10.0, 20.0]  # nagyobb célok, mert napos skála

AUDIT_VERY_GOOD_MIN_RETURN_PCT = 10.0
AUDIT_VERY_GOOD_MAX_MAE_PCT = 5.0
AUDIT_GOOD_MIN_RETURN_PCT = 3.0
AUDIT_BAD_MAX_RETURN_PCT = -5.0

AUDIT_DAILY_REPORT_HOUR = 23

BAN_AFTER_CONSECUTIVE_BAD = 3
BAN_DURATION_HOURS = 24 * 7   # napos bot - hetes tiltás, nem napi
SYMBOL_OUTCOME_HISTORY_MAX = 10

MAX_AUDIT_RESOLVE_PER_RUN = 30

MIN_SUGGESTION_SAMPLE = 20     # kevesebb, mert napos skálán eleve ritkábban tüzel
MIN_SUGGESTION_GAP_PCT = 1.0
THRESHOLD_SUGGESTION_FIELDS = [
    ("drop_from_peak_pct", "numeric", "Visszaesés a csúcstól (%)"),
    ("volume_growth_ratio", "numeric", "Volumen-növekedési arány"),
    ("price_range_pct", "numeric", "Ár-szűkösség (%)"),
]

NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"

STATE_FILE = Path(__file__).parent / "accum_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "accum_alert_log.jsonl"

MAX_CONCURRENT_REQUESTS = 10
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150


# ----------------------------------------------------------------------------
# STATE
# ----------------------------------------------------------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("A state fájl olvasása sikertelen, üres állapotból indulunk.")
    return {}

def save_state(state: dict) -> None:
    tmp_path = STATE_FILE.with_suffix(".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp_path, STATE_FILE)
    except OSError:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass

def _append_signal_log(record: dict) -> None:
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass

def _append_log_to(path: Path, record: dict) -> None:
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ----------------------------------------------------------------------------
# API HÍVÁSOK (azonos mintázat, mint a másik botokban)
# ----------------------------------------------------------------------------
async def _get_json(session, url, params=None):
    endpoint_key = url
    cooldown_until = _ENDPOINT_COOLDOWN_UNTIL.get(endpoint_key)
    if cooldown_until is not None:
        if time.monotonic() < cooldown_until:
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
                        logger.warning("Endpoint hűtésre kényszerítve %.0f mp-re (code 100410)", wait_seconds)
                        return None
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                return data
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    if last_error is not None:
        logger.warning("Sikertelen API-hívás: %s | url=%s params=%s", last_error, url, params)
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
            result[symbol] = {"quote_volume_24h": float(t.get("quoteVolume", 0) or 0)}
        except (TypeError, ValueError):
            continue
    return result


async def fetch_valid_contract_symbols(session):
    data = await _get_json(session, CONTRACTS_ENDPOINT)
    if not data or "data" not in data:
        return None
    try:
        return {c["symbol"] for c in data["data"] if c.get("symbol")}
    except (TypeError, KeyError):
        return None


async def fetch_klines(session, semaphore, symbol, interval, limit=HISTORY_CANDLES):
    async with semaphore:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.05)
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
# A FŐ SETUP: CSENDES FELHALMOZÁS DETEKTÁLÁSA
# ----------------------------------------------------------------------------
def evaluate_accumulation(kdf: pd.DataFrame) -> Optional[dict]:
    """Tisztán OHLC+volumen alapú kiértékelés - lásd a fájl elején lévő
    blokk-kommentet a módszertanért. A LEZÁRT napi gyertyákat vizsgáljuk,
    az élő (ma még formálódó) napot csak a "még nem tört ki" ellenőrzésre
    használjuk."""
    min_needed = ATH_LOOKBACK_DAYS + PATTERN_WINDOW_DAYS + 5
    if kdf is None or len(kdf) < min_needed:
        return None

    closed = kdf.iloc[:-1].reset_index(drop=True)
    live = kdf.iloc[-1]
    if len(closed) < ATH_LOOKBACK_DAYS:
        return None

    current_price = float(closed["close"].iloc[-1])

    # --- 1) "NAGYON LOW": a 180 napos csúcshoz képest ---
    ath_window = closed.iloc[-ATH_LOOKBACK_DAYS:]
    peak = float(ath_window["high"].max())
    if peak <= 0:
        return None
    drop_from_peak_pct = (peak - current_price) / peak * 100
    if drop_from_peak_pct < MIN_DROP_FROM_PEAK_PCT:
        return None

    # --- 2) + 3) Mintázat-ablak: volumen-növekedés + ár-szűkösség ---
    pattern_window = closed.iloc[-PATTERN_WINDOW_DAYS:]
    half = PATTERN_WINDOW_DAYS // 2
    first_half_vol = float(pattern_window["volume"].iloc[:half].mean())
    second_half_vol = float(pattern_window["volume"].iloc[half:].mean())
    if first_half_vol <= 0:
        return None
    volume_growth_ratio = second_half_vol / first_half_vol
    if volume_growth_ratio < VOLUME_GROWTH_MIN_RATIO:
        return None

    window_avg_price = float(pattern_window["close"].mean())
    window_high = float(pattern_window["high"].max())
    window_low = float(pattern_window["low"].min())
    if window_avg_price <= 0:
        return None
    price_range_pct = (window_high - window_low) / window_avg_price * 100
    if price_range_pct > MAX_PRICE_RANGE_PCT:
        return None

    # --- PLUSZ: a mai (élő) nap még ne mutasson nagy kitörést ---
    live_open = float(live["open"])
    live_close = float(live["close"])
    if live_open > 0:
        today_change_pct = abs(live_close - live_open) / live_open * 100
        if today_change_pct > MAX_TODAY_CHANGE_PCT:
            return None

    return {
        "direction": "LONG",  # a setup jellegénél fogva mindig LONG (kitörésre várunk)
        "price": current_price,
        "peak_180d": peak,
        "drop_from_peak_pct": round(drop_from_peak_pct, 2),
        "volume_growth_ratio": round(volume_growth_ratio, 2),
        "price_range_pct": round(price_range_pct, 2),
    }


# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV SIGNAL-AUDIT MOTOR - NAPOS FELBONTÁSSAL
# ----------------------------------------------------------------------------
def register_signal_audit(state: dict, symbol: str, direction: str, signal_type: str,
                            score, entry_price: float, now: datetime,
                            meta: Optional[dict] = None) -> str:
    signal_id = str(uuid.uuid4())
    windows = {
        label: {"target_minutes": minutes, "resolved": False, "future_price": None,
                 "directional_return_pct": None, "mfe_pct": None, "mae_pct": None,
                 "classification": None, "resolved_ts": None}
        for label, minutes in AUDIT_WINDOWS_MINUTES
    }
    audit_pending = state.setdefault("_audit_pending", [])
    audit_pending.append({
        "signal_id": signal_id, "symbol": symbol, "direction": direction,
        "signal_type": signal_type, "score": score, "timeframe": ALERT_TIMEFRAME,
        "entry_price": entry_price, "entry_ts": now.isoformat(), "windows": windows,
        "time_to_move": {str(lvl): None for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT},
    })
    _append_log_to(AUDIT_SIGNALS_FILE, {
        "signal_id": signal_id, "ts": now.isoformat(), "symbol": symbol,
        "direction": direction, "signal_type": signal_type, "score": score,
        "timeframe": ALERT_TIMEFRAME, "entry_price": entry_price, "meta": meta or {},
    })
    return signal_id


async def resolve_signal_audit(state: dict, session, semaphore, now: datetime) -> None:
    """A napos skála miatt a feloldáshoz ÓRÁS (1h) gyertyákat használunk
    (nem 1m/5m-et, mint a többi botnál) - ez ésszerű egyensúly a
    pontosság és a lekérdezett adatmennyiség között egy 7 napos ablaknál."""
    pending = state.get("_audit_pending", [])
    if not pending:
        return

    pending_sorted = sorted(pending, key=lambda r: r.get("entry_ts", ""))
    to_process = pending_sorted[:MAX_AUDIT_RESOLVE_PER_RUN]
    deferred = pending_sorted[MAX_AUDIT_RESOLVE_PER_RUN:]
    if deferred:
        logger.info("Audit feloldás: %d jelzés halasztva a következő körre (körönkénti limit: %d).",
                    len(deferred), MAX_AUDIT_RESOLVE_PER_RUN)

    still_pending = list(deferred)
    for rec in to_process:
        try:
            entry_dt = datetime.fromisoformat(rec["entry_ts"])
        except (KeyError, ValueError):
            continue
        age_minutes = (now - entry_dt).total_seconds() / 60
        limit = min(300, int(age_minutes / 60) + 24)

        _, kdf = await fetch_klines(session, semaphore, rec["symbol"], "1h", limit)
        if kdf is None or len(kdf) == 0:
            still_pending.append(rec)
            continue

        entry_ts_naive = pd.Timestamp(entry_dt.replace(tzinfo=None))
        after = kdf[kdf["timestamp"] >= entry_ts_naive].reset_index(drop=True)
        if after.empty:
            still_pending.append(rec)
            continue

        direction = rec["direction"]
        entry_price = rec["entry_price"]

        for _, row in after.iterrows():
            hi, lo = float(row["high"]), float(row["low"])
            move_pct = (hi - entry_price) / entry_price * 100 if direction == "LONG" else (entry_price - lo) / entry_price * 100
            for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
                key = str(lvl)
                if rec["time_to_move"][key] is None and move_pct >= lvl:
                    row_ts = row["timestamp"]
                    if pd.notna(row_ts):
                        elapsed_min = (row_ts.to_pydatetime().replace(tzinfo=timezone.utc) - entry_dt).total_seconds() / 60
                        rec["time_to_move"][key] = round(max(0.0, elapsed_min), 1)

        any_unresolved = False
        for label, minutes in AUDIT_WINDOWS_MINUTES:
            w = rec["windows"][label]
            if w["resolved"]:
                continue
            if age_minutes < minutes:
                any_unresolved = True
                continue
            target_ts = entry_ts_naive + pd.Timedelta(minutes=minutes)
            window_slice = after[after["timestamp"] <= target_ts]
            if window_slice.empty:
                window_slice = after.iloc[:1]
            at_or_after_target = after[after["timestamp"] >= target_ts]
            window_price = float(at_or_after_target.iloc[0]["close"]) if not at_or_after_target.empty else float(window_slice.iloc[-1]["close"])

            fav = float(window_slice["high"].max()); adv = float(window_slice["low"].min())
            mfe_pct = (fav - entry_price) / entry_price * 100
            mae_pct = (adv - entry_price) / entry_price * 100
            directional_return = (window_price - entry_price) / entry_price * 100

            classification = _classify_audit_result(directional_return, mae_pct)
            w.update({
                "resolved": True, "future_price": window_price,
                "directional_return_pct": round(directional_return, 3),
                "mfe_pct": round(mfe_pct, 3), "mae_pct": round(mae_pct, 3),
                "classification": classification, "resolved_ts": now.isoformat(),
            })
            _append_log_to(AUDIT_RESULTS_FILE, {
                "signal_id": rec["signal_id"], "symbol": rec["symbol"],
                "direction": direction, "signal_type": rec["signal_type"],
                "score": rec["score"], "entry_ts": rec["entry_ts"],
                "window": label, "directional_return_pct": round(directional_return, 3),
                "mfe_pct": round(mfe_pct, 3), "mae_pct": round(mae_pct, 3),
                "classification": classification, "time_to_move": dict(rec["time_to_move"]),
            })

        if any_unresolved and age_minutes <= AUDIT_MAX_WINDOW_MINUTES + 120:
            still_pending.append(rec)
        else:
            final_classification = None
            best_idx = -1
            for w_idx, (w_label, _) in enumerate(AUDIT_WINDOWS_MINUTES):
                w = rec["windows"][w_label]
                if w["resolved"] and w_idx > best_idx:
                    final_classification = w["classification"]
                    best_idx = w_idx
            if final_classification is not None:
                _record_symbol_outcome(state, rec["symbol"], final_classification, now)

    state["_audit_pending"] = still_pending


def _record_symbol_outcome(state: dict, symbol: str, classification: str, now: datetime) -> None:
    history = state.setdefault("_symbol_outcome_history", {})
    sym_hist = history.setdefault(symbol, [])
    sym_hist.append(classification)
    if len(sym_hist) > SYMBOL_OUTCOME_HISTORY_MAX:
        sym_hist[:] = sym_hist[-SYMBOL_OUTCOME_HISTORY_MAX:]
    if len(sym_hist) >= BAN_AFTER_CONSECUTIVE_BAD and all(c == "BAD" for c in sym_hist[-BAN_AFTER_CONSECUTIVE_BAD:]):
        bans = state.setdefault("_symbol_ban_until", {})
        ban_until = now + timedelta(hours=BAN_DURATION_HOURS)
        bans[symbol] = ban_until.isoformat()
        logger.warning("SYMBOL TILTÁS: %s - %d egymás utáni BAD minősítés, tiltva %s-ig",
                        symbol, BAN_AFTER_CONSECUTIVE_BAD, ban_until.isoformat())


def is_symbol_banned(state: dict, symbol: str, now: datetime) -> bool:
    bans = state.get("_symbol_ban_until", {})
    ban_until_str = bans.get(symbol)
    if not ban_until_str:
        return False
    try:
        ban_until = datetime.fromisoformat(ban_until_str)
    except ValueError:
        return False
    if now >= ban_until:
        del bans[symbol]
        return False
    return True


def _classify_audit_result(directional_return_pct: float, mae_pct: float) -> str:
    abs_mae = abs(mae_pct)
    if directional_return_pct >= AUDIT_VERY_GOOD_MIN_RETURN_PCT and abs_mae <= AUDIT_VERY_GOOD_MAX_MAE_PCT:
        return "VERY_GOOD"
    if directional_return_pct >= AUDIT_GOOD_MIN_RETURN_PCT:
        return "GOOD"
    if directional_return_pct <= AUDIT_BAD_MAX_RETURN_PCT:
        return "BAD"
    return "NEUTRAL"


def _load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    records = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return records


def generate_threshold_suggestions() -> Optional[str]:
    signals = _load_jsonl(AUDIT_SIGNALS_FILE)
    results = _load_jsonl(AUDIT_RESULTS_FILE)
    if not signals or not results:
        return None
    signals_by_id = {s["signal_id"]: s for s in signals}
    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
    final_by_signal = {}
    for r in results:
        sid = r["signal_id"]
        if sid not in final_by_signal or window_order.get(r["window"], -1) > window_order.get(final_by_signal[sid]["window"], -1):
            final_by_signal[sid] = r
    finals_with_meta = []
    for sid, r in final_by_signal.items():
        sig = signals_by_id.get(sid)
        if sig:
            finals_with_meta.append((r, sig.get("meta", {}) or {}))
    if len(finals_with_meta) < MIN_SUGGESTION_SAMPLE * 2:
        return None

    suggestions = []
    for field, kind, label in THRESHOLD_SUGGESTION_FIELDS:
        pairs = [(r, m.get(field)) for r, m in finals_with_meta if m.get(field) is not None]
        if len(pairs) < MIN_SUGGESTION_SAMPLE * 2:
            continue
        vals = sorted(v for _, v in pairs)
        split_value = vals[len(vals) // 2]
        group_hi = [r for r, v in pairs if v >= split_value]
        group_lo = [r for r, v in pairs if v < split_value]
        if len(group_hi) < MIN_SUGGESTION_SAMPLE or len(group_lo) < MIN_SUGGESTION_SAMPLE:
            continue
        avg_hi = sum(r["directional_return_pct"] for r in group_hi) / len(group_hi)
        avg_lo = sum(r["directional_return_pct"] for r in group_lo) / len(group_lo)
        if abs(avg_hi - avg_lo) < MIN_SUGGESTION_GAP_PCT:
            continue
        win_hi = sum(1 for r in group_hi if r["directional_return_pct"] > 0) / len(group_hi) * 100
        win_lo = sum(1 for r in group_lo if r["directional_return_pct"] > 0) / len(group_lo) * 100
        if avg_hi > avg_lo:
            suggestions.append(f"• <b>{label}</b>: a medián ({split_value:.2f}) FÖLÖTTI jelzések jobban teljesítenek "
                                f"({avg_hi:+.2f}% vs {avg_lo:+.2f}%, találati arány {win_hi:.0f}% vs {win_lo:.0f}%, n={len(group_hi)}/{len(group_lo)})")
        else:
            suggestions.append(f"• <b>{label}</b>: a medián ({split_value:.2f}) ALATTI jelzések jobban teljesítenek "
                                f"({avg_lo:+.2f}% vs {avg_hi:+.2f}%, találati arány {win_lo:.0f}% vs {win_hi:.0f}%, n={len(group_lo)}/{len(group_hi)})")

    if not suggestions:
        return None
    lines = [f"🔧 <b>KÜSZÖB-HANGOLÁSI JAVASLATOK</b> (összesen {len(finals_with_meta)} lezárt jelzés alapján)",
             "⚠️ Statisztikai összefüggések, nem garantált okozati kapcsolatok.\n"]
    lines.extend(suggestions)
    return "\n".join(lines)


def generate_daily_audit_report(now: datetime) -> Optional[str]:
    today_str = now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d")
    signals = _load_jsonl(AUDIT_SIGNALS_FILE)
    results = _load_jsonl(AUDIT_RESULTS_FILE)
    # ÚJ: napos skálán a "mai" jelzések gyakran nagyon kevesen vannak -
    # ezért itt az UTÓBBI 7 NAP jelzéseit összesítjük, nem csak a mait.
    cutoff = now - timedelta(days=7)
    signals_recent = {
        s["signal_id"]: s for s in signals
        if s.get("ts") and datetime.fromisoformat(s["ts"]) >= cutoff
    }
    if not signals_recent:
        return None
    results_recent = [r for r in results if r.get("signal_id") in signals_recent]
    if not results_recent:
        return None

    window_stats = {}
    for label, _ in AUDIT_WINDOWS_MINUTES:
        wr = [r for r in results_recent if r.get("window") == label]
        if wr:
            correct = sum(1 for r in wr if r["directional_return_pct"] > 0)
            window_stats[label] = {"n": len(wr), "accuracy_pct": round(correct / len(wr) * 100, 1)}

    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
    final_by_signal = {}
    for r in results_recent:
        sid = r["signal_id"]
        if sid not in final_by_signal or window_order.get(r["window"], -1) > window_order.get(final_by_signal[sid]["window"], -1):
            final_by_signal[sid] = r
    finals = list(final_by_signal.values())
    if not finals:
        return None

    total_signals = len(signals_recent)
    avg_mfe = sum(r["mfe_pct"] for r in finals) / len(finals)
    avg_mae = sum(r["mae_pct"] for r in finals) / len(finals)
    sorted_mfe = sorted(r["mfe_pct"] for r in finals); sorted_mae = sorted(r["mae_pct"] for r in finals)
    median_mfe = sorted_mfe[len(sorted_mfe) // 2]; median_mae = sorted_mae[len(sorted_mae) // 2]

    class_counts = {"VERY_GOOD": 0, "GOOD": 0, "NEUTRAL": 0, "BAD": 0}
    for r in finals:
        c = r.get("classification")
        if c in class_counts:
            class_counts[c] += 1

    by_symbol = {}
    for r in finals:
        sym = signals_recent.get(r["signal_id"], {}).get("symbol", "?")
        by_symbol.setdefault(sym, []).append(r)

    ttm_medians = {}
    for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
        vals = sorted(r["time_to_move"][str(lvl)] for r in finals if r.get("time_to_move", {}).get(str(lvl)) is not None)
        if vals:
            ttm_medians[lvl] = vals[len(vals) // 2]

    lines = [f"📊 <b>HETI SIGNAL PERFORMANCE - {today_str}</b> (CSENDES FELHALMOZÁS, napos)",
             f"\nJelzések száma (utóbbi 7 nap): {total_signals}"]
    if window_stats:
        lines.append("\n<b>Irány-pontosság ablakonként:</b>")
        for label, _ in AUDIT_WINDOWS_MINUTES:
            if label in window_stats:
                ws = window_stats[label]
                lines.append(f"  {label}: {ws['accuracy_pct']}% (n={ws['n']})")
    lines.append(f"\n<b>Átlag MFE:</b> {avg_mfe:+.2f}%  <b>Átlag MAE:</b> {avg_mae:+.2f}%")
    lines.append(f"<b>Medián MFE:</b> {median_mfe:+.2f}%  <b>Medián MAE:</b> {median_mae:+.2f}%")
    lines.append(f"\n<b>Minősítés:</b> Very Good: {class_counts['VERY_GOOD']} | Good: {class_counts['GOOD']} | "
                 f"Neutral: {class_counts['NEUTRAL']} | Bad: {class_counts['BAD']}")
    if by_symbol:
        lines.append("\n<b>Coinok:</b>")
        for sym, rs in sorted(by_symbol.items(), key=lambda x: -len(x[1]))[:10]:
            acc = sum(1 for r in rs if r["directional_return_pct"] > 0) / len(rs) * 100
            lines.append(f"  {sym}: {acc:.0f}% (n={len(rs)})")
    if ttm_medians:
        lines.append("\n<b>Medián idő a kedvező mozgás eléréséhez:</b>")
        for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
            if lvl in ttm_medians:
                lines.append(f"  +{lvl}%: {ttm_medians[lvl]:.1f} perc")
    return "\n".join(lines)


async def maybe_send_daily_audit_report(state: dict, now: datetime) -> None:
    local_now = now.astimezone(SUMMARY_TIMEZONE)
    today_str = local_now.strftime("%Y-%m-%d")
    if local_now.hour < AUDIT_DAILY_REPORT_HOUR:
        return
    if state.get("_audit_report_sent_date") == today_str:
        return
    report = generate_daily_audit_report(now)
    if report:
        suggestions = generate_threshold_suggestions()
        if suggestions:
            report = f"{report}\n\n{suggestions}"
        await send_telegram_message(report)
        logger.info("Heti signal-audit riport elküldve.")
    state["_audit_report_sent_date"] = today_str


# ----------------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------------
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


def format_accum_message(symbol: str, result: dict) -> str:
    header = f"🌱 <b>[FELHALMOZÁS] {symbol}</b> CSENDES FELHALMOZÁS 🟩"
    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {result['price']:.8f}\n"
        f"📉 Visszaesés a {ATH_LOOKBACK_DAYS} napos csúcstól: -{result['drop_from_peak_pct']:.1f}% "
        f"(csúcs: {result['peak_180d']:.8f})\n"
        f"📊 Volumen-növekedés az utóbbi {PATTERN_WINDOW_DAYS} napban: {result['volume_growth_ratio']:.2f}x\n"
        f"📏 Ár-szűkösség: {result['price_range_pct']:.1f}% (szűk tartomány)\n"
        f"\n"
        f"ℹ️ MEGFIGYELÉSI jelzés, NEM pontos időzítésű belépő: a mintázat "
        f"(csendes felhalmozás - emelkedő volumen, lapos ár) kialakult, de "
        f"a tényleges kitörés napokkal/hetekkel később is történhet, vagy "
        f"el is maradhat. Csak napos idősíkon értelmezhető, tisztán "
        f"OHLC-geometria, indikátor nélkül. Ellenőrizd a chartot, mielőtt "
        f"döntesz - ez nem automatikus vétel/eladás jelzés."
    )
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# FŐ FUTÁS
# ----------------------------------------------------------------------------
async def run_once(state: dict, now: datetime) -> tuple:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni.")
            return 0, 0
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

        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        kline_results = await asyncio.gather(*kline_tasks, return_exceptions=True)

    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0

    for symbol in candidates:
        if is_symbol_banned(state, symbol, now):
            continue
        kdf = klines_map.get(symbol)
        if kdf is None:
            continue
        evaluated += 1

        result = evaluate_accumulation(kdf)
        if result is None:
            continue

        entry = state.setdefault(symbol, {"last_alert_ts": None})
        if entry.get("last_alert_ts"):
            last_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_dt) < timedelta(days=ALERT_COOLDOWN_DAYS):
                continue

        msg = format_accum_message(symbol, result)
        await send_telegram_message(msg)
        entry["last_alert_ts"] = now.isoformat()
        alerts_sent += 1

        register_signal_audit(state, symbol, "LONG", "ACCUMULATION", None, result["price"], now,
                                meta={
                                    "drop_from_peak_pct": result["drop_from_peak_pct"],
                                    "volume_growth_ratio": result["volume_growth_ratio"],
                                    "price_range_pct": result["price_range_pct"],
                                })
        _append_signal_log({
            "ts": now.isoformat(), "symbol": symbol, "direction": "LONG",
            "price": result["price"], "drop_from_peak_pct": result["drop_from_peak_pct"],
            "volume_growth_ratio": result["volume_growth_ratio"], "price_range_pct": result["price_range_pct"],
        })
        logger.info("JELZÉS küldve: %s [FELHALMOZÁS] ár=%.8f visszaesés=%.1f%% vol-növekedés=%.2fx",
                    symbol, result["price"], result["drop_from_peak_pct"], result["volume_growth_ratio"])

    return alerts_sent, evaluated


async def main():
    state = load_state()
    now = datetime.now(timezone.utc)
    try:
        try:
            audit_connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
            async with aiohttp.ClientSession(connector=audit_connector) as audit_session:
                audit_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
                await resolve_signal_audit(state, audit_session, audit_semaphore, now)
            await maybe_send_daily_audit_report(state, now)
        except Exception as e:
            logger.warning("Signal-audit feloldás/riport sikertelen: %s", e)

        alerts, evaluated = await run_once(state, now)
        logger.info("Futás kész: %d pár kiértékelve, %d riasztás.", evaluated, alerts)
    finally:
        save_state(state)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva.")
    asyncio.run(main())
