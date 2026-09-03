"""
BingX Perpetual - Likviditás-kiszúrás (Liquidity Sweep) Figyelő (sweep_checker.py)
====================================================================
Önálló, tisztán PRICE ACTION alapú bot - SEMMILYEN indikátort (RSI, MACD,
volumen-szorzó, OI stb.) nem használ a jelzéshez. Csak nyers OHLC-
geometria: swing-pontok + gyertya-alak.

--------------------------------------------------------------------
A SETUP: LIKVIDITÁS-KISZÚRÁS (ICT/SMC "stop hunt" / "liquidity grab")
--------------------------------------------------------------------
Egy jól ismert, sokat dokumentált price action mintázat:

  1. Van egy nemrég kialakult, jól látható swing-mélypont (LONG esetén)
     vagy swing-csúcs (SHORT esetén) - ez az a szint, ahol a piaci
     szereplők stop-megbízásai koncentrálódnak.
  2. Az ár RÖVIDEN ÁTLÉPI ezt a szintet (a kanóccal "kiszúrja" az ott lévő
     likviditást/stop-okat), DE...
  3. ...a gyertya UGYANAZON a gyertyán VISSZAZÁR a szint MÖGÉ (nem marad
     kint) - ez maga a kiszúrás visszautasítása.
  4. A visszautasító kanócnak ARÁNYAIBAN NAGYNAK kell lennie a gyertya
     testéhez képest - minél nagyobb a kanóc a testhez képest, annál
     erősebb az elutasítás.

Semmi RSI, MACD, volumen-növekedés, OI - csak ez a négy, tisztán
geometriai feltétel. A cél: megnézni, hogy egy indikátor-mentes,
"tiszta" price action megközelítés hogyan teljesít az audit-rendszerben
a többi, indikátor-alapú bothoz képest.
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
logger = logging.getLogger("sweep_checker")

# ----------------------------------------------------------------------------
# PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "15m"     # köztes idősík - se nem az 1m (kaszkád), se az 1h (daytrade)
HISTORY_CANDLES = 150

SWING_FRACTAL_LEGS = 3       # ennyi gyertyát nézünk mindkét oldalon egy swing-ponthoz

# A kiszúrásnak legalább ennyi %-kal kell átlépnie a szintet - különben
# csak zaj/kerekítési pontatlanság, nem valódi "likviditás-kiszúrás".
MIN_PIERCE_PCT = 0.1

# A visszautasító kanócnak legalább ennyiszer akkorának kell lennie, mint
# a gyertya teste - minél nagyobb, annál egyértelműbb az elutasítás.
MIN_REJECTION_RATIO = 1.2

# Likviditási alapszint (NEM növekedési küszöb, csak minimális
# részvételi szint, hogy ne egy szinte forgalom nélküli gyertyára tüzeljünk).
MIN_CANDLE_VOL_USDT = 15_000

ALERT_COOLDOWN_HOURS = 4
MAX_ALERTS_PER_RUN = 6

MIN_VOLUME_USDT = 5_000_000
MAX_VOLUME_USDT = 20_000_000_000   # bőven fedi a BTC-t/ETH-t is

SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV, SL/TP-MENTES SIGNAL-AUDIT RENDSZER - lásd a
# daytrade_checker.py azonos, tesztelt implementációját a teljes indoklásért.
# ----------------------------------------------------------------------------
AUDIT_SIGNALS_FILE = Path(__file__).parent / "sweep_audit_signals.jsonl"
AUDIT_RESULTS_FILE = Path(__file__).parent / "sweep_audit_results.jsonl"

AUDIT_WINDOWS_MINUTES = [("5m", 5), ("15m", 15), ("30m", 30), ("1h", 60), ("2h", 120), ("4h", 240)]
AUDIT_MAX_WINDOW_MINUTES = AUDIT_WINDOWS_MINUTES[-1][1]
AUDIT_TIME_TO_MOVE_LEVELS_PCT = [0.5, 1.0, 2.0, 3.0]

AUDIT_VERY_GOOD_MIN_RETURN_PCT = 2.0
AUDIT_VERY_GOOD_MAX_MAE_PCT = 1.0
AUDIT_GOOD_MIN_RETURN_PCT = 0.5
AUDIT_BAD_MAX_RETURN_PCT = -1.0

AUDIT_DAILY_REPORT_HOUR = 23

# ÚJ: SYMBOL-TILTÁS 3 EGYMÁS UTÁNI BAD MINŐSÍTÉS UTÁN.
BAN_AFTER_CONSECUTIVE_BAD = 3
BAN_DURATION_HOURS = 24
SYMBOL_OUTCOME_HISTORY_MAX = 10

# ÚJ: KÜSZÖB-HANGOLÁSI JAVASLAT RENDSZER
MIN_SUGGESTION_SAMPLE = 50
MIN_SUGGESTION_GAP_PCT = 0.3
THRESHOLD_SUGGESTION_FIELDS = [
    ("pierce_pct", "numeric", "Kiszúrás mértéke (%)"),
    ("rejection_ratio", "numeric", "Elutasító kanóc aránya"),
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

STATE_FILE = Path(__file__).parent / "sweep_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "sweep_alert_log.jsonl"

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
# SWING-KERESÉS (ugyanaz a fraktál-módszer, mint a másik botokban)
# ----------------------------------------------------------------------------
def find_swing_points(df: pd.DataFrame, legs: int = SWING_FRACTAL_LEGS) -> list:
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    n = len(highs)
    points = []
    for i in range(legs, n - legs):
        hw = highs[i - legs:i + legs + 1]
        if highs[i] == hw.max() and (hw == highs[i]).sum() == 1:
            points.append((i, float(highs[i]), "H"))
        lw = lows[i - legs:i + legs + 1]
        if lows[i] == lw.min() and (lw == lows[i]).sum() == 1:
            points.append((i, float(lows[i]), "L"))
    points.sort(key=lambda p: p[0])
    return points


# ----------------------------------------------------------------------------
# A FŐ SETUP: LIKVIDITÁS-KISZÚRÁS DETEKTÁLÁSA
# ----------------------------------------------------------------------------
def evaluate_liquidity_sweep(kdf: pd.DataFrame) -> Optional[dict]:
    """Tisztán OHLC-geometria alapú kiértékelés - lásd a fájl elején lévő
    blokk-kommentet a módszertanért. A LEZÁRT gyertyákból építjük fel a
    swing-pontokat, majd az ÉLŐ (jelenleg formálódó) gyertyát vizsgáljuk:
    kiszúrta-e valamelyik legutóbbi swing-szintet, és visszazárt-e mögé,
    erős elutasító kanóccal."""
    if kdf is None or len(kdf) < SWING_FRACTAL_LEGS * 4 + 10:
        return None

    closed = kdf.iloc[:-1].reset_index(drop=True)
    live = kdf.iloc[-1]
    if len(closed) < SWING_FRACTAL_LEGS * 2 + 1:
        return None

    swing_points = find_swing_points(closed, legs=SWING_FRACTAL_LEGS)
    highs = [p for _, p, t in swing_points if t == "H"]
    lows = [p for _, p, t in swing_points if t == "L"]

    live_open = float(live["open"])
    live_high = float(live["high"])
    live_low = float(live["low"])
    live_close = float(live["close"])
    live_vol_usdt = float(live["volume"]) * live_close
    body = abs(live_close - live_open)
    body_safe = max(body, live_close * 0.0005)

    if live_vol_usdt < MIN_CANDLE_VOL_USDT:
        return None

    # --- LONG kiszúrás: a legutóbbi swing-mélypont alá megy, majd visszazár fölé ---
    if lows:
        swing_low = lows[-1]
        pierce_pct = (swing_low - live_low) / swing_low * 100 if swing_low > 0 else 0
        if live_low < swing_low and pierce_pct >= MIN_PIERCE_PCT and live_close > swing_low:
            lower_wick = min(live_open, live_close) - live_low
            rejection_ratio = max(0.0, lower_wick) / body_safe
            if rejection_ratio >= MIN_REJECTION_RATIO:
                return {
                    "direction": "LONG", "price": live_close,
                    "swept_level": swing_low, "pierce_pct": round(pierce_pct, 3),
                    "rejection_ratio": round(rejection_ratio, 2),
                }

    # --- SHORT kiszúrás: a legutóbbi swing-csúcs fölé megy, majd visszazár alá ---
    if highs:
        swing_high = highs[-1]
        pierce_pct = (live_high - swing_high) / swing_high * 100 if swing_high > 0 else 0
        if live_high > swing_high and pierce_pct >= MIN_PIERCE_PCT and live_close < swing_high:
            upper_wick = live_high - max(live_open, live_close)
            rejection_ratio = max(0.0, upper_wick) / body_safe
            if rejection_ratio >= MIN_REJECTION_RATIO:
                return {
                    "direction": "SHORT", "price": live_close,
                    "swept_level": swing_high, "pierce_pct": round(pierce_pct, 3),
                    "rejection_ratio": round(rejection_ratio, 2),
                }

    return None


# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV SIGNAL-AUDIT MOTOR (lásd a daytrade_checker.py implementációját)
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
    pending = state.get("_audit_pending", [])
    if not pending:
        return
    still_pending = []
    for rec in pending:
        try:
            entry_dt = datetime.fromisoformat(rec["entry_ts"])
        except (KeyError, ValueError):
            continue
        age_minutes = (now - entry_dt).total_seconds() / 60
        if age_minutes <= 60:
            interval, limit = "1m", min(300, int(age_minutes) + 10)
        else:
            interval, limit = "5m", min(300, int(age_minutes / 5) + 10)

        _, kdf = await fetch_klines(session, semaphore, rec["symbol"], interval, limit)
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

            if direction == "LONG":
                fav = float(window_slice["high"].max()); adv = float(window_slice["low"].min())
                mfe_pct = (fav - entry_price) / entry_price * 100
                mae_pct = (adv - entry_price) / entry_price * 100
                directional_return = (window_price - entry_price) / entry_price * 100
            else:
                fav = float(window_slice["low"].min()); adv = float(window_slice["high"].max())
                mfe_pct = (entry_price - fav) / entry_price * 100
                mae_pct = (entry_price - adv) / entry_price * 100
                directional_return = (entry_price - window_price) / entry_price * 100

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

        if any_unresolved and age_minutes <= AUDIT_MAX_WINDOW_MINUTES + 30:
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
    signals_today = {
        s["signal_id"]: s for s in signals
        if s.get("ts") and datetime.fromisoformat(s["ts"]).astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d") == today_str
    }
    if not signals_today:
        return None
    results_today = [r for r in results if r.get("signal_id") in signals_today]
    if not results_today:
        return None

    window_stats = {}
    for label, _ in AUDIT_WINDOWS_MINUTES:
        wr = [r for r in results_today if r.get("window") == label]
        if wr:
            correct = sum(1 for r in wr if r["directional_return_pct"] > 0)
            window_stats[label] = {"n": len(wr), "accuracy_pct": round(correct / len(wr) * 100, 1)}

    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
    final_by_signal = {}
    for r in results_today:
        sid = r["signal_id"]
        if sid not in final_by_signal or window_order.get(r["window"], -1) > window_order.get(final_by_signal[sid]["window"], -1):
            final_by_signal[sid] = r
    finals = list(final_by_signal.values())
    if not finals:
        return None

    total_signals = len(signals_today)
    avg_mfe = sum(r["mfe_pct"] for r in finals) / len(finals)
    avg_mae = sum(r["mae_pct"] for r in finals) / len(finals)
    sorted_mfe = sorted(r["mfe_pct"] for r in finals); sorted_mae = sorted(r["mae_pct"] for r in finals)
    median_mfe = sorted_mfe[len(sorted_mfe) // 2]; median_mae = sorted_mae[len(sorted_mae) // 2]

    class_counts = {"VERY_GOOD": 0, "GOOD": 0, "NEUTRAL": 0, "BAD": 0}
    for r in finals:
        c = r.get("classification")
        if c in class_counts:
            class_counts[c] += 1

    long_results = [r for r in finals if signals_today.get(r["signal_id"], {}).get("direction") == "LONG"]
    short_results = [r for r in finals if signals_today.get(r["signal_id"], {}).get("direction") == "SHORT"]
    long_acc = round(sum(1 for r in long_results if r["directional_return_pct"] > 0) / len(long_results) * 100, 1) if long_results else None
    short_acc = round(sum(1 for r in short_results if r["directional_return_pct"] > 0) / len(short_results) * 100, 1) if short_results else None

    by_symbol = {}
    for r in finals:
        sym = signals_today.get(r["signal_id"], {}).get("symbol", "?")
        by_symbol.setdefault(sym, []).append(r)
    symbol_acc = {sym: round(sum(1 for r in rs if r["directional_return_pct"] > 0) / len(rs) * 100, 1)
                  for sym, rs in by_symbol.items() if len(rs) >= 2}

    ttm_medians = {}
    for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
        vals = sorted(r["time_to_move"][str(lvl)] for r in finals if r.get("time_to_move", {}).get(str(lvl)) is not None)
        if vals:
            ttm_medians[lvl] = vals[len(vals) // 2]

    false_signals = [r for r in finals if abs(r["mae_pct"]) > 1.0 and r["mfe_pct"] < 0.3]

    lines = [f"📊 <b>NAPI SIGNAL PERFORMANCE - {today_str}</b> (LIKVIDITÁS-KISZÚRÁS 15m)",
             f"\nJelzések száma: {total_signals}"]
    if window_stats:
        lines.append("\n<b>Irány-pontosság időablakonként:</b>")
        for label, _ in AUDIT_WINDOWS_MINUTES:
            if label in window_stats:
                ws = window_stats[label]
                lines.append(f"  {label}: {ws['accuracy_pct']}% (n={ws['n']})")
    if long_acc is not None or short_acc is not None:
        lines.append("\n<b>Irány szerint:</b>")
        if long_acc is not None:
            lines.append(f"  LONG: {long_acc}% (n={len(long_results)})")
        if short_acc is not None:
            lines.append(f"  SHORT: {short_acc}% (n={len(short_results)})")
    lines.append(f"\n<b>Átlag MFE:</b> {avg_mfe:+.2f}%  <b>Átlag MAE:</b> {avg_mae:+.2f}%")
    lines.append(f"<b>Medián MFE:</b> {median_mfe:+.2f}%  <b>Medián MAE:</b> {median_mae:+.2f}%")
    lines.append(f"\n<b>Minősítés:</b> Very Good: {class_counts['VERY_GOOD']} | Good: {class_counts['GOOD']} | "
                 f"Neutral: {class_counts['NEUTRAL']} | Bad: {class_counts['BAD']}")
    if symbol_acc:
        lines.append("\n<b>Coin szerint (min. 2 jelzés):</b>")
        for sym, acc in sorted(symbol_acc.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  {sym}: {acc}%")
    if ttm_medians:
        lines.append("\n<b>Medián idő a kedvező mozgás eléréséhez:</b>")
        for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
            if lvl in ttm_medians:
                lines.append(f"  +{lvl}%: {ttm_medians[lvl]:.0f} perc")
    if false_signals:
        lines.append(f"\n⚠️ <b>Gyanús (fals) jelzések:</b> {len(false_signals)} db")
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
        logger.info("Napi signal-audit riport elküldve.")
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


def format_sweep_message(symbol: str, result: dict) -> str:
    direction = result["direction"]
    action = "LIKVIDITÁS-KISZÚRÁS (mélypont) 🟩⬆️" if direction == "LONG" else "LIKVIDITÁS-KISZÚRÁS (csúcs) 🟥⬇️"
    header = f"🎣 <b>[SWEEP] {symbol}</b> {action}"

    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {result['price']:.6f}\n"
        f"🎯 Kiszúrt szint: {result['swept_level']:.6f}\n"
        f"📏 Kiszúrás mértéke: {result['pierce_pct']:.3f}%\n"
        f"🕯️ Elutasító kanóc aránya: {result['rejection_ratio']:.2f}x (test)\n"
        f"\n"
        f"ℹ️ TISZTÁN PRICE ACTION jelzés - nincs indikátor (RSI/MACD/volumen-"
        f"küszöb), csak a nyers gyertya-szerkezet: az ár átlépte a szintet, "
        f"majd ugyanazon a gyertyán visszazárt mögé, erős elutasító kanóccal. "
        f"Ellenőrizd a chartot, mielőtt döntesz - ez nem automatikus "
        f"vétel/eladás jelzés."
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
    candidates_for_alert = []

    for symbol in candidates:
        if is_symbol_banned(state, symbol, now):
            continue
        kdf = klines_map.get(symbol)
        if kdf is None:
            continue
        evaluated += 1

        result = evaluate_liquidity_sweep(kdf)
        if result is None:
            continue

        entry = state.setdefault(symbol, {"last_alert_ts": None})
        if entry.get("last_alert_ts"):
            last_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_dt) < timedelta(hours=ALERT_COOLDOWN_HOURS):
                continue

        quality_score = tickers.get(symbol, {}).get("quote_volume_24h", 0.0)
        candidates_for_alert.append({"symbol": symbol, "result": result, "entry": entry, "quality_score": quality_score})

    candidates_for_alert.sort(key=lambda c: c["quality_score"], reverse=True)
    to_send = candidates_for_alert[:MAX_ALERTS_PER_RUN]
    if len(candidates_for_alert) > len(to_send):
        logger.info("Rate-limit: %d találat elnyomva ebben a körben.", len(candidates_for_alert) - len(to_send))

    for c in to_send:
        symbol, result, entry = c["symbol"], c["result"], c["entry"]
        msg = format_sweep_message(symbol, result)
        await send_telegram_message(msg)
        entry["last_alert_ts"] = now.isoformat()
        alerts_sent += 1
        register_signal_audit(state, symbol, result["direction"], "LIQUIDITY_SWEEP",
                                None, result["price"], now,
                                meta={
                                    "pierce_pct": result["pierce_pct"],
                                    "rejection_ratio": result["rejection_ratio"],
                                })
        _append_signal_log({
            "ts": now.isoformat(), "symbol": symbol, "direction": result["direction"],
            "price": result["price"], "swept_level": result["swept_level"],
            "pierce_pct": result["pierce_pct"], "rejection_ratio": result["rejection_ratio"],
        })
        logger.info("JELZÉS küldve: %s [%s] szint=%.6f kiszúrás=%.3f%% elutasítás=%.2fx",
                    symbol, result["direction"], result["swept_level"], result["pierce_pct"], result["rejection_ratio"])

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
