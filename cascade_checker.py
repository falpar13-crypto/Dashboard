"""
BingX Perpetual - Likvidáció-kaszkád PROXY figyelő (1m idősík)
====================================================================
Önálló bot, nem épül a daytrade_checker.py / alert_checker.py egyikére
sem, és NEM használ OI/funding/HTF/RSI adatot - kizárólag ÁR és VOLUMEN
alapú, nagyon szigorú, nagyon gyors (1 perces gyertyás) szélsőérték-
figyelő.

MIÉRT KÜLÖN BOT, MIÉRT CSAK ÁR+VOLUMEN:
A cél a likvidáció-kaszkádok (sok kényszer-zárás egymás után rövid idő
alatt) hatásának elkapása lenne, DE a BingX nyilvános websocketén nem
találtunk megbízhatóan dokumentált likvidációs ("forceOrder") csatornát -
sem a hivatalos doksiban, sem független (CCXT) integrációkban. Ahelyett,
hogy egy bizonytalan, esetleg nem is létező csatornára építenénk (ami
csendben soha nem adna adatot), egy MEGBÍZHATÓAN MŰKÖDŐ, már bizonyított
REST-endpointra (kline) építünk: egy likvidáció-kaszkád szinte mindig
egy nagyon rövid idő alatti, extrém ár+volumen tüskét okoz - ezt a
lenyomatot nagyon szigorú küszöbökkel, 1 perces gyertyákon figyeljük.
Ez nem "közvetlen likvidáció-jel", de a piaci HATÁSÁT ugyanúgy elkapja.

Ez a bot szándékosan EGYSZERŰ (nincs OI/funding/HTF/RSI/orderbook-réteg),
mert a cél a sebesség és a megbízhatóság, nem a kontextus gazdagsága - a
másik két bot (daytrade_checker.py, alert_checker.py) már úgyis megadja
a kontextust, ha a symbol egyébként is figyelt.
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
logger = logging.getLogger("cascade_checker")

# ----------------------------------------------------------------------------
# 1) PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1m"

# ÚJ (2026-09-04, felhasználóval egyeztetve): FORDULAT-FOGADÁS
# (mean-reversion / "fade") mód - a bot eddig a hirtelen kiugrás
# IRÁNYÁBA fogadott (folytatódásra), de az audit-adat szerint ez a
# kaszkád-botnál 3 egymást követő napon a leggyengébb teljesítményt
# adta (MFE≈MAE, gyakran a MAE volt nagyobb) - vagyis mire a bot 1 perc
# alatt észreveszi és tüzel egy kirobbanást, a mozgás gyakran már
# kifulladóban van. A bot eredeti célja is a likvidációs kaszkádok
# elkapása ("Liquidation-proxy") - egy likvidációs kaszkád pedig
# KÉNYSZER-mozgás, nem valódi kereslet/kínálat, ezért JELLEMZŐEN gyorsan
# visszapattan, amint a kényszer-kilépések kifogynak. Emiatt most a bot
# az ELLENKEZŐ irányba fogad: hirtelen PUMP -> SHORT (fogadás a
# visszapattanásra lefelé), hirtelen DUMP -> LONG (fogadás a
# visszapattanásra fölfelé). Könnyen visszaállítható False-ra, ha az
# audit-adat nem igazolja vissza.
ENABLE_FADE_MODE = True
CANDLE_DURATION_SECONDS = 60

# SZIGORÚ küszöbök - ez a bot direkt csak a VALÓBAN szélsőséges,
# kaszkád-jellegű mozgásokat akarja elkapni, nem a "normál" pumpokat
# (azokat a másik két bot úgyis figyeli).
MIN_PRICE_CHANGE_PCT = 2.5          # min. |ár-mozgás| a live 1m gyertyában
MIN_VOL_MULTIPLIER = 8.0            # a live gyertya volumene ennyiszerese
                                      # legyen a megelőző gyertyák átlagának
MIN_CANDLE_VOL_USDT = 25_000        # abszolút minimum forgalom, hogy egy
                                      # nagyon illikvid coin zaja ne tüzeljen

VOLUME_MA_PERIOD = 20               # ennyi megelőző LEZÁRT 1m gyertya átlaga

# EARLY (gyorsulás-alapú) - lásd a másik két bot azonos logikáját. Itt
# még szigorúbb, mert 1 perces gyertyán a zaj nagyobb arányban számít.
EARLY_MIN_PACE_VOL_MULT = 15.0
EARLY_MIN_ELAPSED_FRACTION = 0.15   # kb. 9 mp (60s * 0.15)
EARLY_MAX_ELAPSED_FRACTION = 0.6
EARLY_MIN_CANDLE_VOL_USDT = 12_000

# Rövidebb cooldown, mint a másik két botnál, mert ezek gyors, önmagukban
# lezajló események - ha a kaszkád folytatódik, érdemes újra jelezni.
ALERT_COOLDOWN_MINUTES = 15

TOTAL_RUN_BUDGET_SECONDS = 520      # 10 perces külső cron esetén ez hagy
                                      # kb. 80 mp-et checkout/push overhead-re
                                      # (ez a bot nem tölt HTF/funding cache-t,
                                      # tehát a rezsije kisebb, mint a másik
                                      # két botnak - jobban ki lehet tölteni
                                      # a 10 perces ablakot)
PASS_INTERVAL_SECONDS = 15          # rövidebb, mint a másik két botnál
                                      # (10s/15s), mert 1 perces gyertyánál
                                      # a 30s-es pass-intervallum már maga
                                      # is túl lassú lenne

# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK (azonos a másik két bottal, hogy a viselkedés
# konzisztens legyen - lásd daytrade_checker.py / alert_checker.py)
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"

STATE_FILE = Path(__file__).parent / "cascade_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "cascade_alert_log.jsonl"

# ÚJ: ez a bot eddig nem használt helyi időzónát (nem volt napi
# összesítője) - az audit napi riportjához most szükséges.
SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV, SL/TP-MENTES SIGNAL-AUDIT RENDSZER - lásd a
# daytrade_checker.py azonos blokk-kommentjét a teljes indoklásért. Ennél a
# botnál a "score" mindig None lesz (a kaszkád-bot szándékosan nem számol
# meggyőződés-pontszámot - lásd a korábbi tervezési döntést a fájl elején).
# ----------------------------------------------------------------------------
AUDIT_SIGNALS_FILE = Path(__file__).parent / "cascade_audit_signals.jsonl"
AUDIT_RESULTS_FILE = Path(__file__).parent / "cascade_audit_results.jsonl"

AUDIT_WINDOWS_MINUTES = [("5m", 5), ("15m", 15), ("30m", 30), ("1h", 60), ("2h", 120), ("4h", 240)]
AUDIT_MAX_WINDOW_MINUTES = AUDIT_WINDOWS_MINUTES[-1][1]
AUDIT_TIME_TO_MOVE_LEVELS_PCT = [0.5, 1.0, 2.0, 3.0]

AUDIT_VERY_GOOD_MIN_RETURN_PCT = 2.0
AUDIT_VERY_GOOD_MAX_MAE_PCT = 1.0
AUDIT_GOOD_MIN_RETURN_PCT = 0.5
AUDIT_BAD_MAX_RETURN_PCT = -1.0

AUDIT_DAILY_REPORT_HOUR = 23

# ÚJ (hibajavítás - torlódás elkerülése): körönként LEGFELJEBB ennyi
# nyitott jelzést oldunk fel egyszerre - lásd a daytrade_checker.py
# azonos kommentjét a teljes indoklásért.
MAX_AUDIT_RESOLVE_PER_RUN = 30

# ÚJ: SYMBOL-TILTÁS 3 EGYMÁS UTÁNI BAD MINŐSÍTÉS UTÁN - lásd a
# daytrade_checker.py azonos, tesztelt implementációját.
BAN_AFTER_CONSECUTIVE_BAD = 3
BAN_DURATION_HOURS = 24
SYMBOL_OUTCOME_HISTORY_MAX = 10


# ----------------------------------------------------------------------------
# ÚJ: BOT-KÖZI MEGERŐSÍTÉS (cross-bot confirmation) - lásd a
# daytrade_checker.py azonos blokk-kommentjét a teljes indoklásért.
# ----------------------------------------------------------------------------
CROSS_SIGNAL_FILE = Path(__file__).parent / "cascade_recent_signals.jsonl"
OTHER_BOT_SIGNAL_FILES = {
    "SCALP": Path(__file__).parent / "scalp_recent_signals.jsonl",
    "DAYTRADE": Path(__file__).parent / "daytrade_recent_signals.jsonl",
}
CROSS_BOT_WINDOW_MINUTES = 45
CROSS_SIGNAL_RETENTION_HOURS = 6

def _append_cross_bot_signal(symbol: str, direction: str, signal_type: str, now: datetime) -> None:
    """A SAJÁT (bot-specifikus nevű) fájlba ír egy sort - ezt olvassák majd
    a MÁSIK botok a kereszt-megerősítéshez."""
    cutoff = now - timedelta(hours=CROSS_SIGNAL_RETENTION_HOURS)
    rows = []
    if CROSS_SIGNAL_FILE.exists():
        try:
            with CROSS_SIGNAL_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        if datetime.fromisoformat(rec["ts"]) >= cutoff:
                            rows.append(rec)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        except OSError:
            pass
    rows.append({"ts": now.isoformat(), "symbol": symbol, "direction": direction, "signal_type": signal_type})
    try:
        with CROSS_SIGNAL_FILE.open("w", encoding="utf-8") as f:
            for rec in rows:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("Kereszt-bot jelzés-fájl írása sikertelen: %s", e)


def get_cross_bot_confirmations(symbol: str, direction: str, now: datetime) -> list:
    """A MÁSIK botok jelzés-fájljait nézi végig, és visszaadja azok
    listáját, amik az elmúlt CROSS_BOT_WINDOW_MINUTES percben UGYANARRA
    a symbolra, UGYANABBA az irányba jeleztek."""
    cutoff = now - timedelta(minutes=CROSS_BOT_WINDOW_MINUTES)
    confirmations = []
    for label, path in OTHER_BOT_SIGNAL_FILES.items():
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError:
            continue
        best_ts = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("symbol") != symbol or rec.get("direction") != direction:
                    continue
                ts = datetime.fromisoformat(rec["ts"])
                if ts < cutoff:
                    continue
                if best_ts is None or ts > best_ts:
                    best_ts = ts
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        if best_ts is not None:
            age_minutes = (now - best_ts).total_seconds() / 60
            confirmations.append(f"{label} ({age_minutes:.0f} perce)")
    return confirmations


MIN_VOLUME_USDT = 3_000_000  # 1M -> 3M: shitcoin-szűrés, a napi audit-adat alapján
MAX_VOLUME_USDT = 150_000_000

NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

MAX_CONCURRENT_REQUESTS = 16
KLINES_MAX_CONCURRENT_REQUESTS = 6
KLINES_REQUEST_PACING_SECONDS = 0.2

REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

KLINES_LIMIT = VOLUME_MA_PERIOD + 5

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150


class CandleEval(TypedDict):
    price: float
    price_change_pct: float
    vol_multiplier: float
    candle_vol_usdt: float
    direction: str
    signal_type: str
    elapsed_fraction: Optional[float]
    pace_vol_multiplier: Optional[float]


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
            f.write(json.dumps(record) + "\n")
    except OSError:
        pass


# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV SIGNAL-AUDIT MOTOR (lásd a fájl elején a blokk-kommentet és
# a daytrade_checker.py azonos, offline tesztekkel igazolt implementációját)
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
        "signal_id": signal_id,
        "symbol": symbol,
        "direction": direction,
        "signal_type": signal_type,
        "score": score,
        "timeframe": ALERT_TIMEFRAME,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "windows": windows,
        "time_to_move": {str(lvl): None for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT},
    })
    _append_log_to(AUDIT_SIGNALS_FILE, {
        "signal_id": signal_id, "ts": now.isoformat(), "symbol": symbol,
        "direction": direction, "signal_type": signal_type, "score": score,
        "timeframe": ALERT_TIMEFRAME, "entry_price": entry_price,
        "meta": meta or {},
    })
    return signal_id


def _append_log_to(path: Path, record: dict) -> None:
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass


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


async def resolve_signal_audit(state: dict, session, semaphore, now: datetime) -> None:
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
            if direction == "LONG":
                move_pct = (hi - entry_price) / entry_price * 100
            else:
                move_pct = (entry_price - lo) / entry_price * 100
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
                fav = float(window_slice["high"].max())
                adv = float(window_slice["low"].min())
                mfe_pct = (fav - entry_price) / entry_price * 100
                mae_pct = (adv - entry_price) / entry_price * 100
                directional_return = (window_price - entry_price) / entry_price * 100
            else:
                fav = float(window_slice["low"].min())
                adv = float(window_slice["high"].max())
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
                "classification": classification,
                "time_to_move": dict(rec["time_to_move"]),
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


def generate_daily_audit_report(now: datetime):
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
        if not wr:
            continue
        correct = sum(1 for r in wr if r["directional_return_pct"] > 0)
        window_stats[label] = {"n": len(wr), "accuracy_pct": round(correct / len(wr) * 100, 1)}

    final_by_signal = {}
    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
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
    sorted_mfe = sorted(r["mfe_pct"] for r in finals)
    sorted_mae = sorted(r["mae_pct"] for r in finals)
    median_mfe = sorted_mfe[len(sorted_mfe) // 2]
    median_mae = sorted_mae[len(sorted_mae) // 2]

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
    symbol_acc = {
        sym: round(sum(1 for r in rs if r["directional_return_pct"] > 0) / len(rs) * 100, 1)
        for sym, rs in by_symbol.items() if len(rs) >= 2
    }

    hour_buckets = [("00-04", 0, 4), ("04-08", 4, 8), ("08-12", 8, 12), ("12-16", 12, 16), ("16-20", 16, 20), ("20-24", 20, 24)]
    hour_acc = {}
    for label, lo, hi in hour_buckets:
        bucket = []
        for r in finals:
            sig = signals_today.get(r["signal_id"])
            if not sig:
                continue
            h = datetime.fromisoformat(sig["ts"]).astimezone(SUMMARY_TIMEZONE).hour
            if lo <= h < hi:
                bucket.append(r)
        if bucket:
            hour_acc[label] = round(sum(1 for r in bucket if r["directional_return_pct"] > 0) / len(bucket) * 100, 1)

    ttm_medians = {}
    for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
        vals = sorted(r["time_to_move"][str(lvl)] for r in finals if r.get("time_to_move", {}).get(str(lvl)) is not None)
        if vals:
            ttm_medians[lvl] = vals[len(vals) // 2]

    false_signals = [r for r in finals if abs(r["mae_pct"]) > 1.0 and r["mfe_pct"] < 0.3]

    # ÚJ: a kaszkád-botnál nincs meggyőződés-pontszám (score mindig None),
    # ezért a score-sáv szerinti bontás és a jelzéstípus-bontás (itt csak
    # STANDARD/EARLY van) egyszerűbb - kihagyjuk a score-sávot.
    lines = [f"📊 <b>NAPI SIGNAL PERFORMANCE - {today_str}</b> (KASZKÁD 1m)",
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

    if hour_acc:
        lines.append("\n<b>Napszak szerint:</b>")
        for label, _, _ in hour_buckets:
            if label in hour_acc:
                lines.append(f"  {label}: {hour_acc[label]}%")

    if ttm_medians:
        lines.append("\n<b>Medián idő a kedvező mozgás eléréséhez:</b>")
        for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
            if lvl in ttm_medians:
                lines.append(f"  +{lvl}%: {ttm_medians[lvl]:.0f} perc")

    if false_signals:
        lines.append(f"\n⚠️ <b>Gyanús (fals) jelzések:</b> {len(false_signals)} db "
                     f"(nagy ellenirányú mozgás, minimális kedvező mozgás)")

    return "\n".join(lines)


# ----------------------------------------------------------------------------
# ÚJ: KÜSZÖB-HANGOLÁSI JAVASLAT RENDSZER - lásd a daytrade_checker.py
# azonos, tesztelt implementációját a teljes indoklásért. Ennél a botnál
# nincs meggyőződés-pontszám, ezért a mezőlista a nyers ár/volumen-
# mutatókra korlátozódik.
# ----------------------------------------------------------------------------
MIN_SUGGESTION_SAMPLE = 50
MIN_SUGGESTION_GAP_PCT = 0.3

THRESHOLD_SUGGESTION_FIELDS = [
    ("price_change_pct", "numeric", "Ár-elmozdulás mértéke (%)"),
    ("vol_multiplier", "numeric", "Volumen-szorzó"),
]


def _compute_group_stats(finals_with_meta: list, field: str, kind: str) -> Optional[dict]:
    pairs = [(r, m.get(field)) for r, m in finals_with_meta if m.get(field) is not None]
    if len(pairs) < MIN_SUGGESTION_SAMPLE * 2:
        return None

    if kind == "bool":
        group_hi = [r for r, v in pairs if v]
        group_lo = [r for r, v in pairs if not v]
        split_value = None
    else:
        vals = sorted(v for _, v in pairs)
        split_value = vals[len(vals) // 2]
        group_hi = [r for r, v in pairs if v >= split_value]
        group_lo = [r for r, v in pairs if v < split_value]

    if len(group_hi) < MIN_SUGGESTION_SAMPLE or len(group_lo) < MIN_SUGGESTION_SAMPLE:
        return None

    avg_hi = sum(r["directional_return_pct"] for r in group_hi) / len(group_hi)
    avg_lo = sum(r["directional_return_pct"] for r in group_lo) / len(group_lo)
    win_hi = sum(1 for r in group_hi if r["directional_return_pct"] > 0) / len(group_hi) * 100
    win_lo = sum(1 for r in group_lo if r["directional_return_pct"] > 0) / len(group_lo) * 100

    return {
        "field": field, "kind": kind, "split_value": split_value,
        "n_hi": len(group_hi), "n_lo": len(group_lo),
        "avg_return_hi": avg_hi, "avg_return_lo": avg_lo,
        "win_rate_hi": win_hi, "win_rate_lo": win_lo,
        "gap": avg_hi - avg_lo,
    }


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
        if not sig:
            continue
        finals_with_meta.append((r, sig.get("meta", {}) or {}))

    if len(finals_with_meta) < MIN_SUGGESTION_SAMPLE * 2:
        return None

    suggestions = []
    for field, kind, label in THRESHOLD_SUGGESTION_FIELDS:
        stat = _compute_group_stats(finals_with_meta, field, kind)
        if stat is None:
            continue
        if abs(stat["gap"]) < MIN_SUGGESTION_GAP_PCT:
            continue
        if stat["avg_return_hi"] > stat["avg_return_lo"]:
            suggestions.append(
                f"• <b>{label}</b>: a medián ({stat['split_value']:.2f}) FÖLÖTTI jelzések jobban "
                f"teljesítenek ({stat['avg_return_hi']:+.2f}% vs {stat['avg_return_lo']:+.2f}%, "
                f"találati arány {stat['win_rate_hi']:.0f}% vs {stat['win_rate_lo']:.0f}%, "
                f"n={stat['n_hi']}/{stat['n_lo']}) - érdemes lehet a küszöböt a medián közelébe emelni"
            )
        else:
            suggestions.append(
                f"• <b>{label}</b>: a medián ({stat['split_value']:.2f}) ALATTI jelzések jobban "
                f"teljesítenek ({stat['avg_return_lo']:+.2f}% vs {stat['avg_return_hi']:+.2f}%, "
                f"találati arány {stat['win_rate_lo']:.0f}% vs {stat['win_rate_hi']:.0f}%, "
                f"n={stat['n_lo']}/{stat['n_hi']}) - meglepő, ellenőrizd, miért ront a magas érték"
            )

    if not suggestions:
        return None

    lines = [f"🔧 <b>KÜSZÖB-HANGOLÁSI JAVASLATOK</b> (összesen {len(finals_with_meta)} lezárt jelzés alapján)",
             "⚠️ Statisztikai összefüggések, nem garantált okozati kapcsolatok - "
             "mérlegeld, mielőtt bármit módosítasz.\n"]
    lines.extend(suggestions)
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
# API HÍVÁSOK (azonos mintázat, mint a másik két botban)
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
    try:
        return {c["symbol"] for c in data["data"] if c.get("symbol")}
    except (TypeError, KeyError):
        return None


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


# ----------------------------------------------------------------------------
# KIÉRTÉKELÉS
# ----------------------------------------------------------------------------
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
    momentum_direction = "LONG" if current_price >= live["open"] else "SHORT"
    # ÚJ: FADE mód - lásd az ENABLE_FADE_MODE kommentjét. A jelzett irány
    # az ELLENKEZŐJE a nyers ár-mozgásnak (fordulatra fogadunk, nem
    # folytatódásra).
    if ENABLE_FADE_MODE:
        direction = "SHORT" if momentum_direction == "LONG" else "LONG"
    else:
        direction = momentum_direction

    elapsed_fraction = None
    pace_vol_multiplier = None
    if now is not None and "timestamp" in kdf.columns:
        try:
            live_open_ts = live["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc)
            now_utc = now.astimezone(timezone.utc)
            elapsed_seconds = (now_utc - live_open_ts).total_seconds()
            if elapsed_seconds >= 5:
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
        "signal_type": "STANDARD",
        "elapsed_fraction": round(elapsed_fraction, 3) if elapsed_fraction is not None else None,
        "pace_vol_multiplier": round(pace_vol_multiplier, 2) if pace_vol_multiplier is not None else None,
    }


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


DIRECTION_LABELS = {"LONG": "PUMP", "SHORT": "DUMP"}

def format_cascade_message(symbol, direction, price, price_change_pct, candle_vol_usdt,
                             vol_multiplier, signal_type="STANDARD",
                             pace_vol_multiplier=None, elapsed_fraction=None,
                             cross_bot_confirmations=None) -> str:
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "EARLY":
        header = f"🌋 <b>[KASZKÁD] {symbol}</b> {action} (KORAI, 1m)"
    else:
        header = f"🌋 <b>[KASZKÁD] {symbol}</b> {action} (1m)"

    # ÚJ: FADE mód egyértelműsítő sor - lásd az ENABLE_FADE_MODE
    # kommentjét. FONTOS: a fenti PUMP/DUMP címke a JELZÉS IRÁNYÁT (mire
    # fogadunk), NEM a nyers ár-mozgást írja le - FADE módban ez a kettő
    # ELLENTÉTES, ezért itt külön kiírjuk, mi történt ténylegesen.
    fade_line = ""
    if ENABLE_FADE_MODE:
        raw_move = "felfelé pumpolt" if price_change_pct >= 0 else "lefelé dumpolt"
        fade_line = (
            f"\nℹ️ FORDULAT-FOGADÁS: az ár az elmúlt percben {raw_move} "
            f"({price_change_pct:+.2f}%) - a jelzés a VISSZAPATTANÁSRA "
            f"fogad ({action}), nem a folytatódásra."
        )

    early_line = ""
    if signal_type == "EARLY":
        pace_note = f", vetített ütem: {pace_vol_multiplier:.1f}x" if pace_vol_multiplier is not None else ""
        elapsed_note = f" (a gyertya ~{elapsed_fraction * 100:.0f}%-ánál)" if elapsed_fraction is not None else ""
        early_line = f"\n🔬 Korai jelzés{pace_note}{elapsed_note}"

    # ÚJ: bot-közi megerősítés - lásd get_cross_bot_confirmations()
    # kommentjét. Ez a bot nem számol teljes meggyőződés-pontszámot
    # (szándékosan egyszerű), de a megerősítést mégis érdemes kiírni,
    # mert ez a bot pont a leggyorsabb reakciójú - ha a másik két bot is
    # jelzett rá, az önmagában erős plusz infó.
    cross_line = ""
    if cross_bot_confirmations:
        cross_line = f"\n🔥 Megerősítve: {', '.join(cross_bot_confirmations)}"

    body = (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%, 1 PERC alatt)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"⚠️ Extrém, kaszkád-jellegű mozgás - ellenőrizd a piacot, mielőtt lépsz."
        f"{fade_line}"
        f"{early_line}"
        f"{cross_line}"
    )
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR
# ----------------------------------------------------------------------------
async def run_single_pass(state: dict, valid_contracts, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + KLINES_MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        klines_semaphore = asyncio.Semaphore(KLINES_MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni, kör kihagyva.")
            return 0, 0, valid_contracts

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

        kline_tasks = [fetch_klines(session, klines_semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        kline_results = await asyncio.gather(*kline_tasks, return_exceptions=True)

    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0
    pass_diagnostics = []

    for symbol in candidates:
        # ÚJ: symbol-tiltás ellenőrzése MINDENEK ELŐTT.
        if is_symbol_banned(state, symbol, now):
            continue

        candle = evaluate_candle(klines_map.get(symbol), now=now)
        if candle is None:
            continue
        evaluated += 1

        entry = state.setdefault(symbol, {"last_alert_ts": None})

        is_setup = (
            abs(candle["price_change_pct"]) >= MIN_PRICE_CHANGE_PCT
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        is_setup_early = False
        if not is_setup:
            elapsed_fraction = candle.get("elapsed_fraction")
            pace_vol_multiplier = candle.get("pace_vol_multiplier")
            if (
                elapsed_fraction is not None
                and EARLY_MIN_ELAPSED_FRACTION <= elapsed_fraction <= EARLY_MAX_ELAPSED_FRACTION
                and pace_vol_multiplier is not None
                and pace_vol_multiplier >= EARLY_MIN_PACE_VOL_MULT
                and candle["candle_vol_usdt"] >= EARLY_MIN_CANDLE_VOL_USDT
                and abs(candle["price_change_pct"]) >= MIN_PRICE_CHANGE_PCT * 0.6
            ):
                is_setup_early = True

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        fired_signal_type = "STANDARD" if is_setup else ("EARLY" if is_setup_early else None)

        if fired_signal_type is None:
            # Diagnosztika: csak akkor rögzítjük, ha legalább KÖZEL volt a
            # küszöbhöz (különben minden egyes symbol minden körben bekerülne,
            # feleslegesen elárasztva a logot egy ilyen szigorú küszöbű botnál).
            if abs(candle["price_change_pct"]) >= MIN_PRICE_CHANGE_PCT * 0.5:
                pass_diagnostics.append({
                    "symbol": symbol,
                    "price_change_pct": candle["price_change_pct"],
                    "vol_multiplier": candle["vol_multiplier"],
                    "candle_vol_usdt": candle["candle_vol_usdt"],
                })

        if fired_signal_type and cooldown_ok:
            # ÚJ: bot-közi megerősítés ellenőrzése.
            cross_bot_confirmations = get_cross_bot_confirmations(symbol, candle["direction"], now)
            msg = format_cascade_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                signal_type=fired_signal_type,
                pace_vol_multiplier=candle.get("pace_vol_multiplier"),
                elapsed_fraction=candle.get("elapsed_fraction"),
                cross_bot_confirmations=cross_bot_confirmations,
            )
            await send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            _append_signal_log({
                "ts": now.isoformat(), "symbol": symbol, "direction": candle["direction"],
                "signal_type": fired_signal_type, "price": candle["price"],
                "price_change_pct": candle["price_change_pct"], "vol_multiplier": candle["vol_multiplier"],
            })
            # ÚJ: objektív, SL/TP-mentes signal-audit regisztrálása. Ennél a
            # botnál nincs meggyőződés-pontszám, ezért score=None.
            register_signal_audit(state, symbol, candle["direction"], fired_signal_type,
                                    None, candle["price"], now,
                                    meta={
                                        "price_change_pct": abs(candle["price_change_pct"]),
                                        "vol_multiplier": candle["vol_multiplier"],
                                    })
            # ÚJ: a SAJÁT jelzésünket is elmentjük a kereszt-bot fájlba,
            # hogy a MÁSIK két bot lássa a következő futásukban.
            _append_cross_bot_signal(symbol, candle["direction"], fired_signal_type, now)
            logger.info("JELZÉS küldve [%s]: %s [%s] (ár %+.2f%%, vol %.1fx, %.0f USDT)",
                        fired_signal_type, symbol, candle["direction"],
                        candle["price_change_pct"], candle["vol_multiplier"], candle["candle_vol_usdt"])

    if pass_diagnostics:
        pass_diagnostics.sort(key=lambda d: abs(d["price_change_pct"]), reverse=True)
        for d in pass_diagnostics[:3]:
            logger.info("  [közel, de nem tüzelt] %s: ár %+.2f%%, vol %.1fx, %.0f USDT",
                        d["symbol"], d["price_change_pct"], d["vol_multiplier"], d["candle_vol_usdt"])

    return alerts_sent, evaluated, valid_contracts


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
    pass_num = 0
    total_alerts = 0

    # ÚJ: signal-audit feloldás + napi riport - EGYSZER a futás elején,
    # lásd a daytrade_checker.py azonos blokk-kommentjét.
    try:
        audit_connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=audit_connector) as audit_session:
            audit_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            await resolve_signal_audit(state, audit_session, audit_semaphore, datetime.now(timezone.utc))
        await maybe_send_daily_audit_report(state, datetime.now(timezone.utc))
        save_state(state)
    except Exception as e:
        logger.warning("Signal-audit feloldás/riport sikertelen (a fő ciklus ettől függetlenül folytatódik): %s", e)

    while True:
        elapsed_total = time.monotonic() - loop_start
        if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
            break
        pass_num += 1
        pass_start = time.monotonic()
        now = datetime.now(timezone.utc)
        remaining_budget = max(15.0, TOTAL_RUN_BUDGET_SECONDS - elapsed_total)
        try:
            alerts, evaluated, valid_contracts = await asyncio.wait_for(
                run_single_pass(state, valid_contracts, now),
                timeout=remaining_budget,
            )
        except asyncio.TimeoutError:
            logger.warning("[%d. kör] Túllépte az időkeretet (%.0f mp), megszakítva.", pass_num, remaining_budget)
            save_state(state)
            break

        total_alerts += alerts
        save_state(state)
        logger.info("[%d. kör] %d pár kiértékelve, %d riasztás (összesen eddig: %d).",
                    pass_num, evaluated, alerts, total_alerts)

        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break
        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    logger.info("Ciklus vége: %d kör lefutott, összesen %d riasztás.", pass_num, total_alerts)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
                        "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
