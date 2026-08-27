"""
BingX Perpetual - Csendes Trend Figyelő (Slow/Steady Climb Detector)
====================================================================
Önálló, negyedik bot. A másik három (daytrade, scalp, kaszkád) mind
TÜSKE-alapú: egyetlen gyertyán belüli hirtelen kiugrást keresnek
(volumen-szorzó, OI-ugrás, ár-mozgás egy rövid ablakban). Ez a bot egy
TELJESEN MÁS mintázatot keres: egy lassú, egyenletes, "csendben felkúszó"
árfolyamot sok óra alatt - amit a tüske-alapú botok SOSEM kapnának el,
mert egyetlen gyertyájuk sem üt be semmilyen küszöböt, csak a KUMULÁLT
hatás jelentős.

MÓDSZERTAN:
Az elmúlt TREND_LOOKBACK_HOURS (alapból 24) LEZÁRT 1h gyertyáján vizsgálja:
  1. Kumulatív ár-mozgás (a lookback-ablak elejétől a végéig) eléri-e a
     küszöböt (MIN_CUMULATIVE_CHANGE_PCT).
  2. "Egyenletesség": a gyertyák nagy hányada (MIN_ALIGNED_FRACTION) megy
     ugyanabba az irányba - nem csak egy-két nagy lökés viszi a mozgást.
  3. "Nem tüske-vezérelt": EGYETLEN gyertya se dominálja a teljes
     mozgást (MAX_SINGLE_CANDLE_SHARE) - ha egy gyertya adná a mozgás
     nagy részét, az valójában egy tüske, amit a másik botok úgyis
     elkapnának, nem csendes felkúszás.

Ez a bot SZÁNDÉKOSAN EGYSZERŰBB architektúrájú, mint a másik három: nincs
belső 30 mp-es pass-ciklus (nem kell "korai" jelzés egy 24 órás
mintázathoz), ezért NEM igényel külső cron-job.org ütemezést sem - a
GitHub Actions saját óránkénti cron-ja tökéletesen elég neki.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("trend_checker")

# ----------------------------------------------------------------------------
# PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1h"
TREND_LOOKBACK_HOURS = 24        # ennyi lezárt 1h gyertyát vizsgálunk vissza
KLINES_LIMIT = TREND_LOOKBACK_HOURS + 5

MIN_CUMULATIVE_CHANGE_PCT = 10.0  # a lookback-ablak eleje-vége közti minimum |mozgás|
MIN_ALIGNED_FRACTION = 0.55       # a gyertyák ennyi hányada menjen a fő irányba
MAX_SINGLE_CANDLE_SHARE = 0.35    # egyetlen gyertya legfeljebb ennyi hányadát adhatja a teljes mozgásnak

# OI-trend: OPCIONÁLIS megerősítés (nem feltétele a tüzelésnek), mert a
# BingX publikus API nem ad historikus OI-t - a history-t magunknak kell
# felépítenünk a state-ben, óránként egy ponttal. Amíg nincs elég
# felhalmozott adat (kb. TREND_LOOKBACK_HOURS óra), ez egyszerűen None
# marad, és a bot ATTÓL FÜGGETLENÜL működik az ár-szerkezet alapján.
OI_HISTORY_TARGET_HOURS = 24
OI_HISTORY_MIN_HOURS = 12
OI_HISTORY_MAX_HOURS = 36
OI_HISTORY_RETENTION_HOURS = 48

TREND_COOLDOWN_HOURS = 24    # lassú, sokáig tartó mintázat - nem kell gyakran újra jelezni

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

BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"

STATE_FILE = Path(__file__).parent / "trend_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "trend_alert_log.jsonl"

MAX_CONCURRENT_REQUESTS = 12
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


# ----------------------------------------------------------------------------
# API HÍVÁSOK (azonos mintázat, mint a másik három botban)
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


async def fetch_klines(session, semaphore, symbol, interval, limit=KLINES_LIMIT):
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


def find_oi_baseline(history_without_current: list, now: datetime,
                       target_hours: float = OI_HISTORY_TARGET_HOURS,
                       min_hours: float = OI_HISTORY_MIN_HOURS,
                       max_hours: float = OI_HISTORY_MAX_HOURS) -> Optional[dict]:
    best, best_diff = None, None
    for h in history_without_current:
        age_h = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 3600
        if min_hours <= age_h <= max_hours:
            diff = abs(age_h - target_hours)
            if best_diff is None or diff < best_diff:
                best, best_diff = h, diff
    return best


# ----------------------------------------------------------------------------
# KIÉRTÉKELÉS
# ----------------------------------------------------------------------------
def evaluate_slow_trend(kdf: pd.DataFrame, lookback: int = TREND_LOOKBACK_HOURS) -> Optional[dict]:
    """LEZÁRT gyertyákon (az élőt kihagyjuk, mert még változhat) vizsgálja,
    van-e 'csendes, egyenletes' trend. Lásd a fájl elején lévő
    blokk-kommentet a módszertanért."""
    if kdf is None or len(kdf) < lookback + 1:
        return None
    closed = kdf.iloc[:-1]
    window = closed.iloc[-lookback:]
    if len(window) < lookback:
        return None

    start_price = float(window.iloc[0]["open"])
    end_price = float(window.iloc[-1]["close"])
    if start_price <= 0:
        return None
    cumulative_change_pct = (end_price - start_price) / start_price * 100
    direction = "LONG" if cumulative_change_pct > 0 else "SHORT"

    candle_changes = (window["close"] - window["open"]) / window["open"] * 100
    if direction == "LONG":
        aligned_fraction = float((candle_changes > 0).mean())
    else:
        aligned_fraction = float((candle_changes < 0).mean())

    abs_changes = candle_changes.abs()
    total_abs_move = float(abs_changes.sum())
    max_single = float(abs_changes.max())
    single_candle_share = (max_single / total_abs_move) if total_abs_move > 0 else 1.0

    return {
        "direction": direction,
        "price": end_price,
        "cumulative_change_pct": round(cumulative_change_pct, 2),
        "aligned_fraction": round(aligned_fraction, 2),
        "single_candle_share": round(single_candle_share, 2),
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


def format_trend_message(symbol, trend, oi_change_pct=None) -> str:
    direction = trend["direction"]
    action = "FELKÚSZÁS 🐢📈" if direction == "LONG" else "LECSÚSZÁS 🐢📉"
    header = f"🌊 <b>[CSENDES TREND] {symbol}</b> {action}"

    oi_line = ""
    if oi_change_pct is not None:
        note = " ✅ egyezik" if (
            (direction == "LONG" and oi_change_pct > 0) or (direction == "SHORT" and oi_change_pct < 0)
        ) else " ⚠️ nem egyezik"
        oi_line = f"\n🧲 OI-változás (~{OI_HISTORY_TARGET_HOURS}h): {oi_change_pct:+.2f}%{note}"

    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {trend['price']:.6f}\n"
        f"📈 Kumulatív mozgás ({TREND_LOOKBACK_HOURS}h): {trend['cumulative_change_pct']:+.2f}%\n"
        f"📐 Egyenletesség: a gyertyák {trend['aligned_fraction']*100:.0f}%-a ment ebbe az irányba\n"
        f"🧩 Legnagyobb egyedi gyertya részesedése: {trend['single_candle_share']*100:.0f}% "
        f"(minél kisebb, annál 'csendesebb' a mozgás)"
        f"{oi_line}\n"
        f"ℹ️ Ez NEM tüske-alapú jelzés - lassú, sok órán át tartó, egyenletes "
        f"elmozdulást jelez, amit a másik botok (tüske-figyelők) nem vesznek észre."
    )
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# FŐ FUTÁS (egyetlen kiértékelési kör - nincs belső pass-ciklus)
# ----------------------------------------------------------------------------
async def run_once(state: dict, now: datetime) -> tuple:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni, futás kihagyva.")
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
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_results, oi_results = await asyncio.gather(
            asyncio.gather(*kline_tasks, return_exceptions=True),
            asyncio.gather(*oi_tasks, return_exceptions=True),
        )

    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}
    oi_map = {item[0]: item[1] for item in oi_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0
    cutoff = now - timedelta(hours=OI_HISTORY_RETENTION_HOURS)

    for symbol in candidates:
        trend = evaluate_slow_trend(klines_map.get(symbol))
        entry = state.setdefault(symbol, {"last_alert_ts": None, "oi_history": []})

        # OI history frissítése FÜGGETLENÜL attól, hogy tüzel-e a jelzés -
        # így a jövőbeli futásoknak lesz mihez visszanézniük.
        oi_now = oi_map.get(symbol)
        if oi_now is not None:
            entry.setdefault("oi_history", [])
            entry["oi_history"].append({"ts": now.isoformat(), "oi": oi_now})
            entry["oi_history"] = [h for h in entry["oi_history"] if datetime.fromisoformat(h["ts"]) >= cutoff]

        if trend is None:
            continue
        evaluated += 1

        is_setup = (
            abs(trend["cumulative_change_pct"]) >= MIN_CUMULATIVE_CHANGE_PCT
            and trend["aligned_fraction"] >= MIN_ALIGNED_FRACTION
            and trend["single_candle_share"] <= MAX_SINGLE_CANDLE_SHARE
        )
        if not is_setup:
            continue

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(hours=TREND_COOLDOWN_HOURS):
                cooldown_ok = False
        if not cooldown_ok:
            continue

        oi_change_pct = None
        if oi_now is not None and entry.get("oi_history"):
            baseline = find_oi_baseline(entry["oi_history"][:-1], now)
            if baseline is not None and baseline["oi"] > 0:
                oi_change_pct = (oi_now - baseline["oi"]) / baseline["oi"] * 100

        msg = format_trend_message(symbol, trend, oi_change_pct=oi_change_pct)
        await send_telegram_message(msg)
        entry["last_alert_ts"] = now.isoformat()
        alerts_sent += 1
        _append_signal_log({
            "ts": now.isoformat(), "symbol": symbol, "direction": trend["direction"],
            "cumulative_change_pct": trend["cumulative_change_pct"],
            "aligned_fraction": trend["aligned_fraction"],
            "single_candle_share": trend["single_candle_share"],
            "oi_change_pct": oi_change_pct,
        })
        logger.info("JELZÉS küldve: %s [%s] (kumulatív %+.2f%%, egyenletesség %.0f%%, max gyertya-részesedés %.0f%%)",
                    symbol, trend["direction"], trend["cumulative_change_pct"],
                    trend["aligned_fraction"] * 100, trend["single_candle_share"] * 100)

    return alerts_sent, evaluated


async def main():
    state = load_state()
    now = datetime.now(timezone.utc)
    try:
        alerts, evaluated = await run_once(state, now)
        logger.info("Futás kész: %d pár kiértékelve, %d riasztás.", evaluated, alerts)
    finally:
        save_state(state)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
                        "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
