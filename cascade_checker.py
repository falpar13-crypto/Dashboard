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
    direction = "LONG" if current_price >= live["open"] else "SHORT"

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
                             pace_vol_multiplier=None, elapsed_fraction=None) -> str:
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "EARLY":
        header = f"🌋 <b>[KASZKÁD] {symbol}</b> {action} (KORAI, 1m)"
    else:
        header = f"🌋 <b>[KASZKÁD] {symbol}</b> {action} (1m)"

    early_line = ""
    if signal_type == "EARLY":
        pace_note = f", vetített ütem: {pace_vol_multiplier:.1f}x" if pace_vol_multiplier is not None else ""
        elapsed_note = f" (a gyertya ~{elapsed_fraction * 100:.0f}%-ánál)" if elapsed_fraction is not None else ""
        early_line = f"\n🔬 Korai jelzés{pace_note}{elapsed_note}"

    body = (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%, 1 PERC alatt)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"⚠️ Extrém, kaszkád-jellegű mozgás - ellenőrizd a piacot, mielőtt lépsz."
        f"{early_line}"
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
            msg = format_cascade_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                signal_type=fired_signal_type,
                pace_vol_multiplier=candle.get("pace_vol_multiplier"),
                elapsed_fraction=candle.get("elapsed_fraction"),
            )
            await send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            _append_signal_log({
                "ts": now.isoformat(), "symbol": symbol, "direction": candle["direction"],
                "signal_type": fired_signal_type, "price": candle["price"],
                "price_change_pct": candle["price_change_pct"], "vol_multiplier": candle["vol_multiplier"],
            })
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
