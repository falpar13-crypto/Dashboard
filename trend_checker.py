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
     küszöböt (a lookback ablak méretével arányosan, lásd
     MIN_CUMULATIVE_CHANGE_PCT_PER_HOUR).
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

# ÚJ: TÖBB-ABLAKOS elemzés a régi, egyetlen fix 24h ablak helyett. A
# probléma a régivel: az egyenletesség-feltételt (MIN_ALIGNED_FRACTION)
# a TELJES 24h ablakon kellett teljesíteni - ha egy mozgás csak 6-8
# órája indult, a 24 órout nagy része még a "csendes" szakaszból
# származik, tehát a feltétel STRUKTURÁLISAN csak akkor teljesülhetett,
# ha a mozgás már sokáig (és ezért nagy %-ban) tartott. Emiatt a bot a
# valóságban szinte mindig csak 18-20%-os, már-már kifulladó mozgásokat
# kapott el, sosem a korai szakaszt.
#
# A megoldás: több, egyre HOSSZABB ablakot próbálunk (a legrövidebbtől
# indulva), és a LEGRÖVIDEBBET használjuk, ami már minden feltételnek
# megfelel. Ha egy mozgás csak 8 órája tart, de ott már egyenletes/erős,
# AZONNAL jelez - nem kell megvárni, hogy a teljes 24h ablakot is
# domináns legyen benne.
TREND_LOOKBACK_OPTIONS_HOURS = [8, 12, 16, 20, 24]
TREND_LOOKBACK_HOURS = max(TREND_LOOKBACK_OPTIONS_HOURS)   # a klines-lekéréshez ennyi kell összesen
KLINES_LIMIT = TREND_LOOKBACK_HOURS + 5

# ÚJ: a küszöb mostantól ÓRÁNKÉNTI ÜTEM alapján skálázódik (a korábbi,
# 24h-ra vonatkozó 8%-os küszöb ~0.33%/óra ütemnek felel meg) - minden
# ablak-méretnél ezt az ütemet várjuk el, arányosan kisebb abszolút %-kal
# a rövidebb ablakoknál. Így egy 8 órás, még csak ~2.7%-ot mozgott, de
# már egyértelműen egyenletes trend is jelezhet, nem kell megvárni,
# amíg 24 óra alatt eléri a 8%-ot.
MIN_CUMULATIVE_CHANGE_PCT_PER_HOUR = 8.0 / 24.0   # ~0.333 %/óra
MIN_ALIGNED_FRACTION = 0.55       # a gyertyák ennyi hányada menjen a fő irányba
MAX_SINGLE_CANDLE_SHARE = 0.35    # egyetlen gyertya legfeljebb ennyi hányadát adhatja a teljes mozgásnak

# ÚJ: "MÉG ÉL-E A TREND" ellenőrzés. A cél, hogy a jelzés a mozgás
# KÖZEPE/ELEJE felé érkezzen, ne a végén - ezért megnézzük, hogy az adott
# ablak UTOLSÓ NEGYEDÉBEN a mozgás MÉG mindig ugyanabba az irányba
# folytatódik-e, nem laposodott-e már el vagy fordult meg. Ha a friss
# szakasz már nem a fő irányba mutat, az a kifulladás jele - valószínűleg
# elkéstünk volna vele, inkább kihagyjuk. (Az ablak méretével arányosan
# skálázódik: egy 8h-s ablaknál az utolsó ~2h, egy 24h-snál az utolsó ~6h.)
RECENT_WINDOW_FRACTION = 0.25
RECENT_WINDOW_MIN_HOURS = 2

# ÚJ: biztonsági háló - még ha a fenti (szigorúbb) küszöbök mellett is sok
# találat lenne egyszerre (pl. egy igazán élénk piaci napon), KÖRÖNKÉNT
# legfeljebb ennyi riasztást küldünk ki - a többi találat a legerősebbeket
# rangsorolva marad ki (lásd _rank_trend_candidates), csak a logban jelenik
# meg, nem árasztja el a Telegramot.
MAX_ALERTS_PER_RUN = 5

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
def evaluate_slow_trend(kdf: pd.DataFrame, lookback: int) -> Optional[dict]:
    """LEZÁRT gyertyákon (az élőt kihagyjuk, mert még változhat) vizsgálja,
    van-e 'csendes, egyenletes' trend EGY ADOTT lookback (óra) ablakon.
    Lásd a fájl elején lévő blokk-kommentet a módszertanért, és
    evaluate_slow_trend_multi()-t a több-ablakos rangsorolásért."""
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

    # "friss folytatódás" - az ablak UTOLSÓ NEGYEDÉBEN (min.
    # RECENT_WINDOW_MIN_HOURS órára korlátozva) a mozgás MÉG mindig
    # ugyanabba az irányba tart-e. Az ablak méretével arányosan skálázódik.
    recent_hours = max(RECENT_WINDOW_MIN_HOURS, round(lookback * RECENT_WINDOW_FRACTION))
    recent_window = window.iloc[-min(recent_hours, len(window)):]
    recent_start = float(recent_window.iloc[0]["open"])
    recent_end = float(recent_window.iloc[-1]["close"])
    recent_change_pct = (recent_end - recent_start) / recent_start * 100 if recent_start > 0 else 0.0
    still_active = (
        (direction == "LONG" and recent_change_pct > 0)
        or (direction == "SHORT" and recent_change_pct < 0)
    )

    min_cumulative_pct = MIN_CUMULATIVE_CHANGE_PCT_PER_HOUR * lookback
    is_setup = (
        abs(cumulative_change_pct) >= min_cumulative_pct
        and aligned_fraction >= MIN_ALIGNED_FRACTION
        and single_candle_share <= MAX_SINGLE_CANDLE_SHARE
        and still_active
    )

    return {
        "direction": direction,
        "price": end_price,
        "cumulative_change_pct": round(cumulative_change_pct, 2),
        "aligned_fraction": round(aligned_fraction, 2),
        "single_candle_share": round(single_candle_share, 2),
        "recent_change_pct": round(recent_change_pct, 2),
        "still_active": still_active,
        "lookback_hours": lookback,
        "min_cumulative_pct": round(min_cumulative_pct, 2),
        "is_setup": is_setup,
    }


def evaluate_slow_trend_multi(kdf: pd.DataFrame) -> Optional[dict]:
    """Végigpróbálja a TREND_LOOKBACK_OPTIONS_HOURS ablakméreteket a
    LEGRÖVIDEBBTŐL a leghosszabbig, és az ELSŐ (tehát legrövidebb, azaz
    legkorábbi) ablakot adja vissza, ami megfelel MINDEN feltételnek. Ha
    egyik sem felel meg, None-t ad vissza. Ez oldja meg azt a problémát,
    hogy a régi, fix 24h-s ablak strukturálisan csak "már régóta tartó,
    ezért nagy %-os" mozgásokat tudott elkapni - most a bot a
    LEHETŐ LEGKORÁBBI pillanatban jelez, amint egy rövidebb ablakon is
    egyértelműen egyenletes/erős a trend."""
    for lookback in TREND_LOOKBACK_OPTIONS_HOURS:
        trend = evaluate_slow_trend(kdf, lookback)
        if trend is not None and trend["is_setup"]:
            return trend
    # Egyik ablak sem felelt meg - de a leghosszabb (24h) eredményét
    # visszaadjuk (is_setup=False-sal) a diagnosztikai logoláshoz.
    return evaluate_slow_trend(kdf, max(TREND_LOOKBACK_OPTIONS_HOURS))


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

    # ÚJ: mostantól a TALÁLT (legrövidebb, megfelelő) ablakméretet mutatjuk,
    # nem mindig a 24h-t - így látod, milyen KORÁN kapta el a bot a trendet.
    lookback = trend["lookback_hours"]
    recent_hours = max(RECENT_WINDOW_MIN_HOURS, round(lookback * RECENT_WINDOW_FRACTION))

    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {trend['price']:.6f}\n"
        f"📈 Kumulatív mozgás ({lookback}h ablak): {trend['cumulative_change_pct']:+.2f}%\n"
        f"⏱️ Friss folytatódás (utolsó {recent_hours}h): {trend['recent_change_pct']:+.2f}% - a trend MÉG aktív\n"
        f"📐 Egyenletesség: a gyertyák {trend['aligned_fraction']*100:.0f}%-a ment ebbe az irányba\n"
        f"🧩 Legnagyobb egyedi gyertya részesedése: {trend['single_candle_share']*100:.0f}% "
        f"(minél kisebb, annál 'csendesebb' a mozgás)"
        f"{oi_line}\n"
        f"ℹ️ Ez NEM tüske-alapú jelzés - lassú, egyenletes elmozdulást jelez, "
        f"amit a másik botok (tüske-figyelők) nem vesznek észre. A {lookback}h-s "
        f"ablak a LEGRÖVIDEBB volt, ami már minden feltételnek megfelelt - tehát "
        f"ez a lehető legkorábbi pillanat, amikor ezt a mozgást el lehetett kapni."
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

    # ÚJ (két lépéses feldolgozás): ELŐSZÖR csak összegyűjtjük az összes
    # küszöböt teljesítő, cooldown-mentes találatot (nem küldünk még
    # semmit) - utána egy minőségi pontszám szerint RANGSOROLJUK, és
    # csak a legjobb MAX_ALERTS_PER_RUN darabot küldjük ki. Így egy
    # aktív piaci napon (sok egyszerre teljesülő találat) sem árasztja
    # el a Telegramot a bot - a gyengébb találatok egyszerűen a
    # következő órai futásban újra versenyeznek (nem kapnak cooldown-t,
    # mert nem lettek elküldve).
    matches = []

    for symbol in candidates:
        trend = evaluate_slow_trend_multi(klines_map.get(symbol))
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

        if not trend["is_setup"]:
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

        # Minőségi pontszám: nagyobb kumulatív mozgás + egyenletesebb +
        # kevésbé tüske-vezérelt + ERŐSEBB FRISS FOLYTATÓDÁS (a
        # recent_change_pct is beleszámít, hogy a rangsor a "még
        # gyorsuló/aktív" trendeket előnyben részesítse a "már csak
        # éppen hogy még pozitív" esetekkel szemben).
        quality_score = (
            abs(trend["cumulative_change_pct"])
            * trend["aligned_fraction"]
            * (1 - trend["single_candle_share"])
            * (1 + abs(trend["recent_change_pct"]) / 10)
        )
        matches.append({
            "symbol": symbol, "trend": trend, "entry": entry,
            "oi_change_pct": oi_change_pct, "quality_score": quality_score,
        })

    matches.sort(key=lambda m: m["quality_score"], reverse=True)
    to_send = matches[:MAX_ALERTS_PER_RUN]
    suppressed = matches[MAX_ALERTS_PER_RUN:]

    if suppressed:
        logger.info("Rate-limit: %d találat elnyomva ebben a körben (csak a legjobb %d ment ki). Elnyomva: %s",
                    len(suppressed), MAX_ALERTS_PER_RUN,
                    ", ".join(f"{m['symbol']}({m['trend']['cumulative_change_pct']:+.1f}%)" for m in suppressed))

    for m in to_send:
        symbol, trend, entry = m["symbol"], m["trend"], m["entry"]
        oi_change_pct = m["oi_change_pct"]
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
