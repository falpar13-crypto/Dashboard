"""
BingX Perpetual - POC (Point of Control) Visszateszt Figyelő (poc_checker.py)
====================================================================
Önálló, hetedik bot. Fixed Range Volume Profile (FRVP) logikán alapul:

MÓDSZERTAN:
  1. Fraktál-alapú swing-pont keresés (ugyanaz a módszer, mint a másik
     botoknál a támasz/ellenállás szinteknél) egy 15 perces gyertyasoron.
  2. A legutóbbi, LEZÁRT swing-lábra (az utolsó két, váltakozó típusú
     swing-pont közötti szakaszra - pl. utolsó low-tól az utána kialakult
     high-ig) VOLUMEN-PROFILT építünk: az ártartományt sávokra bontjuk, és
     minden gyertya volumenét szétosztjuk a sávok között, amiket a
     gyertya high-low tartománya érint.
  3. Megkeressük a POC-ot (Point of Control) - azt az ársávot, ahol a
     LEGTÖBB volumen forgott a szakaszban.
  4. Miután a láb lezárult, figyeljük, hogy az ár VISSZATÉR-e a POC-hoz
     (támasz/ellenállás-tesztként) - ez a tényleges jelzés, nem a profil
     kialakulása maga.

Ez a bot NEM a meglévő négy (tüske-alapú) bot logikáját követi - itt a
piaci STRUKTÚRA (hol forgott a legtöbb kontraktus) adja a szintet, nem
egy hirtelen ár/volumen-kiugrás.
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
import numpy as np
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("poc_checker")

# ----------------------------------------------------------------------------
# PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "15m"
HISTORY_CANDLES = 200        # kb. 50 óra 15m-en - elég hely több swing-lábhoz

SWING_FRACTAL_LEGS = 3       # ennyi gyertyát nézünk mindkét oldalon egy swing-ponthoz

# ÚJ: minimum swing-mozgás szűrő - a nyers fraktál-keresés minden apró
# kilengést "swingnek" vesz, ami zajossá teszi a láb-kiválasztást. Ez a
# szűrő két egymást követő zigzag-pont között MINIMUM ennyi %-os mozgást
# követel meg - ha kisebb, a kevésbé jelentős pontot eltávolítjuk, csak a
# valóban jelentős fordulópontok maradnak.
MIN_SWING_MOVE_PCT = 1.5
MIN_SWING_LEG_CANDLES = 5    # a profil-szakasznak legalább ennyi gyertyát kell átfognia
VP_BINS = 30                 # ennyi ársávra bontjuk a volumen-profilt

POC_TOLERANCE_PCT = 0.3      # az ár ennyi %-on belül számít "POC-érintésnek"
MIN_AWAY_PCT = 1.0           # a visszatérés előtt legalább ennyire el kellett
                               # távolodnia az ártól a POC-tól (különben triviális
                               # lenne minden gyertya "érintés", rögtön a profil
                               # kialakulása után)

ALERT_COOLDOWN_HOURS = 4     # ugyanarra a POC-ra ennyi órán belül nem jelez újra

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

STATE_FILE = Path(__file__).parent / "poc_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "poc_alert_log.jsonl"

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


# ----------------------------------------------------------------------------
# API HÍVÁSOK (azonos mintázat, mint a másik botokban)
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
# SWING-KERESÉS: standard "százalékos zigzag" algoritmus
# ----------------------------------------------------------------------------
# JAVÍTÁS: a korábbi verzió fraktál-alapú swing-keresést épített, majd
# UTÓLAG próbálta megszűrni a szomszédos, kicsi mozgású pontokat - ez a
# szomszédonkénti törlés hibás volt, láncreakcióban szinte mindent
# összevont (4 pontból 1 maradt egy tesztben). Ehelyett ez egy EGYETLEN
# ÁTHALADÁSOS, klasszikus "N%-os zigzag" algoritmus: a nyers ár-adaton
# fut végig, és csak akkor rögzít új pivot-pontot, ha az aktuális
# szélsőértéktől legalább MIN_SWING_MOVE_PCT%-ot elmozdult az ár az
# ELLENTÉTES irányba - ez természeténél fogva kiszűri az apró zajt,
# nincs szükség utólagos, hibalehetőséget rejtő post-processzálásra.
def _zigzag_pct(closed: pd.DataFrame, min_move_pct: float = MIN_SWING_MOVE_PCT) -> list:
    highs = closed["high"].to_numpy()
    lows = closed["low"].to_numpy()
    n = len(highs)
    if n < 2:
        return []

    pivots = []
    dir_up = None  # None: még nem dőlt el az irány
    # JAVÍTÁS: a kezdeti (irány-eldöntés előtti) szakaszban a szélsőérték
    # POZÍCIÓJÁT is külön nyomon kell követni - a korábbi verzió hibásan
    # mindig a 0. indexet rögzítette pivot-helyként, nem a tényleges
    # szélsőérték gyertyáját.
    extreme_high = float(highs[0])
    extreme_high_idx = 0
    extreme_low = float(lows[0])
    extreme_low_idx = 0
    extreme_idx = 0

    for i in range(1, n):
        if dir_up is None:
            up_move = (highs[i] - extreme_low) / extreme_low * 100 if extreme_low > 0 else 0
            down_move = (extreme_high - lows[i]) / extreme_high * 100 if extreme_high > 0 else 0
            if up_move >= min_move_pct and up_move >= down_move:
                pivots.append((extreme_low_idx, extreme_low, "L"))
                dir_up = True
                extreme_high = float(highs[i])
                extreme_idx = i
            elif down_move >= min_move_pct:
                pivots.append((extreme_high_idx, extreme_high, "H"))
                dir_up = False
                extreme_low = float(lows[i])
                extreme_idx = i
            else:
                if highs[i] > extreme_high:
                    extreme_high = float(highs[i])
                    extreme_high_idx = i
                if lows[i] < extreme_low:
                    extreme_low = float(lows[i])
                    extreme_low_idx = i
            continue

        if dir_up:
            if highs[i] > extreme_high:
                extreme_high = float(highs[i])
                extreme_idx = i
            pullback = (extreme_high - lows[i]) / extreme_high * 100 if extreme_high > 0 else 0
            if pullback >= min_move_pct:
                pivots.append((extreme_idx, extreme_high, "H"))
                dir_up = False
                extreme_low = float(lows[i])
                extreme_idx = i
        else:
            if lows[i] < extreme_low:
                extreme_low = float(lows[i])
                extreme_idx = i
            rally = (highs[i] - extreme_low) / extreme_low * 100 if extreme_low > 0 else 0
            if rally >= min_move_pct:
                pivots.append((extreme_idx, extreme_low, "L"))
                dir_up = True
                extreme_high = float(highs[i])
                extreme_idx = i

    return pivots


# ----------------------------------------------------------------------------
# VOLUMEN-PROFIL + POC
# ----------------------------------------------------------------------------
def compute_poc(df: pd.DataFrame, start_idx: int, end_idx: int, bins: int = VP_BINS) -> Optional[float]:
    """Fixed Range Volume Profile a [start_idx, end_idx] (zárt) gyertya-
    tartományon. Minden gyertya volumenét egyenletesen szétosztjuk azokon
    a sávokon, amiket a gyertya high-low tartománya érint. Visszaadja a
    legtöbb volument kapó sáv KÖZÉPÁRÁT (POC)."""
    window = df.iloc[start_idx:end_idx + 1]
    if len(window) < MIN_SWING_LEG_CANDLES:
        return None

    range_low = float(window["low"].min())
    range_high = float(window["high"].max())
    if range_high <= range_low:
        return None

    bin_edges = np.linspace(range_low, range_high, bins + 1)
    bin_volumes = np.zeros(bins)

    for _, row in window.iterrows():
        c_low, c_high, c_vol = float(row["low"]), float(row["high"]), float(row["volume"])
        if c_vol <= 0:
            continue
        lo_bin = np.searchsorted(bin_edges, c_low, side="right") - 1
        hi_bin = np.searchsorted(bin_edges, c_high, side="right") - 1
        lo_bin = max(0, min(bins - 1, lo_bin))
        hi_bin = max(0, min(bins - 1, hi_bin))
        span = hi_bin - lo_bin + 1
        bin_volumes[lo_bin:hi_bin + 1] += c_vol / span

    poc_bin = int(np.argmax(bin_volumes))
    poc_price = (bin_edges[poc_bin] + bin_edges[poc_bin + 1]) / 2
    return float(poc_price)


def evaluate_poc_retest(kdf: pd.DataFrame) -> Optional[dict]:
    """A legutóbbi lezárt swing-lábra épített POC-hoz való visszatérést
    keresi. Csak akkor ad vissza találatot, ha: (1) a POC-tól a láb vége
    óta legalább MIN_AWAY_PCT %-ra eltávolodott az ár, ÉS (2) az UTOLSÓ
    (élő) gyertyán most tér vissza a POC tolerancia-sávjába."""
    if kdf is None or len(kdf) < HISTORY_CANDLES // 2:
        return None
    closed = kdf.iloc[:-1].reset_index(drop=True)
    if len(closed) < SWING_FRACTAL_LEGS * 2 + MIN_SWING_LEG_CANDLES:
        return None

    zigzag = _zigzag_pct(closed, min_move_pct=MIN_SWING_MOVE_PCT)
    if len(zigzag) < 2:
        return None

    leg_start_idx, leg_start_price, leg_start_type = zigzag[-2]
    leg_end_idx, leg_end_price, leg_end_type = zigzag[-1]
    if leg_end_idx - leg_start_idx < MIN_SWING_LEG_CANDLES:
        return None

    poc_price = compute_poc(closed, leg_start_idx, leg_end_idx)
    if poc_price is None or poc_price <= 0:
        return None

    # a láb vége UTÁNI (a profil lezárása utáni) gyertyák - ide értve az
    # élő gyertyát is - adják a "visszatérés" ellenőrzés ablakát
    post_leg = kdf.iloc[leg_end_idx + 1:].reset_index(drop=True)
    if len(post_leg) < 2:
        return None

    tolerance = poc_price * (POC_TOLERANCE_PCT / 100)
    away_threshold = poc_price * (MIN_AWAY_PCT / 100)

    max_abs_distance = float((post_leg["close"] - poc_price).abs().max())
    if max_abs_distance < away_threshold:
        return None  # sosem távolodott el eléggé - nincs valódi "visszatérés"

    live = kdf.iloc[-1]
    live_close = float(live["close"])
    live_high = float(live["high"])
    live_low = float(live["low"])
    touching_now = (live_low - tolerance) <= poc_price <= (live_high + tolerance)
    if not touching_now:
        return None

    # az UTOLSÓ ELŐTTI (megelőző) post-leg gyertyáknak távol kellett
    # lenniük a POC-tól - különben ez nem "visszatérés", hanem folyamatos
    # ott-tartózkodás lenne
    prior = post_leg.iloc[:-1] if len(post_leg) > 1 else post_leg
    if len(prior) > 0:
        prior_min_distance = float((prior["close"] - poc_price).abs().min())
        if prior_min_distance < tolerance:
            return None  # már korábban is a POC közelében volt - nem friss esemény

    direction = "LONG" if live_close >= poc_price else "SHORT"

    # ÚJ: explicit swing high/low mezők (nem csak a kronológiai
    # start/end sorrend) - a felhasználó élesben szeretné pontosan
    # látni, melyik szint melyik (ez segít visszaigazolni/cáfolni, hogy
    # a fraktál-megerősítési késleltetés miatt elavult szakaszra épül-e
    # a profil, ahelyett hogy a legfrissebb mozgást használná).
    if leg_start_type == "H":
        swing_high_price, swing_high_ts = leg_start_price, closed["timestamp"].iloc[leg_start_idx].isoformat()
        swing_low_price, swing_low_ts = leg_end_price, closed["timestamp"].iloc[leg_end_idx].isoformat()
    else:
        swing_low_price, swing_low_ts = leg_start_price, closed["timestamp"].iloc[leg_start_idx].isoformat()
        swing_high_price, swing_high_ts = leg_end_price, closed["timestamp"].iloc[leg_end_idx].isoformat()

    return {
        "poc_price": round(poc_price, 8),
        "direction": direction,
        "price": live_close,
        "leg_start_ts": closed["timestamp"].iloc[leg_start_idx].isoformat(),
        "leg_end_ts": closed["timestamp"].iloc[leg_end_idx].isoformat(),
        "leg_start_type": leg_start_type,
        "leg_end_type": leg_end_type,
        "swing_high_price": round(swing_high_price, 8),
        "swing_high_ts": swing_high_ts,
        "swing_low_price": round(swing_low_price, 8),
        "swing_low_ts": swing_low_ts,
        "candles_since_leg_end": int(len(kdf) - 1 - leg_end_idx),  # ÚJ: hány gyertyányira van a jelenlegitől
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


def format_poc_message(symbol: str, result: dict) -> str:
    direction = result["direction"]
    action = "POC TÁMASZ-TESZT 🟩⬆️" if direction == "LONG" else "POC ELLENÁLLÁS-TESZT 🟥⬇️"
    header = f"📊 <b>[POC] {symbol}</b> {action}"

    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {result['price']:.6f}\n"
        f"🎯 POC (legforgalmasabb szint): {result['poc_price']:.6f}\n"
        f"📈 Swing HIGH: {result['swing_high_price']:.6f} ({result['swing_high_ts']})\n"
        f"📉 Swing LOW: {result['swing_low_price']:.6f} ({result['swing_low_ts']})\n"
        f"⏱️ A szakasz vége {result['candles_since_leg_end']} gyertyával a jelenlegi előtt zárult "
        f"({ALERT_TIMEFRAME})\n"
        f"ℹ️ Az ár eltávolodott, majd MOST visszatért a legutóbbi swing-láb "
        f"legforgalmasabb (Volume Profile POC) szintjéhez - ez klasszikus "
        f"támasz/ellenállás-teszt pillanat. Ellenőrizd a chartot, mielőtt "
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
        kline_results = await asyncio.gather(*kline_tasks, return_exceptions=True)

    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0

    for symbol in candidates:
        kdf = klines_map.get(symbol)
        if kdf is None:
            continue
        evaluated += 1

        result = evaluate_poc_retest(kdf)
        if result is None:
            continue

        entry = state.setdefault(symbol, {"last_alert_ts": None, "last_poc_price": None})
        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(hours=ALERT_COOLDOWN_HOURS):
                cooldown_ok = False
        if not cooldown_ok:
            continue

        msg = format_poc_message(symbol, result)
        await send_telegram_message(msg)
        entry["last_alert_ts"] = now.isoformat()
        entry["last_poc_price"] = result["poc_price"]
        alerts_sent += 1
        _append_signal_log({
            "ts": now.isoformat(), "symbol": symbol, "direction": result["direction"],
            "poc_price": result["poc_price"], "price": result["price"],
            "leg_start_ts": result["leg_start_ts"], "leg_end_ts": result["leg_end_ts"],
        })
        logger.info("JELZÉS küldve: %s [%s] POC=%.6f ár=%.6f",
                    symbol, result["direction"], result["poc_price"], result["price"])

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
