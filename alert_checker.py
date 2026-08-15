"""
BingX Perpetual - "5M Skalp" kitörés előtti figyelő (v2 - MA-volumen alapú)
====================================================================
Ez a szkript NEM a Streamlit dashboard része — teljesen önállóan fut, a
dashboard-on beállított idősíktól FÜGGETLENÜL mindig az 5 PERCES gyertyákat
vizsgálja (lásd ALERT_TIMEFRAME lent). GitHub Actions cron job hívja meg
periodikusan (elméletileg 5 percenként, de lásd a #2-es megjegyzést).

Mit keres: olyan kis/közepes market cap-ú, TISZTÁN kriptó altcoint, ahol az
utolsó LEZÁRT 5 perces gyertyán:
    - az ár szinte nem mozdult (szűk oldalazás, ±MAX_PRICE_CHANGE%),
    - az Open Interest hirtelen ugrott (min. MIN_OI_INCREASE%),
    - a gyertya volumene messze meghaladja az előző VOLUME_MA_PERIOD db
      lezárt gyertya ÁTLAGÁT (min. MIN_VOL_MULTIPLIER-szerese), és eléri a
      minimum USDT-küszöböt (likviditásszűrő).

v2 VÁLTOZÁS: a volumen-feltétel korábban csak az EGY előző gyertyához
hasonlított, ami lassan épülő trendeknél hamis-negatív jelzést adott. Most
egy mozgóátlaghoz (Moving Average Volume) viszonyítunk, ami stabilabb
referencia egy hirtelen tőkebeáramlás felismerésére.

FONTOS #1: minden Δ számítás KIZÁRÓLAG lezárt gyertyákon dolgozik - az utolsó,
még formálódó (nyitott) gyertyát mindig eldobjuk.

FONTOS #2: mivel minden Actions-futás friss, "üres memóriájú" gépen indul, az
OI-hoz (aminek nincs nyilvános historikus API-ja) egy JSON állapotfájlban
(alert_state.json) tárolt pillanatképet használunk referenciaként. A GitHub
Actions "*/5 * * * *" ütemezése csak "best effort" (csúszhat/kimaradhat), ezért
az OI-referenciapontot is TÉNYLEGES időbélyeg alapján, ~5 perces célablakkal
keressük meg, nem fix darabszám alapján.

FONTOS #3: a BingX ticker végpontja nemcsak kriptó perpetualokat ad vissza,
hanem tokenizált részvény-szerű szintetikus termékeket is - ezeket kiszűrjük.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import pandas as pd
import requests

# ----------------------------------------------------------------------------
# 1) SKALP PARAMÉTEREK - fix (hardkódolt) globális változók
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "5m"      # a háttér-figyelő MINDIG ezt vizsgálja, a dashboard
                             # idősík-választójától teljesen függetlenül
MAX_PRICE_CHANGE = 1.0      # max. %-os ármozgás a gyertyán belül (v2: 0.5 -> 1.0)
MIN_OI_INCREASE = 2.5       # minimum OI-ugrás %-ban (v2: 3.5 -> 2.5)
MIN_CANDLE_VOL_USDT = 50_000  # a vizsgált gyertya USDT-forgalmának minimuma (változatlan)

# --- ÚJ (v2): mozgóátlaghoz viszonyított volumen-feltétel ---
VOLUME_MA_PERIOD = 10       # ennyi megelőző LEZÁRT gyertya átlagához viszonyítunk
MIN_VOL_MULTIPLIER = 2.0    # a vizsgált gyertya legalább ennyiszerese legyen az átlagnak

# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"

STATE_FILE = Path(__file__).parent / "alert_state.json"

# --- "Kis/közepes market cap altcoin" előszűrés (VÁLTOZATLAN, ahogy kérted) ---
MIN_VOLUME_USDT = 300_000
MAX_VOLUME_USDT = 15_000_000

# --- Nem-kriptó termékek kiszűrése (VÁLTOZATLAN) ---
NON_CRYPTO_PREFIXES = ("NCSK",)

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

# --- OI referenciapont keresése (VÁLTOZATLAN) ---
OI_TARGET_WINDOW_MINUTES = 5
OI_MIN_WINDOW_MINUTES = 2
OI_MAX_WINDOW_MINUTES = 20   # kis puffer, ha a külső cron pár percet csúszna
MAX_HISTORY_AGE_MINUTES = 60

# --- Spam-védelem (VÁLTOZATLAN) ---
ALERT_COOLDOWN_MINUTES = 30

MAX_CONCURRENT_REQUESTS = 8
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5
# Kell: 1 nyitott (eldobandó) + 1 vizsgált lezárt + VOLUME_MA_PERIOD baseline gyertya,
# plusz egy kis ráhagyás.
KLINES_LIMIT = VOLUME_MA_PERIOD + 5

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def _timeframe_to_minutes(tf: str) -> int:
    """'5m' -> 5, '1h' -> 60, stb. - az üzenetben megjelenő ablak-hossz kiírásához."""
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 60 * 24
    return 5

AVG_WINDOW_MINUTES = _timeframe_to_minutes(ALERT_TIMEFRAME) * VOLUME_MA_PERIOD

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
# TELEGRAM ÉRTESÍTÉS
# ----------------------------------------------------------------------------

def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("HIBA: hiányzik a TELEGRAM_BOT_TOKEN vagy TELEGRAM_CHAT_ID env változó.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"Telegram hiba ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"Telegram küldési hiba: {e}")


def format_scalp_message(symbol, price, price_change_pct, candle_vol_usdt,
                          vol_multiplier, oi_value, oi_change_pct):
    return (
        f"⚡ 5M SKALP JELZÉS: <b>{symbol}</b> ⚡\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol (5m): {candle_vol_usdt:,.0f} USDT "
        f"({vol_multiplier:.1f}x az elmúlt {AVG_WINDOW_MINUTES} perc átlagához képest)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)\n"
        f"⏱ Szűk oldalazás hirtelen tőkebeáramlással. Figyeld a kitörést!"
    )

# ----------------------------------------------------------------------------
# OI REFERENCIAPONT KERESÉSE (VÁLTOZATLAN)
# ----------------------------------------------------------------------------

def find_oi_baseline(history_without_current, now):
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if OI_MIN_WINDOW_MINUTES <= age_min <= OI_MAX_WINDOW_MINUTES:
            diff = abs(age_min - OI_TARGET_WINDOW_MINUTES)
            if best_diff is None or diff < best_diff:
                best, best_diff = h, diff
    return best

# ----------------------------------------------------------------------------
# EGY SZIMBÓLUM KIÉRTÉKELÉSE (v2: MA-volumen alapú)
# ----------------------------------------------------------------------------

def evaluate_candle(kdf: pd.DataFrame):
    """A klines DataFrame-ből kiszámolja az utolsó LEZÁRT gyertya adatait:
    - ár-változás az előtte lezárt gyertyához képest (változatlan logika)
    - volumen-szorzó a megelőző VOLUME_MA_PERIOD db lezárt gyertya átlagához
      képest (ÚJ v2 logika, a korábbi "csak 1 előző gyertya" helyett)
    Az utolsó, még formálódó gyertyát mindig eldobjuk."""
    if kdf is None:
        return None
    closed = kdf.iloc[:-1]  # az utolsó, még nyitott gyertya eldobása
    # Kell: 1 vizsgált (utolsó lezárt) + VOLUME_MA_PERIOD baseline gyertya
    if len(closed) < VOLUME_MA_PERIOD + 2:
        return None

    curr = closed.iloc[-1]
    prev = closed.iloc[-2]
    baseline_window = closed.iloc[-(VOLUME_MA_PERIOD + 1):-1]  # az utolsó előtti N gyertya

    if prev["close"] <= 0:
        return None

    avg_vol = baseline_window["volume"].mean()
    if avg_vol is None or pd.isna(avg_vol) or avg_vol <= 0:
        return None

    price_change_pct = (curr["close"] - prev["close"]) / prev["close"] * 100
    vol_multiplier = curr["volume"] / avg_vol
    # A kline válasz csak bázis-mennyiségi volument ad, nincs külön USDT-mező,
    # ezért közelítjük: bázis-volumen * záróár ≈ a gyertya USDT-forgalma.
    candle_vol_usdt = float(curr["volume"] * curr["close"])

    return {
        "price": float(curr["close"]),
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
    }

# ----------------------------------------------------------------------------
# FŐ LOGIKA
# ----------------------------------------------------------------------------

async def main():
    state = load_state()
    now = datetime.now(timezone.utc)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            print("Nem sikerült ticker adatot lekérni a BingX API-ból, kilépés.")
            return

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

        print(f"{len(candidates)} jelölt (kis/közepes cap, tisztán kriptó altcoin) a {len(tickers)} párból. "
              f"5m gyertyák ({KLINES_LIMIT} db/pár) + OI lekérése következik...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        oi_results = await asyncio.gather(*oi_tasks)
        kline_results = await asyncio.gather(*kline_tasks)

    oi_map = {s: oi for s, oi in oi_results if oi is not None}
    klines_map = {s: df for s, df in kline_results if df is not None}

    alerts_sent = 0
    evaluated = 0
    for symbol in candidates:
        candle = evaluate_candle(klines_map.get(symbol))
        oi_now = oi_map.get(symbol)
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

        # --- A négy skalp-feltétel egyszerre (v2 küszöbökkel) ---
        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= MIN_OI_INCREASE
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        if is_setup and cooldown_ok:
            msg = format_scalp_message(
                symbol, candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                oi_now, oi_change_pct,
            )
            send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            print(f"SKALP JELZÉS küldve: {symbol} (Ár {candle['price_change_pct']:+.2f}%, "
                  f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%)")

    save_state(state)
    print(f"Kész. {evaluated} pár volt ténylegesen kiértékelhető (elég gyertya+OI history), "
          f"{alerts_sent} riasztás kiküldve ebben a futásban.")


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
              "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
