"""
BingX Perpetual - "Élő Gyertya" Skalp Felhalmozás-figyelő (v4)
====================================================================
Ez a szkript NEM a Streamlit dashboard része — teljesen önállóan fut, a
dashboard-on beállított idősíktól FÜGGETLENÜL mindig az 5 PERCES gyertyákat
vizsgálja (lásd ALERT_TIMEFRAME lent).

v3 VÁLTOZÁS #1 - ÉLŐ GYERTYA: a korábbi verziók mindig eldobták az utolsó,
még formálódó gyertyát, és csak lezárt gyertyákat hasonlítottak össze. Ez
biztonságos volt, de KÉSŐN jelzett - mire egy gyertya lezárt, a mozgás nagy
része már megtörtént. Most a bot a MÉG NYITOTT (élő) gyertyát vizsgálja a
megelőző N db LEZÁRT gyertya átlagához képest, így már a gyertya kialakulása
KÖZBEN jelezhet, ha a volumen/OI szokatlanul felpörög.
Ára van: az élő gyertya adatai a lekérdezés pillanatáig "STOP-kamerázott"
részleges adatok - a végleges (lezárt) érték eltérhet, és elméletileg egy
gyors visszapattanás miatt "hamis" jelzés is előfordulhat. Ez a tudatosan
vállalt ára annak, hogy korábban jelezzen.

v3 VÁLTOZÁS #2 - BELSŐ 30 MÁSODPERCES CIKLUS: mivel a GitHub Actions indítása
(gépfoglalás, checkout, csomagtelepítés) önmagában kb. 15-20 másodpercet
elvesz, ha csak egyszer futtatnánk le a kiértékelést egy Actions-hívásban,
rengeteg idő veszne kárba "üresjáratban". Ezért a main() most egy belső
while-ciklusban, kb. 4.5 percig (270 mp) fut, 30 másodpercenként újra
lekérdezve és kiértékelve az adatokat, majd rendesen leáll - így a következő,
5 percenkénti külső cron-indítás (cron-job.org) egy friss példányt indít, és
a lefedettség gyakorlatilag folyamatos.

v3 VÁLTOZÁS #3 - IRÁNY + ÚJ ÜZENETFORMÁTUM: az üzenet most zöld/piros ponttal
és LONG/SHORT címkével jelzi az irányt (élő gyertya open vs. jelenlegi ár
alapján), a korábbi "szűk oldalazás" szöveg nélkül.

v4 VÁLTOZÁS - AUTOMATIKUS VISSZAIGAZOLÁS: mivel az élő gyertyás jelzés néha
"hamisnak" bizonyul (a mozgás visszafordul, mire a gyertya lezár), a bot
mostantól minden jelzéshez elmenti, MELYIK gyertyáról volt szó. Amikor az a
gyertya ténylegesen lezár, egy MÁSODIK Telegram-üzenetet küld: "✅ Megerősítve"
vagy "❌ Visszafordult". Ez nem lassítja az eredeti jelzést (az továbbra is
azonnal megy), csak utólag, automatikusan visszajelez a jelzés minőségéről -
így idővel kézi munka nélkül is látszik a bot valódi találati aránya.

v5 VÁLTOZÁS - MAGASABB IDŐSÍK TREND-SZŰRŐ: mostantól a bot megnézi az adott
pár 1 órás trendjét (záróár az 1h EMA50-hez képest) is. Az 1h trendet
takarékosan, futásonként csak egyszer (nem minden 30 mp-es körben) kérdezzük
le és memóriában cache-eljük a futás hátralévő részére.

v6 VÁLTOZÁS - HTF FIGYELMEZTETÉS BLOKKOLÁS HELYETT: a v5-ben a trenddel
szembemenő jelzést egyszerűen NEM küldtük ki. A felhasználói visszajelzés
alapján ez túl szigorúnak bizonyult - mostantól a jelzés MINDIG kimegy, csak
egy "⚠️ Trenddel szemben (1h: DOWN/UP)" figyelmeztető sort kap az üzenet, ha
az irány nem egyezik az 1h trenddel. Így a döntés a felhasználónál marad.

v6 VÁLTOZÁS - VISSZAIGAZOLÁS-KÜSZÖB: kiderült, hogy a visszaigazolás-ellenőrző
korábban egy nagyon apró (pl. -0.02% vs +0.02%), gyakorlatilag zajszintű
nyitó->záró mozgást is egyértelmű LONG/SHORT eredménynek vett, ami hibás
"megerősítve" jelzéseket adott. Mostantól van egy CONFIRMATION_MIN_MOVE_PCT
küszöb: ha a záró gyertya nyitó->záró mozgása ennél kisebb, a bot "➖ Semleges
zárás" üzenetet küld megerősítés/cáfolat helyett.

FONTOS: az OI-hoz (aminek nincs nyilvános historikus API-ja) továbbra is egy
JSON állapotfájlban (alert_state.json) tárolt pillanatképet használunk
referenciaként, valós időbélyeg alapján keresve a ~5 perces referenciapontot.
A cooldown-mechanizmus (alert_state.json alapú, per-szimbólum "last_alert_ts")
VÁLTOZATLAN maradt - ez védi ki, hogy egy élő gyertyán belül (amit a 30
másodperces belső ciklus miatt akár 10x is megvizsgálunk) többször riasszon
ugyanarra a mozgásra.
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
ALERT_TIMEFRAME = "5m"      # a háttér-figyelő MINDIG ezt vizsgálja, a dashboard
                             # idősík-választójától teljesen függetlenül
MAX_PRICE_CHANGE = 1.0      # max. %-os ármozgás az élő gyertyában (a legutóbbi
                             # lezárt gyertya záróárához képest)
MIN_OI_INCREASE = 2.5       # minimum OI-ugrás %-ban (~5 perces referenciaablak)
MIN_CANDLE_VOL_USDT = 50_000  # az élő gyertya eddigi USDT-forgalmának minimuma

VOLUME_MA_PERIOD = 10       # ennyi megelőző LEZÁRT gyertya átlagához viszonyítunk
MIN_VOL_MULTIPLIER = 2.0    # az élő gyertya eddigi volumene legalább ennyiszerese
                             # legyen az átlagnak

# --- ÚJ (v3): belső ciklus időzítése egy GitHub Actions futáson belül ---
TOTAL_RUN_BUDGET_SECONDS = 270   # ~4.5 perc - a szkript ennyi ideig fut egyben
PASS_INTERVAL_SECONDS = 30       # ennyi mp-enként fut újra a kiértékelés

# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"

STATE_FILE = Path(__file__).parent / "alert_state.json"

# --- "Kis/közepes market cap altcoin" előszűrés (VÁLTOZATLAN) ---
MIN_VOLUME_USDT = 300_000
MAX_VOLUME_USDT = 50_000_000

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
OI_MAX_WINDOW_MINUTES = 20
MAX_HISTORY_AGE_MINUTES = 60

# --- Spam-védelem (VÁLTOZATLAN - a 30 perces cooldown egyben azt is biztosítja,
# hogy egy 5 perces élő gyertyát a 30 mp-es belső ciklus többszöri vizsgálata
# se riasszon ki ismételten). ---
ALERT_COOLDOWN_MINUTES = 30

# --- ÚJ: automatikus visszaigazolás, amikor az élő gyertya, amire jeleztünk,
# ténylegesen lezár - így a bot "leellenőrzi a saját munkáját" ---
MAX_CONFIRMATION_WAIT_MINUTES = 20   # ha ennyi idő után sincs eredmény, feladjuk (pl. a pár kiesett a szűrőből)
CONFIRMATION_MIN_MOVE_PCT = 0.05     # ennél kisebb nyitó->záró mozgásnál "semleges" a zárás, nem
                                      # számít se megerősítésnek, se cáfolatnak (zajszint kiszűrése)

# --- ÚJ (v5): magasabb idősík trend-szűrő ---
HIGHER_TIMEFRAME = "1h"       # ezen az idősíkon nézzük a fő trendet
HTF_EMA_PERIOD = 50           # EMA(50) az 1h gyertyákon - záróár ehhez képest = trend
HTF_KLINES_LIMIT = 100        # ennyi 1h gyertyát kérünk le az EMA50 stabilizálásához
REQUIRE_HTF_ALIGNMENT = True  # False-ra állítva kikapcsolható a szűrő kódtörlés nélkül

MAX_CONCURRENT_REQUESTS = 8
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5
# Kell: 1 élő (nyitott) + VOLUME_MA_PERIOD lezárt gyertya a baseline-hoz, ráhagyással.
KLINES_LIMIT = VOLUME_MA_PERIOD + 5

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


async def fetch_htf_trend(session, semaphore, symbol):
    """Az 1 órás (HIGHER_TIMEFRAME) trend meghatározása: az utolsó LEZÁRT 1h
    gyertya záróára az EMA(50) fölött van-e (UP) vagy alatta (DOWN). Csak
    lezárt gyertyákat használ, hogy az élő 1h gyertya zaja ne billentse ki."""
    async with semaphore:
        params = {"symbol": symbol, "interval": HIGHER_TIMEFRAME, "limit": HTF_KLINES_LIMIT}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        df = pd.DataFrame(data["data"])
        if "close" not in df.columns or "time" not in df.columns:
            return symbol, None
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["time"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)

        closed = df.iloc[:-1]  # az élő 1h gyertyát itt is eldobjuk
        if len(closed) < HTF_EMA_PERIOD:
            return symbol, None  # nincs elég adat egy megbízható EMA(50)-hez

        ema = closed["close"].ewm(span=HTF_EMA_PERIOD, adjust=False).mean()
        last_close = closed["close"].iloc[-1]
        last_ema = ema.iloc[-1]
        if pd.isna(last_ema):
            return symbol, None
        if last_close > last_ema:
            return symbol, "UP"
        if last_close < last_ema:
            return symbol, "DOWN"
        return symbol, "NEUTRAL"
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


def format_scalp_message(symbol, direction, price, price_change_pct,
                          candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct,
                          htf_trend=None):
    if direction == "LONG":
        header = f"🟢 LONG Felhalmozás: <b>{symbol}</b>"
    else:
        header = f"🔴 SHORT Felhalmozás: <b>{symbol}</b>"

    warning_line = ""
    against_trend = (
        (direction == "LONG" and htf_trend == "DOWN")
        or (direction == "SHORT" and htf_trend == "UP")
    )
    if against_trend:
        warning_line = f"\n⚠️ Trenddel szemben (1h: {htf_trend})"

    return (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{warning_line}"
    )


def format_confirmation_message(symbol, original_direction, status, price_change_since_alert):
    """status: 'confirmed' / 'reversed' / 'neutral' (túl kicsi mozgás a lezáráskor)"""
    if status == "confirmed":
        return (
            f"✅ Megerősítve: <b>{symbol}</b>\n"
            f"A jelzett {original_direction} irány kitartott a gyertya zárásáig "
            f"({price_change_since_alert:+.2f}% a jelzés óta)."
        )
    if status == "neutral":
        return (
            f"➖ Semleges zárás: <b>{symbol}</b>\n"
            f"A gyertya lényegében változatlanul zárt ({price_change_since_alert:+.2f}%) - "
            f"túl kicsi mozgás ahhoz, hogy egyértelműen megerősítsük vagy megcáfoljuk a {original_direction} jelzést."
        )
    return (
        f"❌ Visszafordult: <b>{symbol}</b>\n"
        f"A jelzett {original_direction} irány NEM tartott ki a gyertya zárásáig "
        f"({price_change_since_alert:+.2f}% a jelzés óta) - hamis jelzés volt."
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
# EGY SZIMBÓLUM KIÉRTÉKELÉSE (v3: ÉLŐ gyertya a lezártak átlagához képest)
# ----------------------------------------------------------------------------

def evaluate_candle(kdf: pd.DataFrame):
    """Az ÉLŐ (még nyitott) gyertyát értékeli ki a megelőző VOLUME_MA_PERIOD db
    LEZÁRT gyertya átlagához képest. Az irányt az élő gyertya nyitó- és
    jelenlegi ára határozza meg."""
    if kdf is None or len(kdf) < VOLUME_MA_PERIOD + 1:
        return None

    live = kdf.iloc[-1]                      # az éppen formálódó gyertya
    closed = kdf.iloc[:-1]                    # az összes lezárt gyertya
    baseline_window = closed.iloc[-VOLUME_MA_PERIOD:]
    if len(baseline_window) < VOLUME_MA_PERIOD:
        return None

    prev_close = closed.iloc[-1]["close"]     # az utolsó LEZÁRT gyertya záróára
    if prev_close <= 0 or live["open"] <= 0:
        return None

    avg_vol = baseline_window["volume"].mean()
    if avg_vol is None or pd.isna(avg_vol) or avg_vol <= 0:
        return None

    current_price = float(live["close"])      # az élő gyertya JELENLEGI ára
    price_change_pct = (current_price - prev_close) / prev_close * 100
    vol_multiplier = live["volume"] / avg_vol
    # A kline válasz csak bázis-mennyiségi volument ad, nincs külön USDT-mező,
    # ezért közelítjük: bázis-volumen * jelenlegi ár ≈ az élő gyertya eddigi
    # USDT-forgalma.
    candle_vol_usdt = float(live["volume"] * current_price)
    direction = "LONG" if current_price >= live["open"] else "SHORT"

    return {
        "price": current_price,
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
        "direction": direction,
        "candle_open_ts": live["timestamp"].isoformat(),
    }

# ----------------------------------------------------------------------------
# ÚJ: FÜGGŐ VISSZAIGAZOLÁSOK ELLENŐRZÉSE
# Amikor egy jelzés kiment egy élő gyertyára, elmentjük, melyik gyertyáról
# volt szó (candle_open_ts). Minden következő körben megnézzük: ha ez a
# gyertya időközben LEZÁRT (megjelenik a kdf lezárt részében), elküldjük a
# visszaigazoló/cáfoló üzenetet, és töröljük a "függő" állapotot.
# ----------------------------------------------------------------------------

def check_pending_confirmation(entry: dict, symbol: str, kdf: pd.DataFrame, now: datetime) -> bool:
    """True-t ad vissza, ha küldött visszaigazoló üzenetet (ekkor a hívó fél
    törli a pending_confirmation-t az entry-ből)."""
    pending = entry.get("pending_confirmation")
    if not pending:
        return False

    sent_dt = datetime.fromisoformat(pending["sent_ts"])
    if (now - sent_dt) > timedelta(minutes=MAX_CONFIRMATION_WAIT_MINUTES):
        # Túl régi, valószínűleg a pár kiesett a szűrőből - feladjuk csendben.
        entry["pending_confirmation"] = None
        return False

    if kdf is None or len(kdf) < 2:
        return False  # ebben a körben nincs friss adatunk erről a párról

    try:
        target_ts = pd.Timestamp(pending["candle_open_ts"])
    except (ValueError, TypeError):
        entry["pending_confirmation"] = None
        return False
    if target_ts.tzinfo is not None:
        target_ts = target_ts.tz_localize(None)  # a BingX klines timestampjei tz-naive-ok

    closed = kdf.iloc[:-1]
    match = closed[closed["timestamp"] == target_ts]
    if match.empty:
        return False  # a gyertya még nem zárt le (vagy még nem látjuk lezártként)

    final_candle = match.iloc[-1]
    final_open = float(final_candle["open"])
    final_close = float(final_candle["close"])
    final_move_pct = (final_close - final_open) / final_open * 100 if final_open > 0 else 0.0

    if abs(final_move_pct) < CONFIRMATION_MIN_MOVE_PCT:
        # Túl kicsi, gyakorlatilag zajszintű mozgás - se nem megerősítés, se nem cáfolat.
        status = "neutral"
    else:
        final_direction = "LONG" if final_move_pct > 0 else "SHORT"
        status = "confirmed" if final_direction == pending["direction"] else "reversed"

    price_change_since_alert = (
        (final_close - pending["alert_price"]) / pending["alert_price"] * 100
        if pending["alert_price"] > 0 else 0.0
    )

    msg = format_confirmation_message(symbol, pending["direction"], status, price_change_since_alert)
    send_telegram_message(msg)
    print(f"VISSZAIGAZOLÁS küldve: {symbol} -> {status}")
    entry["pending_confirmation"] = None
    return True

# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR (a belső 30 mp-es ciklus egy "üteme")
# ----------------------------------------------------------------------------

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            print("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, 0, htf_cache

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
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        oi_results = await asyncio.gather(*oi_tasks)
        kline_results = await asyncio.gather(*kline_tasks)

        # --- ÚJ (v5): 1h HTF trend lekérése, de CSAK azokra a szimbólumokra,
        # amikre még nincs cache-elt eredményünk ebben a futásban. ---
        missing_htf = [s for s in candidates if s not in htf_cache]
        if missing_htf:
            htf_tasks = [fetch_htf_trend(session, semaphore, s) for s in missing_htf]
            htf_results = await asyncio.gather(*htf_tasks)
            for s, trend in htf_results:
                if trend is not None:
                    htf_cache[s] = trend

    oi_map = {s: oi for s, oi in oi_results if oi is not None}
    klines_map = {s: df for s, df in kline_results if df is not None}

    # --- 1. lépés: függő visszaigazolások ellenőrzése (minden korábban
    # jelzett, még le nem zárt gyertyára, nem csak a mostani jelöltekre) ---
    confirmations_sent = 0
    for symbol, entry in state.items():
        if not isinstance(entry, dict) or not entry.get("pending_confirmation"):
            continue
        if check_pending_confirmation(entry, symbol, klines_map.get(symbol), now):
            confirmations_sent += 1

    # --- 2. lépés: új jelzések keresése (VÁLTOZATLAN logika) ---
    alerts_sent = 0
    evaluated = 0
    htf_warned = 0
    for symbol in candidates:
        candle = evaluate_candle(klines_map.get(symbol))
        oi_now = oi_map.get(symbol)
        if candle is None or oi_now is None:
            continue
        evaluated += 1

        entry = state.setdefault(symbol, {"oi_history": [], "last_alert_ts": None, "pending_confirmation": None})
        entry["oi_history"].append({"ts": now.isoformat(), "oi": oi_now})
        cutoff = now - timedelta(minutes=MAX_HISTORY_AGE_MINUTES)
        entry["oi_history"] = [
            h for h in entry["oi_history"] if datetime.fromisoformat(h["ts"]) >= cutoff
        ]

        oi_baseline = find_oi_baseline(entry["oi_history"][:-1], now)
        if oi_baseline is None or oi_baseline["oi"] <= 0:
            continue

        oi_change_pct = (oi_now - oi_baseline["oi"]) / oi_baseline["oi"] * 100

        # --- v6: a magasabb idősík trendje MOSTANTÓL NEM blokkol, csak
        # figyelmeztető sort kap az üzenet, ha a jelzés a trenddel szemben megy. ---
        htf_trend = htf_cache.get(symbol)
        against_trend = REQUIRE_HTF_ALIGNMENT and (
            (candle["direction"] == "LONG" and htf_trend == "DOWN")
            or (candle["direction"] == "SHORT" and htf_trend == "UP")
        )

        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= MIN_OI_INCREASE
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        if against_trend:
            htf_warned += 1

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
            )
            send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            # Elmentjük, mit kell majd visszaigazolni, amikor ez a gyertya lezár.
            entry["pending_confirmation"] = {
                "candle_open_ts": candle["candle_open_ts"],
                "alert_price": candle["price"],
                "direction": candle["direction"],
                "sent_ts": now.isoformat(),
            }
            alerts_sent += 1
            trend_note = " ⚠️ TRENDDEL SZEMBEN" if against_trend else ""
            print(f"JELZÉS küldve: {symbol} [{candle['direction']}] (Ár {candle['price_change_pct']:+.2f}%, "
                  f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%, "
                  f"1h trend: {htf_trend or 'ismeretlen'}){trend_note}")

    if htf_warned:
        print(f"  (ebben a körben {htf_warned} kiküldött jelzés ment trenddel szemben - figyelmeztetéssel)")

    return alerts_sent, evaluated, valid_contracts, confirmations_sent, htf_cache

# ----------------------------------------------------------------------------
# FŐ CIKLUS - kb. 4.5 percig fut, 30 mp-enként újra kiértékelve
# ----------------------------------------------------------------------------

async def main():
    state = load_state()
    loop_start = time.monotonic()
    valid_contracts = None
    htf_cache = {}   # symbol -> "UP"/"DOWN"/"NEUTRAL", futáson belül újrahasznosítva
    pass_num = 0
    total_alerts = 0
    total_confirmations = 0

    while True:
        elapsed_total = time.monotonic() - loop_start
        if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
            break

        pass_num += 1
        pass_start = time.monotonic()
        now = datetime.now(timezone.utc)

        alerts, evaluated, valid_contracts, confirmations, htf_cache = await run_single_pass(
            state, valid_contracts, htf_cache, now
        )
        total_alerts += alerts
        total_confirmations += confirmations
        save_state(state)  # minden kör után mentünk, ne vesszen el adat félbeszakadás esetén

        print(f"[{pass_num}. kör] {evaluated} pár kiértékelve, {alerts} riasztás, "
              f"{confirmations} visszaigazolás (összesen eddig: {total_alerts} riasztás, "
              f"{total_confirmations} visszaigazolás).")

        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break

        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    print(f"Ciklus vége: {pass_num} kör lefutott, összesen {total_alerts} riasztás, "
          f"{total_confirmations} visszaigazolás. A szkript rendesen leáll - a következő "
          f"külső cron-hívás friss példányt indít.")


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
              "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
