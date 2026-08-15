"""
BingX Perpetual - "Csendes felhalmozás / kitörés előtti" figyelő
====================================================================
Ez a szkript NEM a Streamlit dashboard része — önállóan fut, GitHub Actions
cron job hívja meg periodikusan (elméletileg 5 percenként). Célja: kis/közepes
market cap-ú ALTCOINOK között megkeresi azokat, ahol:

    - az Open Interest (nyitott pozíciók) NÖVEKSZIK,
    - a 24h volumen is NÖVEKSZIK,
    - de az ÁR ALIG MOZDUL (oldalaz)

...ami klasszikus "csendes felhalmozás" mintázat egy hirtelen kitörés előtt.
Találat esetén Telegram üzenetet küld.

FONTOS #1: mivel minden Actions-futás friss, "üres memóriájú" gépen indul, a
korábbi méréseket egy JSON állapotfájlban (alert_state.json) tároljuk, amit
minden futás után visszaírunk a repóba. Enélkül nem lenne mihez viszonyítani.

FONTOS #2 (JAVÍTVA): a GitHub Actions "*/5 * * * *" cron ütemezése CSAK "best
effort" - nagy terhelés esetén akár 1+ órás csúszás/kihagyás is előfordulhat.
Emiatt NEM szabad fix darabszámú (pl. "utolsó 10 mérés") ablakot feltételezni,
mert az irreálisan hosszú/rövid tényleges időtartamot takarhat. Ehelyett a
TÉNYLEGES időbélyegek alapján keressük meg a ~TARGET_WINDOW_MINUTES korú
referenciapontot minden egyes szimbólumnál.

FONTOS #3 (JAVÍTVA): a BingX ticker végpontja nemcsak kriptó perpetualokat ad
vissza, hanem tokenizált részvény-szerű szintetikus termékeket is (pl.
"NCSKJNJ2USD-USDT" = a Johnson & Johnson részvényt követő termék). Ezeket egy
heurisztikus szűrővel + a hivatalos contracts listával kizárjuk.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import requests

# ----------------------------------------------------------------------------
# BEÁLLÍTÁSOK - nyugodtan hangold ízlés szerint
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"

STATE_FILE = Path(__file__).parent / "alert_state.json"

# --- "Kis/közepes market cap altcoin" szűrés a 24h volumen alapján ---
MIN_VOLUME_USDT = 300_000        # ennél illikvidebb "shitcoin" ne érdekeljen
MAX_VOLUME_USDT = 15_000_000     # ennél nagyobb napi volumen -> valszeg large cap, kizárva

# --- ÚJ: nem-kriptó (tokenizált részvény/egyéb szintetikus) termékek kiszűrése ---
# Megfigyelés alapján a BingX ilyen prefixszel jelöli ezeket - bővítsd, ha
# újabb "gyanús" tickert látsz a riasztásokban.
NON_CRYPTO_PREFIXES = ("NCSK",)

def is_probably_crypto(symbol: str) -> bool:
    """Heurisztikus szűrő: kizárja a tokenizált részvény-szerű termékeket.
    Valódi kriptó tickerek szinte sosem tartalmaznak "USD"-t a szimbólum
    BELSEJÉBEN (csak a "-USDT" végződésben) - ez erős jel szintetikus
    termékre (pl. "...2USD-USDT")."""
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

# --- ÚJ: valós időbélyeg alapú, változó cron-cadence-hez robusztus ablak ---
TARGET_WINDOW_MINUTES = 45    # ideális összehasonlítási ablak
MIN_WINDOW_MINUTES = 20       # ennél frissebb baseline túl zajos, nem fogadjuk el
MAX_WINDOW_MINUTES = 90       # ennél régebbi baseline már nem "gyors mozgás", kihagyjuk
MAX_HISTORY_AGE_MINUTES = 240  # ennél régebbi history-bejegyzéseket eldobjuk (fájl tisztán tartása)

# --- Riasztási küszöbök (a fenti ablakon belüli változás) ---
OI_GROWTH_THRESHOLD_PCT = 5.0        # OI-nak legalább ennyit kell nőnie
VOLUME_GROWTH_THRESHOLD_PCT = 15.0   # a volumennek is nőnie kell legalább ennyit
PRICE_FLAT_THRESHOLD_PCT = 1.0       # az ár +/- ennyi %-on belül maradjon ("oldalaz")

# --- Spam-védelem: ugyanarra a párra ennyi ideig nem riaszt újra ---
ALERT_COOLDOWN_MINUTES = 90   # ~1.5 óra (a felhasználó kérésére csökkentve 4 óráról)

MAX_CONCURRENT_REQUESTS = 8
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

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
    """A hivatalos, listázott USDT-perpetual szerződések halmaza - erre
    keresztezzük rá a ticker-listát, hogy kiszűrjük az esetleges extra
    (nem szabvány perpetual) instrumentumokat."""
    data = await _get_json(session, CONTRACTS_ENDPOINT)
    if not data or "data" not in data:
        return None  # ha nem elérhető, ne blokkoljuk emiatt a futást
    return {
        c["symbol"] for c in data["data"]
        if c.get("symbol", "").endswith("-USDT") and c.get("status", 1) == 1
    }


async def fetch_open_interest(session, semaphore, symbol):
    async with semaphore:
        data = await _get_json(session, OI_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.05)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        try:
            return symbol, float(data["data"].get("openInterest", 0))
        except (TypeError, ValueError):
            return symbol, None

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


def format_alert_message(symbol, current, oi_change, price_change, vol_change, window_minutes):
    return (
        f"🚨 <b>Csendes felhalmozás gyanú</b> — <b>{symbol}</b>\n\n"
        f"💰 Ár: ${current['price']:.6f}  ({price_change:+.2f}%)\n"
        f"📊 Volumen (24h): {current['volume']:,.0f} USDT  ({vol_change:+.2f}%)\n"
        f"🧲 Open Interest: {current['oi']:,.0f}  ({oi_change:+.2f}%)\n\n"
        f"⏱ Elmúlt ~{window_minutes} percben: OI és volumen nő, az ár szinte nem mozdult.\n"
        f"Ez sokszor kitörés előtti felhalmozási mintázat — érdemes ránézni a BingX-en."
    )

# ----------------------------------------------------------------------------
# FŐ LOGIKA
# ----------------------------------------------------------------------------

def find_baseline(history_without_current, now):
    """Megkeresi a history-ban azt a bejegyzést, amelynek kora a legközelebb
    esik a TARGET_WINDOW_MINUTES-hez, de csak a [MIN, MAX] tartományon belül.
    Ez teszi robusztussá a logikát a GitHub Actions kiszámíthatatlan
    ütemezéséhez képest - fix darabszám helyett valós idő alapján dolgozik."""
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if MIN_WINDOW_MINUTES <= age_min <= MAX_WINDOW_MINUTES:
            diff = abs(age_min - TARGET_WINDOW_MINUTES)
            if best_diff is None or diff < best_diff:
                best, best_diff = h, diff
    return best


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

        print(f"{len(candidates)} jelölt (kis/közepes cap, tisztán kriptó altcoin) a {len(tickers)} párból.")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        oi_results = await asyncio.gather(*oi_tasks)

    oi_map = {s: oi for s, oi in oi_results if oi is not None}

    alerts_sent = 0
    for symbol in candidates:
        info = tickers[symbol]
        oi = oi_map.get(symbol)
        if oi is None:
            continue

        entry = state.setdefault(symbol, {"history": [], "last_alert_ts": None})
        entry["history"].append({
            "ts": now.isoformat(),
            "price": info["last_price"],
            "oi": oi,
            "volume": info["quote_volume_24h"],
        })
        # Régi bejegyzések eldobása (időalapú, nem darabszám-alapú trimmelés)
        cutoff = now - timedelta(minutes=MAX_HISTORY_AGE_MINUTES)
        entry["history"] = [
            h for h in entry["history"] if datetime.fromisoformat(h["ts"]) >= cutoff
        ]

        current = entry["history"][-1]
        baseline = find_baseline(entry["history"][:-1], now)
        if baseline is None:
            continue  # még nincs megfelelő korú (20-90 perces) referenciapont

        if baseline["oi"] <= 0 or baseline["price"] <= 0 or baseline["volume"] <= 0:
            continue

        oi_change = (current["oi"] - baseline["oi"]) / baseline["oi"] * 100
        price_change = (current["price"] - baseline["price"]) / baseline["price"] * 100
        vol_change = (current["volume"] - baseline["volume"]) / baseline["volume"] * 100
        window_minutes = int((now - datetime.fromisoformat(baseline["ts"])).total_seconds() / 60)

        is_setup = (
            oi_change >= OI_GROWTH_THRESHOLD_PCT
            and vol_change >= VOLUME_GROWTH_THRESHOLD_PCT
            and abs(price_change) <= PRICE_FLAT_THRESHOLD_PCT
        )

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        if is_setup and cooldown_ok:
            msg = format_alert_message(symbol, current, oi_change, price_change, vol_change, window_minutes)
            send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            print(f"RIASZTÁS küldve: {symbol} (OI {oi_change:+.2f}%, Vol {vol_change:+.2f}%, Ár {price_change:+.2f}%, ablak {window_minutes} perc)")

    save_state(state)
    print(f"Kész. {alerts_sent} riasztás kiküldve ebben a futásban.")


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
              "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
