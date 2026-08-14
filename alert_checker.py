"""
BingX Perpetual - "Csendes felhalmozás / kitörés előtti" figyelő
====================================================================
Ez a szkript NEM a Streamlit dashboard része — önállóan fut, GitHub Actions
cron job hívja meg periodikusan (pl. 5 percenként). Célja: kis/közepes
market cap-ú altcoinok között megkeresi azokat, ahol:

    - az Open Interest (nyitott pozíciók) NÖVEKSZIK,
    - a 24h volumen is NÖVEKSZIK,
    - de az ÁR ALIG MOZDUL (oldalaz)

...ami klasszikus "csendes felhalmozás" mintázat egy hirtelen kitörés előtt.
Találat esetén Telegram üzenetet küld.

FONTOS: mivel minden Actions-futás friss, "üres memóriájú" gépen indul, a
korábbi méréseket egy JSON állapotfájlban (alert_state.json) tároljuk, amit
minden futás után visszaírunk a repóba. Enélkül nem lenne mihez viszonyítani.
"""

import asyncio
import json
import os
import sys
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

STATE_FILE = Path(__file__).parent / "alert_state.json"

# --- "Kis/közepes market cap altcoin" szűrés a 24h volumen alapján ---
# (a BingX publikus API nem ad market cap adatot, ezért a volumen a legjobb
# elérhető közelítés: a nagyon nagy volumenű párok jellemzően a nagy capes
# coinok - BTC, ETH stb. - ezeket itt direkt kizárjuk).
MIN_VOLUME_USDT = 300_000        # ennél illikvidebb "shitcoin" ne érdekeljen
MAX_VOLUME_USDT = 15_000_000     # ennél nagyobb napi volumen -> valszeg large cap, kizárva

# --- Az összehasonlítási ablak mérete ---
# Ha 5 percenként fut a job, HISTORY_WINDOW=10 kb. 45-50 perces "lookback"-ot ad.
# (a friss, aktuális mérés vs. az ablak legrégebbi mérése kerül összevetésre)
HISTORY_WINDOW = 10

# --- Riasztási küszöbök (az összehasonlítási ablakon belüli változás) ---
OI_GROWTH_THRESHOLD_PCT = 5.0        # OI-nak legalább ennyit kell nőnie
VOLUME_GROWTH_THRESHOLD_PCT = 15.0   # a volumennek is nőnie kell legalább ennyit
PRICE_FLAT_THRESHOLD_PCT = 1.0       # az ár +/- ennyi %-on belül maradjon ("oldalaz")

# --- Spam-védelem: ugyanarra a párra ennyi ideig nem riaszt újra ---
ALERT_COOLDOWN_MINUTES = 240   # 4 óra

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

async def main():
    state = load_state()
    now = datetime.now(timezone.utc)
    check_interval_minutes = None  # csak infóhoz, a history hossza számít igazán

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            print("Nem sikerült ticker adatot lekérni a BingX API-ból, kilépés.")
            return

        candidates = [
            s for s, info in tickers.items()
            if MIN_VOLUME_USDT <= info["quote_volume_24h"] <= MAX_VOLUME_USDT
        ]
        print(f"{len(candidates)} jelölt (kis/közepes cap altcoin) a {len(tickers)} párból.")

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
        entry["history"] = entry["history"][-HISTORY_WINDOW:]

        if len(entry["history"]) < HISTORY_WINDOW:
            continue  # még nincs elég historikus adat ehhez a párhoz

        baseline = entry["history"][0]
        current = entry["history"][-1]

        if baseline["oi"] <= 0 or baseline["price"] <= 0 or baseline["volume"] <= 0:
            continue

        oi_change = (current["oi"] - baseline["oi"]) / baseline["oi"] * 100
        price_change = (current["price"] - baseline["price"]) / baseline["price"] * 100
        vol_change = (current["volume"] - baseline["volume"]) / baseline["volume"] * 100

        window_minutes = int(
            (datetime.fromisoformat(current["ts"]) - datetime.fromisoformat(baseline["ts"])).total_seconds() / 60
        )

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
            print(f"RIASZTÁS küldve: {symbol} (OI {oi_change:+.2f}%, Vol {vol_change:+.2f}%, Ár {price_change:+.2f}%)")

    save_state(state)
    print(f"Kész. {alerts_sent} riasztás kiküldve ebben a futásban.")


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
              "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
