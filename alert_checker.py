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

v8 VÁLTOZÁS - TÁMASZ/ELLENÁLLÁS FIGYELMEZTETÉS: a bot most egy egyszerű,
"N-periódusos csatorna" módszerrel (az utolsó SR_LOOKBACK_PERIOD=60 db lezárt
1h gyertya legalacsonyabb mélypontja/legmagasabb csúcsa - NEM chartolvasói
Order Block, hanem jól definiált matematikai közelítés) megnézi, közel van-e
az ár egy támaszhoz/ellenálláshoz (±SR_PROXIMITY_PCT%). Csakúgy, mint a HTF
trend-szűrőnél, ez SEM blokkolja a jelzést - a jelzés MINDIG kimegy, csak:
  - 🎯 kiemelést kap, ha a jelzés egy szintről való visszapattanással egyezik
    (PUMP a támasznál, DUMP az ellenállásnál),
  - ⚠️ figyelmeztetést kap, ha a jelzés egy szint ELLEN menne (DUMP a
    támasznál, PUMP az ellenállásnál - onnan könnyen visszapattanhat).
Nincs plusz API-hívás: ugyanabból az 1h lekérésből számol, amit a HTF
trendhez is használunk.

v9 VÁLTOZÁS - JAVÍTVA A VISSZAIGAZOLÁS PARADOXONA: korábban a "megerősítve/
visszafordult" döntés a lezáró gyertya SAJÁT nyitó->záró irányát nézte, ami
paradox üzenetet adhatott (pl. "Megerősítve +1.52%" egy DUMP jelzésnél).
Mostantól egységesen a JELZÉSKORI árhoz viszonyított tényleges elmozdulás
dönt - ez mindig konzisztens a kiírt %-kal.

v9 VÁLTOZÁS - RSI + MACD INFÓ: a bot most RSI(14)-et és MACD(12,26,9)-et is
számol az 5m adatokból (nincs plusz API-hívás, csak nagyobb limit ugyanarra a
lekérésre). Ez CSAK tájékoztató jellegű sor az üzenetben - nem szűr és nem
blokkol semmit. Az RSI mellett "(túlvett)"/"(túladott)" jelölés jelenik meg
RSI_OVERBOUGHT/RSI_OVERSOLD küszöbök alapján.

v9 VÁLTOZÁS - NCFX KIZÁRVA: az NCSK mellett az NCFX előtagú (szintén nem
kriptó, tokenizált) termékek is ki vannak zárva a jelöltek közül.

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
from typing import Optional

import aiohttp
import pandas as pd
import requests

# ----------------------------------------------------------------------------
# 1) SKALP PARAMÉTEREK - fix (hardkódolt) globális változók
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "5m"      # a háttér-figyelő MINDIG ezt vizsgálja, a dashboard
                             # idősík-választójától teljesen függetlenül
MAX_PRICE_CHANGE = 3.0      # max. %-os ármozgás az élő gyertyában (a legutóbbi
                             # lezárt gyertya záróárához képest)
MIN_OI_INCREASE = 1.5       # minimum OI-ugrás %-ban (~5 perces referenciaablak)
MIN_CANDLE_VOL_USDT = 15_000  # az élő gyertya eddigi USDT-forgalmának minimuma

VOLUME_MA_PERIOD = 10       # ennyi megelőző LEZÁRT gyertya átlagához viszonyítunk
MIN_VOL_MULTIPLIER = 2.0    # az élő gyertya eddigi volumene legalább ennyiszerese
                             # legyen az átlagnak

# --- ÚJ (v10): A/B teszt - "Sáv kitörés" jelzéstípus a "Standard" mellé ---
# Ugyanazok a fő feltételek (OI, volumen, ár) döntik el, hogy egyáltalán menjen-e
# jelzés - ez a rész csak UTÓLAG CÍMKÉZI a már jóváhagyott jelzést aszerint,
# hogy egy előzetes szűk sávból való kitöréssel esik-e egybe.
RANGE_LOOKBACK_PERIOD = 8          # ennyi megelőző lezárt gyertyán nézzük a sáv szélességét (40 perc)
RANGE_COMPRESSION_THRESHOLD_PCT = 1.5  # a sáv ennél szűkebb legyen ahhoz, hogy "beszűkültnek" számítson

# --- ÚJ: Killzone (tőzsdenyitási időablakok) - UTC időzóna, "HH:MM" formátumban ---
LONDON_KILLZONE = ("07:00", "10:00")
NY_KILLZONE = ("13:30", "16:00")

# --- ÚJ: Funding Rate (Squeeze Vadász) - négyezredszázalékos küszöb (-0.01% / +0.01%) ---
FUNDING_SQUEEZE_THRESHOLD_PCT = 0.01

# --- ÚJ: EMA Squeeze (beszorulás) - önálló, a STANDARD/SÁV KITÖRÉS jelzésektől
# teljesen független riasztás-logika ---
EMA_SQUEEZE_FAST_PERIOD = 20
EMA_SQUEEZE_SLOW_PERIOD = 50
EMA_SQUEEZE_LOOKBACK_CANDLES = 4       # ennyi utolsó LEZÁRT gyertyán nézzük a szorítást
EMA_SQUEEZE_MAX_EMA_GAP_PCT = 1.5      # az EMA20 és EMA50 távolsága ennél kisebb legyen
EMA_SQUEEZE_MIN_OI_INCREASE = 0.8      # a standardnál lazább OI-küszöb (a setup önmagában erős)
EMA_SQUEEZE_MIN_VOL_MULTIPLIER = 1.3   # a standardnál lazább volumen-küszöb

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
FUNDING_RATE_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/premiumIndex"

STATE_FILE = Path(__file__).parent / "alert_state.json"

# --- "Kis/közepes market cap altcoin" előszűrés (VÁLTOZATLAN) ---
MIN_VOLUME_USDT = 500_000
MAX_VOLUME_USDT = 15_000_000

# --- Nem-kriptó termékek kiszűrése (VÁLTOZATLAN) ---
NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

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

# --- ÚJ (v5): magasabb idősík trend-szűrő ---
HIGHER_TIMEFRAME = "1h"       # ezen az idősíkon nézzük a fő trendet
HTF_EMA_PERIOD = 50           # EMA(50) az 1h gyertyákon - záróár ehhez képest = trend
HTF_KLINES_LIMIT = 100        # ennyi 1h gyertyát kérünk le az EMA50 stabilizálásához
REQUIRE_HTF_ALIGNMENT = True  # False-ra állítva kikapcsolható a szűrő kódtörlés nélkül

MAX_CONCURRENT_REQUESTS = 12   # a MAX_VOLUME_USDT emelése miatt nagyobb lett a
                                # jelöltlista, ezért itt is emeltünk kicsit (8 -> 12)
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5
# Kell: 1 élő (nyitott) + VOLUME_MA_PERIOD lezárt gyertya a baseline-hoz, PLUSZ
# elég előzmény egy stabil RSI(14)/MACD(12,26,9) számításához (~35-40 minimum,
# biztonsági ráhagyással 65).
KLINES_LIMIT = 65

# --- ÚJ: RSI infó-küszöbök (csak megjelenítés, NEM szűr - a felhasználó kérésére) ---
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

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


# --- ÚJ: Funding Rate lekérdezés (Squeeze Vadász) ---
async def fetch_funding_rate(session, semaphore, symbol):
    async with semaphore:
        data = await _get_json(session, FUNDING_RATE_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        try:
            raw = data["data"].get("lastFundingRate")
            if raw is None:
                return symbol, None
            return symbol, float(raw) * 100  # a BingX tizedes-törtet ad vissza -> %-ra váltjuk
        except (TypeError, ValueError):
            return symbol, None


# --- ÚJ (v7): egyszerű "N-periódusos csatorna" támasz/ellenállás ---
SR_LOOKBACK_PERIOD = 60     # ennyi lezárt 1h gyertya alapján számoljuk a szinteket
SR_PROXIMITY_PCT = 0.5      # ennyi %-on belül számít "a szint közelének"


async def fetch_htf_trend(session, semaphore, symbol):
    """Az 1 órás (HIGHER_TIMEFRAME) trend + támasz/ellenállás meghatározása,
    UGYANABBÓL az egyetlen lekérésből (nincs plusz API-hívás):
    - trend: az utolsó LEZÁRT 1h gyertya záróára az EMA(50) fölött (UP) vagy
      alatta (DOWN) van-e
    - support/resistance: az utolsó SR_LOOKBACK_PERIOD db lezárt 1h gyertya
      legalacsonyabb mélypontja / legmagasabb csúcsa (egyszerű, jól definiált
      "N-periódusos csatorna" módszer - nem chartolvasói/szubjektív szint)
    Csak lezárt gyertyákat használ mindenhol."""
    async with semaphore:
        params = {"symbol": symbol, "interval": HIGHER_TIMEFRAME, "limit": HTF_KLINES_LIMIT}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.03)
        empty_result = {"trend": None, "support": None, "resistance": None}
        if not data or "data" not in data or not data["data"]:
            return symbol, empty_result
        df = pd.DataFrame(data["data"])
        required_cols = {"close", "high", "low", "time"}
        if not required_cols.issubset(df.columns):
            return symbol, empty_result
        for col in ["close", "high", "low"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["time"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)

        closed = df.iloc[:-1]  # az élő 1h gyertyát itt is eldobjuk
        if len(closed) < HTF_EMA_PERIOD:
            return symbol, empty_result  # nincs elég adat egy megbízható EMA(50)-hez

        ema = closed["close"].ewm(span=HTF_EMA_PERIOD, adjust=False).mean()
        last_close = closed["close"].iloc[-1]
        last_ema = ema.iloc[-1]
        trend = None
        if not pd.isna(last_ema):
            if last_close > last_ema:
                trend = "UP"
            elif last_close < last_ema:
                trend = "DOWN"
            else:
                trend = "NEUTRAL"

        support = resistance = None
        sr_window = closed.iloc[-SR_LOOKBACK_PERIOD:]
        if len(sr_window) >= SR_LOOKBACK_PERIOD:
            support = float(sr_window["low"].min())
            resistance = float(sr_window["high"].max())

        return symbol, {"trend": trend, "support": support, "resistance": resistance}




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


# --- ÚJ: Killzone (tőzsdenyitási időablakok) detektálása ---
def _parse_hhmm(hhmm: str) -> tuple:
    h, m = hhmm.split(":")
    return int(h), int(m)


def get_active_killzone(now: datetime) -> Optional[str]:
    """Visszaadja az aktuális UTC időhöz tartozó killzone nevét ("London" /
    "New York"), vagy None-t, ha egyik ablakba sem esik bele."""
    current_minutes = now.hour * 60 + now.minute
    for name, (start, end) in (("London", LONDON_KILLZONE), ("New York", NY_KILLZONE)):
        sh, sm = _parse_hhmm(start)
        eh, em = _parse_hhmm(end)
        start_minutes = sh * 60 + sm
        end_minutes = eh * 60 + em
        if start_minutes <= current_minutes <= end_minutes:
            return name
    return None


DIRECTION_LABELS = {"LONG": "PUMP", "SHORT": "DUMP"}  # belső irány-kód -> megjelenített szöveg
SIGNAL_TYPE_LABELS = {
    "STANDARD": "Standard",
    "RANGE_BREAKOUT": "Sáv kitörés",
    "EMA_SQUEEZE": "EMA Squeeze",
}  # A/B teszthez


def format_scalp_message(symbol, direction, price, price_change_pct,
                          candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct,
                          htf_trend=None, bounce_confluence=False, near_level_risk=False,
                          rsi=None, macd_status=None, signal_type="STANDARD",
                          funding_rate=None, now=None):
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "RANGE_BREAKOUT":
        header = f"🎯 SÁV KITÖRÉS ({action}): <b>{symbol}</b>"
    elif signal_type == "EMA_SQUEEZE":
        header = f"🗜️ EMA SQUEEZE KITÖRÉS ({action}): <b>{symbol}</b>"
    else:
        header = f"⚡ STANDARD {action}: <b>{symbol}</b>"

    warning_line = ""
    against_trend = (
        (direction == "LONG" and htf_trend == "DOWN")
        or (direction == "SHORT" and htf_trend == "UP")
    )
    if against_trend:
        warning_line = f"\n⚠️ Trenddel szemben (1h: {htf_trend})"

    bounce_line = ""
    if bounce_confluence:
        level_type = "támaszról" if direction == "LONG" else "ellenállásról"
        bounce_line = f"\n🎯 Szint-visszapattanás ({level_type}, {SR_LOOKBACK_PERIOD}h-s csatorna)"

    risk_line = ""
    if near_level_risk:
        level_type = "ellenállás" if direction == "LONG" else "támasz"
        risk_line = f"\n⚠️ Közeli {level_type} ({SR_LOOKBACK_PERIOD}h-s csatorna) - onnan visszapattanhat!"

    # ÚJ: RSI/MACD infósor - csak tájékoztat, semmit nem szűr.
    indicator_line = ""
    if rsi is not None or macd_status is not None:
        parts = []
        if rsi is not None:
            rsi_note = ""
            if rsi >= RSI_OVERBOUGHT:
                rsi_note = " (túlvett)"
            elif rsi <= RSI_OVERSOLD:
                rsi_note = " (túladott)"
            parts.append(f"RSI: {rsi:.1f}{rsi_note}")
        if macd_status is not None:
            parts.append(f"MACD: {macd_status}")
        indicator_line = f"\n📐 {' | '.join(parts)}"

    # ÚJ: Funding Rate sor + Squeeze figyelmeztetés (Squeeze Vadász).
    funding_line = ""
    if funding_rate is not None:
        funding_line = f"\n💸 Funding: {funding_rate:+.4f}%"
        if direction == "LONG" and funding_rate <= -FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 SHORT SQUEEZE (Túl sok a shortos!)"
        elif direction == "SHORT" and funding_rate >= FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 LONG SQUEEZE (Túl sok a longos!)"

    # ÚJ: Killzone (tőzsdenyitási időablak) sor.
    killzone_line = ""
    if now is not None:
        active_kz = get_active_killzone(now)
        if active_kz:
            killzone_line = f"\n⏰ Időszak: {active_kz} Killzone"

    body = (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{indicator_line}"
        f"{funding_line}"
        f"{warning_line}"
        f"{bounce_line}"
        f"{risk_line}"
        f"{killzone_line}"
    )
    # ÚJ (szellős dizájn): extra sortörés az elején és a végén, hogy a
    # Telegramon a riasztások ne folyjanak össze.
    return f"\n{body}\n"


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

def compute_rsi_macd(close_series: pd.Series):
    """RSI(14) és MACD(12,26,9) számítása a TELJES sorozaton (lezárt gyertyák +
    élő gyertya) - ez tudatosan a szkript "korai jelzés" filozófiáját követi:
    az RSI/MACD is a jelenleg formálódó mozgást tükrözi, nem csak a múltat.
    Csak INFÓ, nem szűr semmit - ha nincs elég adat, egyszerűen None-t ad
    vissza, és az üzenet kihagyja ezt a sort."""
    if len(close_series) < 35:
        return None, None

    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi_series = 100 - (100 / (1 + rs))
    # Ha avg_loss pontosan 0 (tiszta, megszakítás nélküli emelkedés), a fenti
    # osztás NaN-t ad, holott ez a helyzet elméletileg RSI=100-at jelent.
    rsi_series = rsi_series.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    rsi_val = rsi_series.iloc[-1]
    rsi_val = round(float(rsi_val), 1) if pd.notna(rsi_val) else None

    ema12 = close_series.ewm(span=12, adjust=False).mean()
    ema26 = close_series.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()

    macd_status = None
    if len(macd_line) >= 2 and not macd_line.iloc[-2:].isna().any():
        prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
        curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
        if prev_diff < 0 and curr_diff > 0:
            macd_status = "Bullish Cross"
        elif prev_diff > 0 and curr_diff < 0:
            macd_status = "Bearish Cross"
        elif curr_diff > 0:
            macd_status = "Bullish"
        else:
            macd_status = "Bearish"

    return rsi_val, macd_status


# ----------------------------------------------------------------------------
# ÚJ: EMA SQUEEZE (BESZORULÁS) DETEKTÁLÁS - önálló, a STANDARD/SÁV KITÖRÉS
# jelzésektől teljesen független logika. Ugyanazokból a klines adatokból
# számol, nincs plusz API-hívás.
# Feltétel 1 (szorítás): az utolsó EMA_SQUEEZE_LOOKBACK_CANDLES db LEZÁRT
#   gyertya High/Low-ja szorosan az EMA20-EMA50 sáv körül mozgott, ÉS a két
#   EMA távolsága egymástól legfeljebb EMA_SQUEEZE_MAX_EMA_GAP_PCT %.
# Feltétel 2 (kitörés): az ÉLŐ gyertya ára határozottan kitör ebből a
#   csatornából - a két EMA fölé/alá ÉS a lookback-ablak korábbi csúcsa/
#   mélypontja fölé/alá is.
# ----------------------------------------------------------------------------

def detect_ema_squeeze(closed: pd.DataFrame, live: pd.Series):
    """Visszaadja (irány vagy None, ema_gap_pct vagy None) párost."""
    if len(closed) < EMA_SQUEEZE_SLOW_PERIOD + EMA_SQUEEZE_LOOKBACK_CANDLES:
        return None, None

    ema_fast = closed["close"].ewm(span=EMA_SQUEEZE_FAST_PERIOD, adjust=False).mean()
    ema_slow = closed["close"].ewm(span=EMA_SQUEEZE_SLOW_PERIOD, adjust=False).mean()
    last_fast = ema_fast.iloc[-1]
    last_slow = ema_slow.iloc[-1]
    if pd.isna(last_fast) or pd.isna(last_slow) or last_slow <= 0:
        return None, None

    ema_gap_pct = abs(last_fast - last_slow) / last_slow * 100
    if ema_gap_pct > EMA_SQUEEZE_MAX_EMA_GAP_PCT:
        return None, round(float(ema_gap_pct), 2)  # nincs szorítás - de a gap-et infónak visszaadjuk

    lookback = closed.iloc[-EMA_SQUEEZE_LOOKBACK_CANDLES:]
    band_low = min(last_fast, last_slow)
    band_high = max(last_fast, last_slow)
    # kis tolerancia az EMA-sáv "érintésére" - a sáv szélességének fele,
    # minimum a záróár 0.2%-a (hogy egy szinte 0 szélességű sávnál se legyen
    # a tolerancia nulla).
    tolerance = max((band_high - band_low) * 0.5, band_high * 0.002)
    channel_low = band_low - tolerance
    channel_high = band_high + tolerance

    prior_high = float(lookback["high"].max())
    prior_low = float(lookback["low"].min())
    is_tight = (prior_high <= channel_high) and (prior_low >= channel_low)
    if not is_tight:
        return None, round(float(ema_gap_pct), 2)

    current_price = float(live["close"])
    breakout_up = current_price > band_high and current_price > prior_high
    breakout_down = current_price < band_low and current_price < prior_low

    if breakout_up:
        return "LONG", round(float(ema_gap_pct), 2)
    if breakout_down:
        return "SHORT", round(float(ema_gap_pct), 2)
    return None, round(float(ema_gap_pct), 2)


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

    rsi_val, macd_status = compute_rsi_macd(kdf["close"])

    # --- ÚJ: sáv-beszűkülés (range compression) + kitörés detektálása ---
    # Az élő gyertyát megelőző RANGE_LOOKBACK_PERIOD db lezárt gyertya
    # sávszélessége, és hogy az élő ár most kitör-e ebből a sávból.
    range_window = closed.iloc[-RANGE_LOOKBACK_PERIOD:]
    signal_type = "STANDARD"
    range_width_pct = None
    if len(range_window) >= RANGE_LOOKBACK_PERIOD:
        range_high = float(range_window["high"].max())
        range_low = float(range_window["low"].min())
        if range_low > 0:
            range_width_pct = (range_high - range_low) / range_low * 100
            is_tight_range = range_width_pct <= RANGE_COMPRESSION_THRESHOLD_PCT
            is_breakout = (
                (direction == "LONG" and current_price > range_high)
                or (direction == "SHORT" and current_price < range_low)
            )
            if is_tight_range and is_breakout:
                signal_type = "RANGE_BREAKOUT"

    # ÚJ: EMA Squeeze (beszorulás) kitörés-jelzés - önálló, kiegészítő infó.
    ema_squeeze_signal, ema_gap_pct = detect_ema_squeeze(closed, live)

    return {
        "price": current_price,
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
        "direction": direction,
        "candle_open_ts": live["timestamp"].isoformat(),
        "rsi": rsi_val,
        "macd_status": macd_status,
        "signal_type": signal_type,
        "range_width_pct": round(range_width_pct, 2) if range_width_pct is not None else None,
        "ema_squeeze_signal": ema_squeeze_signal,
        "ema_gap_pct": ema_gap_pct,
    }

# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR (a belső 30 mp-es ciklus egy "üteme")
# ----------------------------------------------------------------------------

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            print("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, htf_cache

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
        missing_htf = [s for s in candidates if s not in htf_cache]

        # JAVÍTÁS: korábban az OI, a gyertyák és a HTF-trend lekérdezése 3
        # EGYMÁS UTÁNI (szekvenciális) await-blokkban történt, ami feleslegesen
        # megnyújtotta a kör futásidejét - főleg az első körben, amikor a teljes
        # HTF-cache még üres. Mostantól mindhárom EGYSZERRE, egy közös
        # gather()-ben fut, a MAX_CONCURRENT_REQUESTS szemafor így is korlátozza
        # az egyidejű valós hálózati kéréseket, csak nem kell egymásra várniuk.
        # ÚJ: a Funding Rate lekérdezése is ugyanebbe a gather()-be került, hogy
        # párhuzamosan fusson és ne lassítsa a kört.
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, semaphore, s) for s in missing_htf]
        funding_tasks = [fetch_funding_rate(session, semaphore, s) for s in candidates]

        oi_results, kline_results, htf_results, funding_results = await asyncio.gather(
            asyncio.gather(*oi_tasks),
            asyncio.gather(*kline_tasks),
            asyncio.gather(*htf_tasks),
            asyncio.gather(*funding_tasks),
        )

        if htf_results:
            for s, htf_data in htf_results:
                if htf_data is not None and htf_data.get("trend") is not None:
                    htf_cache[s] = htf_data

    oi_map = {s: oi for s, oi in oi_results if oi is not None}
    klines_map = {s: df for s, df in kline_results if df is not None}
    funding_map = {s: fr for s, fr in funding_results if fr is not None}

    alerts_sent = 0
    evaluated = 0
    htf_warned = 0
    sr_warned = 0
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
        funding_rate = funding_map.get(symbol)

        # --- v6: a magasabb idősík trendje NEM blokkol, csak figyelmeztető
        # sort kap az üzenet, ha a jelzés a trenddel szemben megy. ---
        htf_data = htf_cache.get(symbol, {})
        htf_trend = htf_data.get("trend")
        support = htf_data.get("support")
        resistance = htf_data.get("resistance")

        against_trend = REQUIRE_HTF_ALIGNMENT and (
            (candle["direction"] == "LONG" and htf_trend == "DOWN")
            or (candle["direction"] == "SHORT" and htf_trend == "UP")
        )

        # --- v8: a támasz/ellenállás-közelség MOSTANTÓL SEM blokkol (ahogy a
        # HTF trend sem) - csak figyelmeztető / kiemelő sort kap az üzenet.
        # near_level_risk: a jelzés a szint ellen menne (DUMP a támasznál,
        #   PUMP az ellenállásnál) -> ⚠️ figyelmeztetés, de a jelzés KIMEGY.
        # bounce_confluence: a jelzés a szintről való visszapattanással
        #   egyezik (PUMP a támasznál, DUMP az ellenállásnál) -> 🎯 kiemelés.
        price = candle["price"]
        near_support = support is not None and support > 0 and abs(price - support) / support * 100 <= SR_PROXIMITY_PCT
        near_resistance = resistance is not None and resistance > 0 and abs(price - resistance) / resistance * 100 <= SR_PROXIMITY_PCT

        near_level_risk = (
            (candle["direction"] == "LONG" and near_resistance)
            or (candle["direction"] == "SHORT" and near_support)
        )
        bounce_confluence = (
            (candle["direction"] == "LONG" and near_support)
            or (candle["direction"] == "SHORT" and near_resistance)
        )

        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= MIN_OI_INCREASE
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        if against_trend:
            htf_warned += 1
        if near_level_risk:
            sr_warned += 1

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
                bounce_confluence=bounce_confluence, near_level_risk=near_level_risk,
                rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                signal_type=candle.get("signal_type", "STANDARD"),
                funding_rate=funding_rate, now=now,
            )
            send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            trend_note = " ⚠️ TRENDDEL SZEMBEN" if against_trend else ""
            bounce_note = " 🎯 SZINT-VISSZAPATTANÁS" if bounce_confluence else ""
            type_note = f" [{candle.get('signal_type', 'STANDARD')}]"
            print(f"JELZÉS küldve: {symbol} [{candle['direction']}]{type_note} (Ár {candle['price_change_pct']:+.2f}%, "
                  f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%, "
                  f"1h trend: {htf_trend or 'ismeretlen'}){trend_note}{bounce_note}")

        # --- ÚJ: EMA SQUEEZE (beszorulás) - önálló, a fenti STANDARD/SÁV
        # KITÖRÉS jelzéstől TELJESEN FÜGGETLEN riasztás, saját cooldown-nal,
        # lazább volumen/OI küszöbökkel (a szoros EMA-csatorna kitörése
        # önmagában is erős setup). ---
        ema_signal = candle.get("ema_squeeze_signal")
        if ema_signal is not None:
            ema_is_setup = (
                oi_change_pct >= EMA_SQUEEZE_MIN_OI_INCREASE
                and candle["vol_multiplier"] >= EMA_SQUEEZE_MIN_VOL_MULTIPLIER
                and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
            )
            ema_cooldown_ok = True
            if entry.get("last_ema_squeeze_alert_ts"):
                last_ema_dt = datetime.fromisoformat(entry["last_ema_squeeze_alert_ts"])
                if (now - last_ema_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    ema_cooldown_ok = False

            if ema_is_setup and ema_cooldown_ok:
                ema_msg = format_scalp_message(
                    symbol, ema_signal, candle["price"], candle["price_change_pct"],
                    candle["candle_vol_usdt"], candle["vol_multiplier"],
                    oi_now, oi_change_pct, htf_trend=htf_trend,
                    rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                    signal_type="EMA_SQUEEZE",
                    funding_rate=funding_rate, now=now,
                )
                send_telegram_message(ema_msg)
                entry["last_ema_squeeze_alert_ts"] = now.isoformat()
                alerts_sent += 1
                print(f"JELZÉS küldve: {symbol} [{ema_signal}] [EMA_SQUEEZE] (EMA gap {candle.get('ema_gap_pct')}%, "
                      f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%)")

    if htf_warned:
        print(f"  (ebben a körben {htf_warned} kiküldött jelzés ment trenddel szemben - figyelmeztetéssel)")
    if sr_warned:
        print(f"  (ebben a körben {sr_warned} kiküldött jelzés ment támasz/ellenállás ellen - figyelmeztetéssel)")

    return alerts_sent, evaluated, valid_contracts, htf_cache

# ----------------------------------------------------------------------------
# FŐ CIKLUS - kb. 4.5 percig fut, 30 mp-enként újra kiértékelve
# ----------------------------------------------------------------------------

# --- ÚJ: alkalmazás-szintű zár (a GitHub Actions "concurrency" beállítása
# önmagában NEM garantálja 100%-ban, hogy két futás sose fusson egyszerre -
# ez egy plusz védelmi réteg, ami a state fájlban tárolt "zár" időbélyeg
# alapján, magában a Python kódban akadályozza meg az átfedést). ---
RUN_LOCK_STALE_MINUTES = (TOTAL_RUN_BUDGET_SECONDS / 60) + 2  # ha ennél régebbi a zár, "beragadtnak" tekintjük és felülírjuk


async def main():
    state = load_state()
    now_start = datetime.now(timezone.utc)

    # --- Zár ellenőrzése: fut-e már másik példány? ---
    existing_lock = state.get("_run_lock")
    if existing_lock:
        try:
            lock_age_minutes = (now_start - datetime.fromisoformat(existing_lock)).total_seconds() / 60
        except (ValueError, TypeError):
            lock_age_minutes = None
        if lock_age_minutes is not None and lock_age_minutes < RUN_LOCK_STALE_MINUTES:
            print(f"Egy másik futás már aktívnak tűnik (zár kora: {lock_age_minutes:.1f} perc) - "
                  f"ez a példány csendben kilép, hogy elkerüljük az átfedést/dupla riasztást.")
            return
        else:
            print("A talált zár elavultnak (beragadtnak) tűnik - felülírjuk és folytatjuk.")

    state["_run_lock"] = now_start.isoformat()
    save_state(state)  # azonnal mentjük, hogy egy majdnem egyidőben induló futás is lássa

    try:
        await _run_main_loop(state)
    finally:
        # A zárat MINDIG feloldjuk, még hiba esetén is, hogy ne ragadjon be véglegesen.
        state["_run_lock"] = None
        save_state(state)


async def _run_main_loop(state: dict):
    loop_start = time.monotonic()
    valid_contracts = None
    htf_cache = {}   # symbol -> {"trend":..., "support":..., "resistance":...}, futáson belül újrahasznosítva
    pass_num = 0
    total_alerts = 0

    while True:
        elapsed_total = time.monotonic() - loop_start
        if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
            break

        pass_num += 1
        pass_start = time.monotonic()
        now = datetime.now(timezone.utc)

        # Biztonsági időkorlát: egyetlen kör se futhat a hátralévő budget-nél
        # tovább (pl. ha a jelöltek száma megnő, vagy a BingX API lassan
        # válaszol) - így a szkript garantáltan időben, rendesen leáll.
        remaining_budget = max(30.0, TOTAL_RUN_BUDGET_SECONDS - elapsed_total)
        try:
            alerts, evaluated, valid_contracts, htf_cache = await asyncio.wait_for(
                run_single_pass(state, valid_contracts, htf_cache, now),
                timeout=remaining_budget,
            )
        except asyncio.TimeoutError:
            print(f"[{pass_num}. kör] Túllépte az időkeretet ({remaining_budget:.0f} mp), megszakítva. "
                  f"A state addig elért állapotát elmentjük, a ciklus leáll.")
            save_state(state)
            break

        total_alerts += alerts
        save_state(state)  # minden kör után mentünk, ne vesszen el adat félbeszakadás esetén

        print(f"[{pass_num}. kör] {evaluated} pár kiértékelve, {alerts} riasztás "
              f"(összesen eddig: {total_alerts} riasztás).")

        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break

        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    print(f"Ciklus vége: {pass_num} kör lefutott, összesen {total_alerts} riasztás. "
          f"A szkript rendesen leáll - a következő külső cron-hívás friss példányt indít.")


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("FIGYELEM: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
              "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
