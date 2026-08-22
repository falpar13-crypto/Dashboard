"""
BingX Perpetual - "Élő Gyertya" Skalp Felhalmozás-figyelő (v18)
====================================================================
Önállóan fut (nem a Streamlit dashboard része), mindig az 5 PERCES
gyertyákat vizsgálja, a dashboard idősík-választójától FÜGGETLENÜL.

Röviden a jelenlegi logika:
  - Az élő (még nyitott) gyertyát hasonlítja a megelőző N db lezárt
    gyertya átlagához (ár, volumen, Open Interest) - így már a mozgás
    KIALAKULÁSA közben jelezhet, nem csak lezárás után.
  - Két jelzéstípust küld: STANDARD (a "felgyűlt" volumen/OI alapján) és
    EARLY (gyorsulás-alapú, a mozgás elején, lásd a fájlban lentebb az
    EARLY paraméterek blokk-kommentjét).
  - Kiegészítő (csak tájékoztató, NEM szűrő) infók: 1h HTF trend,
    támasz/ellenállás-közelség, RSI/MACD, funding rate (squeeze).
  - Napi winrate-összesítőt küld (fix -1.5%-os SL-lel szimulálva).
  - GitHub Actions-ből fut, belső 30 mp-es ciklusban kb. 8m40s-ig,
    hogy a fix indítási költség (checkout, csomagtelepítés) ne
    vesszen kárba minden egyes 10 perces cron-hívásnál.

A teljes, verziónkénti indoklástörténet a CHANGELOG.md fájlban van.
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

# ----------------------------------------------------------------------------
# LOGGOLÁS - eddig print()-ekkel ment minden kimenet, időbélyeg/szint nélkül.
# GitHub Actions logokban ez megnehezítette egy adott hiba visszakeresését.
# Mostantól logging modult használunk: időbélyeg + szint (INFO/WARNING/ERROR)
# minden sorban, és HIBA-jellegű üzenetek explicit logger.error()-ral mennek,
# hogy CI logfeldolgozó eszközökkel is könnyen szűrhetők legyenek.
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("alert_checker")

# ----------------------------------------------------------------------------
# 1) SKALP PARAMÉTEREK - fix (hardkódolt) globális változók
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "5m"      # a háttér-figyelő MINDIG ezt vizsgálja, a dashboard
                             # idősík-választójától teljesen függetlenül
CANDLE_DURATION_SECONDS = 300  # 5m gyertya hossza másodpercben - a pace-alapú
                                 # (EARLY) vetítéshez kell, lásd evaluate_candle()
MAX_PRICE_CHANGE = 3.0      # max. %-os ármozgás az élő gyertyában (a legutóbbi
                             # lezárt gyertya záróárához képest)
MIN_OI_INCREASE = 2.5       # v18 RÁNCFELVARRÁS: szigorú alapokra vissza -
                             # a bot mostantól KIZÁRÓLAG STANDARD jelzést küld,
                             # ehhez markánsan magasabb küszöb kell
MIN_CANDLE_VOL_USDT = 15_000  # az élő gyertya eddigi USDT-forgalmának minimuma

VOLUME_MA_PERIOD = 10       # ennyi megelőző LEZÁRT gyertya átlagához viszonyítunk
MIN_VOL_MULTIPLIER = 2.5    # v18 RÁNCFELVARRÁS: szigorú alapokra vissza (lásd fent)

# ----------------------------------------------------------------------------
# ÚJ: EARLY (gyorsulás-alapú) jelzés paraméterei
# ----------------------------------------------------------------------------
# A STANDARD jelzés definíció szerint csak akkor tud kimenni, ha a mozgás
# már "felhalmozódott" (a teljes eddigi gyertya-volumen eléri a 2.5x-öt) -
# ez sok esetben azt jelenti, hogy a mozgás nagy része már megtörtént, mire
# a jelzés kimegy. Az EARLY jelzés ehelyett a mozgás ÜTEMÉT (sebességét)
# nézi: ha a volumen/OI az elmúlt néhány másodpercben/percben SOKKAL
# gyorsabban nő, mint amit a teljes gyertyára extrapolálva várnánk, azt
# már a mozgás KIALAKULÁSA közben jelzi - jóval a STANDARD küszöb elérése
# előtt. Cserébe zajosabb (rövidebb mérési ablak), ezért szigorúbb OI- és
# pace-küszöböt igényel, és csak akkor tüzel, ha a STANDARD (még) nem
# tüzelt ugyanarra a mozgásra (lásd is_setup_early a fő ciklusban).
EARLY_MIN_PACE_VOL_MULT = 5.0    # a TELJES gyertyára vetített volumen-szorzó
                                   # küszöbe (magasabb, mint a STANDARD 2.5x-e,
                                   # mert ez egy zajosabb, korai becslés)
EARLY_MIN_ELAPSED_FRACTION = 0.07  # kb. 20 mp - ennél korábban túl zajos a mérés
EARLY_MAX_ELAPSED_FRACTION = 0.6   # a gyertya 60%-a után már nincs sok előnye
                                     # az EARLY jelzésnek a STANDARD-hoz képest -
                                     # onnantól inkább hagyjuk, hogy a STANDARD
                                     # logika döntsön
EARLY_MIN_CANDLE_VOL_USDT = 8_000  # alacsonyabb, mint a STANDARD-nál (MIN_CANDLE_VOL_USDT),
                                     # hiszen itt még csak a gyertya elején/közepén
                                     # tartunk - kevesebb abszolút volumen várható
# --- OI "gyors ablak" - lásd find_oi_baseline() opcionális paraméterei ---
OI_FAST_TARGET_WINDOW_MINUTES = 2
OI_FAST_MIN_WINDOW_MINUTES = 1
OI_FAST_MAX_WINDOW_MINUTES = 4
EARLY_MIN_OI_FAST_INCREASE = 1.5   # az OI ennyi %-kal nőjön a "gyors" (kb. 2
                                     # perces) ablakban - alacsonyabb abszolút
                                     # szám, mint a STANDARD MIN_OI_INCREASE
                                     # (2.5%), de jóval RÖVIDEBB idő alatt, tehát
                                     # gyorsabb ütemet jelent

# --- ÚJ: Funding Rate (Squeeze Vadász) - négyezredszázalékos küszöb (-0.01% / +0.01%) ---
FUNDING_SQUEEZE_THRESHOLD_PCT = 0.01

# v18 RÁNCFELVARRÁS: az EMA_SQUEEZE és EMA_REJECTION jelzéstípusok (és a
# RANGE_BREAKOUT címkézés) teljesen törölve - a bot mostantól KIZÁRÓLAG a
# ⚡ STANDARD PUMP/DUMP jelzést küldi, szigorúbb küszöbökkel (lásd
# MIN_OI_INCREASE / MIN_VOL_MULTIPLIER fentebb).

# --- ÚJ (v3): belső ciklus időzítése egy GitHub Actions futáson belül ---
TOTAL_RUN_BUDGET_SECONDS = 520   # v15e: 280 -> 520 (~8m40s). A cron-job.org csak
                                  # kerek (5/10/15...) perces intervallumot enged,
                                  # 6 perc nem választható - ezért 10 PERCES külső
                                  # cron-ütemezésre álltunk át (lásd a válaszban).
                                  # FONTOS BELÁTÁS: a körítés (~31s) és a biztonsági
                                  # tartalék (~45s) MINDEN egyes indításnál
                                  # felemésztődik, függetlenül az intervallum
                                  # hosszától - ezért RITKÁBB, DE HOSSZABB futás
                                  # (10 perc) ÖSSZESSÉGÉBEN KEVESEBB "vak" időt ad,
                                  # mint a gyakoribb, rövidebb (5 perc): óránként
                                  # feleannyiszor kell megfizetni a fix körítési
                                  # költséget. 600s ablak - ~31s körítés - ~45s
                                  # tartalék ≈ 520s.
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

# --- "Kis/közepes market cap altcoin" előszűrés ---
# v15: MIN 500k -> 1M (a legilliquidebb mikrocapok kiszűrése), MAX 15M -> 150M
# (sokkal szélesebb sáv, hogy a nagyobb, likvidebb altcoinok is bekerüljenek
# a jelöltlistába) - a felhasználó kérésére.
MIN_VOLUME_USDT = 1_000_000
MAX_VOLUME_USDT = 150_000_000

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

MAX_CONCURRENT_REQUESTS = 16   # v15: 12 -> 16, mert a MAX_VOLUME_USDT nagy

# ÚJ: KÜLÖN, SOKKAL SZIGORÚBB szemafor kizárólag a klines-endpointra
# (fetch_klines + fetch_htf_trend + resolve_pending_signals is ide hívnak).
# A logokból kiderült, hogy NEM az általános kérésszám (OI, funding, ticker)
# váltja ki a "trigger frequency limit" (code 100410) tiltást, hanem
# KIFEJEZETTEN a klines-endpoint - miközben az OI/funding/ticker más
# endpointokat hív, azok sosem futottak bele ebbe a hibába. A korábbi közös,
# 16-os szemafor emiatt körönként ~500 klines-kérést zúdított ki nagyjából
# egyszerre (a HTF- és candidate-lekérések is ugyanabba a szemaforba
# tartoztak) - ez SZINTE MINDEN körben azonnal újra kiváltotta a tiltást,
# ami miatt egy teljes futás gyakorlatilag adatlekérés nélkül, néma
# csendben pörgött (lásd a felhasználó által küldött logrészletet: "0 pár
# kiértékelve" 8+ egymást követő körön át). A klines-hívásokat ezért
# jóval szűkebb, önálló szemaforral és lassabb ütemezéssel korlátozzuk,
# függetlenül az OI/funding/ticker hívásoktól, amik továbbra is a
# tágabb MAX_CONCURRENT_REQUESTS szemafort használják.
KLINES_MAX_CONCURRENT_REQUESTS = 4
KLINES_REQUEST_PACING_SECONDS = 0.2  # minden klines-kérés UTÁN ennyit várunk,
                                       # mielőtt a szemafor felszabadítaná a
                                       # helyet a következő kérésnek - ez az
                                       # tényleges kérés/mp ütemet fogja vissza,
                                       # amit önmagában a konkurrencia-korlát
                                       # nem feltétlenül tesz meg (gyors
                                       # válaszidőknél a 4-es konkurrencia is
                                       # sok kérést engedne át másodpercenként)
                                # emelése (15M -> 150M) várhatóan jelentősen
                                # megnöveli a jelöltlista méretét - érdemes az
                                # első pár futást figyelni (lásd a válaszban)
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

# ÚJ: HTF (1h) klines-lekérdezés szakaszolása. Korábban egy friss futás
# ELSŐ körében az ÖSSZES jelölt szimbólumhoz EGYSZERRE (a szemafor által
# csak 16-osával korlátozva, de gyakorlatilag egy nagy csomagban) elindult
# a HTF-trend lekérés - ez a burst rendszeresen belefutott a BingX
# "endpoint trigger frequency limit" (code 100410) hibájába, ami percekre
# letiltja az EGÉSZ klines-endpointot minden szimbólumra, nem csak arra,
# amelyik túllépte a limitet. Mostantól körönként csak ennyi ÚJ (még nem
# cache-elt) szimbólum HTF-trendjét kérjük le - a többi a következő
# köveztő körökben pótlódik, mire minden szimbólum cache-elve lesz.
HTF_FETCH_BATCH_SIZE = 20

# ÚJ: endpoint-szintű "hűtési" nyilvántartás. A BingX code=100410 válasza
# nem egy adott szimbólumra, hanem az EGÉSZ endpointra vonatkozó, percekig
# tartó tiltást jelent, és a válasz üzenete tartalmazza a pontos feloldási
# időt (epoch ms). Ha ezt figyelmen kívül hagynánk és minden szimbólumnál
# újra és újra megpróbálnánk (mint korábban), az csak feleslegesen
# terhelné az API-t, és minden egyes hívás úgyis elbukna a tiltás alatt.
# Ehelyett az első ütközéskor megjegyezzük, MEDDIG tilos ezt az endpointot
# hívni, és addig a további hívásokat AZONNAL, hálózati kérés nélkül
# elutasítjuk - ezzel is segítve, hogy a tiltás minél hamarabb feloldódjon.
_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150  # biztonsági felső korlát, ha a válaszból
                                       # valamiért irreálisan távoli/hibás
                                       # időpontot olvasnánk ki
# Kell: 1 élő (nyitott) + VOLUME_MA_PERIOD lezárt gyertya a baseline-hoz, PLUSZ
# elég előzmény egy stabil RSI(14)/MACD(12,26,9) számításához (~35-40 minimum),
# PLUSZ (v11) elég előzmény egy stabilabb 5m EMA(20)/EMA(50) (EMA Squeeze)
# számításához - span=50-nél minél több adat, annál jobban "bemelegszik" az
# EMA. 120 gyertya kb. 10 óra 5m adatot ad, jó kompromisszum a pontosság és a
# lekérdezés mérete/sebessége között (korábban 65 volt, ami az EMA(50)-hez
# kissé szűkös).
KLINES_LIMIT = 120

# --- ÚJ: RSI infó-küszöbök (csak megjelenítés, NEM szűr - a felhasználó kérésére) ---
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ----------------------------------------------------------------------------
# ÚJ (v14): NAPI WINRATE-KÖVETÉS
# ----------------------------------------------------------------------------
# Minden kiküldött jelzést (típustól függetlenül: STANDARD/SÁV KITÖRÉS/EMA
# SQUEEZE/EMA REJECTION) elmentünk egy "függőben lévő" listába (state-ben),
# a belépő árral és egy jövőbeli időponttal, amikor kiértékeljük. Amikor
# letelik ez az idő, a következő körben (ami már úgyis lekéri a ticker-
# árakat - NINCS plusz API-hívás) megnézzük, merre mozdult az ár a jelzés
# iránya szerint, és WIN/LOSS/NEUTRAL-t rendelünk hozzá. Ezt egy önálló,
# append-only naplófájlba (SIGNAL_LOG_FILE) írjuk. Minden nap - kicsit éjfél
# UTC után, hogy az előző nap utolsó jelzései is stabilan kiértékelődjenek -
# a bot egyetlen összesítő Telegram-üzenetben elküldi az előző nap
# jelzéstípusonkénti darabszámát, W/L/N bontását és winrate-jét.
SIGNAL_LOG_FILE = Path(__file__).parent / "alert_log.jsonl"

# v15: a NAPI ÖSSZESÍTŐ napváltása és küldési időzítése mostantól ezen a
# (helyi) időzónán alapul, NEM UTC-n - így "éjfél után" tényleg a te
# éjfélhez (nem UTC éjfélhez, ami nyáron 2, télen 1 órával később van) igazodik.
# Minden MÁS időbélyeg (entry_ts, exit_ts, cooldown stb.) a fájlban
# VÁLTOZATLANUL UTC marad - csak az összesítő naphatára/küldési idő lokális.
SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

# ----------------------------------------------------------------------------
# v18 RÁNCFELVARRÁS: EGYSZERŰSÍTETT, FIX SL-ES KIÉRTÉKELÉS
# ----------------------------------------------------------------------------
# A korábbi (v17) megoldás egy ATR + szerkezeti (swing) alapú, egyénileg
# számolt SL-t használt. Ezt a felhasználó kérésére EGYSZERŰSÍTETTÜK: mostantól
# egy FIX -1.5%-os elmozdulás jelenti a SL-t (nincs ATR/swing számítás).
#
# 1) OUTCOME_EVAL_WINDOW_MINUTES (60 perc) elteltével lekéri az 5 PERCES
#    GYERTYÁK teljes historikumát a belépéstől a kiértékelésig, és
#    végigsétál rajtuk időrendben:
#    - minden gyertyánál frissíti a legjobb elért %-os elmozdulást (a gyertya
#      KEDVEZŐ irányú High/Low-ja alapján), és megjelöli, mely
#      OUTCOME_PROFIT_LEVELS_PCT szinteket (+0.5%, +1%, +2%, +3%) érte el
#      ADDIG a pontig
#    - UTÁNA ellenőrzi, hogy a gyertya KEDVEZŐTLEN irányú High/Low-ja
#      beütötte-e a FIX -1.5%-os SL-t - ha igen, a szimuláció itt megáll
#    DOKUMENTÁLT EGYSZERŰSÍTÉS: tick-szintű adat nélkül nem tudjuk biztosan,
#    hogy egy gyertyán belül a kedvező vagy a kedvezőtlen irányú mozgás
#    történt-e előbb. A fenti sorrend (kedvező -> SL) egy ENYHÉN OPTIMISTA
#    konvenció - a "valós piaci potenciál" bemutatásához megfelelő, de egy
#    szigorúan konzervatív (tick-pontos) szimuláció ennél valamivel rosszabb
#    számokat adna.
# 2) Eredmény: SL-találati arány + minden profitszinthez tartozó "elérte SL
#    előtt" arány a napi összesítőben (kizárólag STANDARD jelzésekre).
# ----------------------------------------------------------------------------
OUTCOME_EVAL_WINDOW_MINUTES = 60      # ennyi idő (5 perces gyertya-historikum)
                                       # alapján értékelünk ki egy jelzést
OUTCOME_FIXED_SL_PCT = 1.5            # FIX stop-loss: a jelzés irányával
                                       # ELLENTÉTES irányú ennyi %-os elmozdulás
OUTCOME_PROFIT_LEVELS_PCT = [0.5, 1.0, 2.0, 3.0]  # ezeket a szinteket vizsgáljuk
OUTCOME_MAX_STALE_MINUTES = 60        # ha a kiértékelési ablak lejárta után ENNYI
                                       # idővel sem sikerül klines-adatot szerezni
                                       # a szimbólumhoz, UNKNOWN-ként lezárjuk

DAILY_SUMMARY_MIN_DELAY_MINUTES = 35  # ennyivel helyi éjfél után küldjük az
                                        # előző napi összesítőt


# ----------------------------------------------------------------------------
# TypedDict-ek - ÚJ, csak dokumentációs/típusellenőrzési célra (futásidőben
# semmit nem változtatnak, a kódban továbbra is sima dict-eket adunk vissza/
# kapunk). Korábban ezek a struktúrák "meztelen" dict-ekként éltek a
# kódban, ami olvashatóbb IDE-támogatás és statikus ellenőrzés (mypy/pyright)
# nélkül könnyen elgépelt kulcsnevekhez vezethetett (pl. "vol_multiplier" vs
# "volume_multiplier") anélkül, hogy ez futásidőben azonnal kiderült volna.
# ----------------------------------------------------------------------------
class CandleEval(TypedDict):
    price: float
    price_change_pct: float
    vol_multiplier: float
    candle_vol_usdt: float
    direction: str          # "LONG" | "SHORT"
    rsi: Optional[float]
    macd_status: Optional[str]
    signal_type: str        # "STANDARD" | "EARLY"
    elapsed_fraction: Optional[float]      # ÚJ: az élő gyertya hány hányada telt el (0-1)
    pace_vol_multiplier: Optional[float]   # ÚJ: a teljes gyertyára vetített volumen-szorzó


class OiBaseline(TypedDict):
    ts: str
    oi: float


def _rotate_signal_log(before_date_str: str) -> None:
    """ÚJ: napló-rotáció. Az alert_log.jsonl korábban append-only volt,
    rotáció nélkül - hónapok/évek alatt korlátlanul nőhetett. Mostantól a
    napi összesítő elküldése UTÁN (tehát csak azután, hogy a tegnapi
    jelzéseket már felhasználtuk!) minden `before_date_str`-nél (kizárólag)
    KORÁBBI 'entry_date'-ű sort kiemelünk a fő naplóból, és a hónapjuk
    szerinti archívum-fájlba (alert_log_YYYY-MM.jsonl.bak) fűzzük hozzá.
    Ez biztonságos: a kiértékelési ablak (OUTCOME_EVAL_WINDOW_MINUTES +
    OUTCOME_MAX_STALE_MINUTES, összesen legfeljebb 2 óra) miatt egy nappal
    régebbi bejegyzés MINDIG már véglegesen kiértékelt (WIN/LOSS/UNKNOWN),
    így archiválás után sem a resolve_pending_signals, sem a napi
    összesítő nem hivatkozik rá többé."""
    if not SIGNAL_LOG_FILE.exists():
        return
    try:
        keep_lines = []
        archive_by_month: dict[str, list[str]] = {}
        with SIGNAL_LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    rec = json.loads(stripped)
                except json.JSONDecodeError:
                    keep_lines.append(line)  # sérült sort inkább megtartjuk, mint elveszítjük
                    continue
                entry_date = rec.get("entry_date")
                if entry_date and entry_date < before_date_str:
                    month_key = entry_date[:7]  # "YYYY-MM"
                    archive_by_month.setdefault(month_key, []).append(line)
                else:
                    keep_lines.append(line)

        if not archive_by_month:
            return  # nincs mit archiválni

        for month_key, lines in archive_by_month.items():
            archive_path = SIGNAL_LOG_FILE.parent / f"alert_log_{month_key}.jsonl.bak"
            with archive_path.open("a", encoding="utf-8") as f:
                f.writelines(lines)

        tmp_path = SIGNAL_LOG_FILE.with_suffix(SIGNAL_LOG_FILE.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            f.writelines(keep_lines)
        os.replace(tmp_path, SIGNAL_LOG_FILE)

        archived_count = sum(len(v) for v in archive_by_month.values())
        logger.info("Napló-rotáció: %d régi bejegyzés archiválva (%d hónapos fájlba).",
                    archived_count, len(archive_by_month))
    except OSError as e:
        logger.error("Napló-rotáció sikertelen: %s", e)


def _log_signal_outcome(record: dict) -> None:
    """Egyetlen sort ír a napló (JSONL) fájlba - append-only, soha nem
    módosítunk/törlünk belőle korábbi sort."""
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error("Nem sikerült írni a jelzés-naplóba: %s", e)


def register_pending_signal(state: dict, symbol: str, signal_type: str,
                             direction: str, entry_price: float, now: datetime) -> None:
    """Egy most kiküldött jelzést berak a 'pending_outcomes' listába - ezt
    fogja a resolve_pending_signals() a megfelelő időben kiértékelni. A SL
    mostantól mindig a FIX OUTCOME_FIXED_SL_PCT (-1.5%), nem kell külön
    átadni/tárolni."""
    pending = state.setdefault("pending_outcomes", [])
    pending.append({
        "id": f"{symbol}_{signal_type}_{now.strftime('%Y%m%dT%H%M%S')}",
        "symbol": symbol,
        "signal_type": signal_type,
        "direction": direction,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "entry_date": now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d"),  # helyi (nem UTC)
                                                    # dátum - ehhez a (helyi) naphoz számít a napi
                                                    # összesítőben, függetlenül attól, mikor zárul
                                                    # le ténylegesen a jelzés kiértékelése
        "window_end_ts": (now + timedelta(minutes=OUTCOME_EVAL_WINDOW_MINUTES)).isoformat(),
    })


def _simulate_trade_outcome(direction: str, entry_price: float, candles: pd.DataFrame) -> dict:
    """Végigsétál az 5 perces gyertyákon (időrendben, a belépés utániakon), és
    szimulálja, mely profitszinteket érte el az ár a FIX -1.5%-os SL beütése
    ELŐTT. Lásd a fájl elején lévő blokk-kommentet a módszertanról és az
    egyszerűsítésről."""
    levels_reached = {lvl: False for lvl in OUTCOME_PROFIT_LEVELS_PCT}
    max_favorable_pct = 0.0
    sl_hit = False

    if direction == "LONG":
        sl_price = entry_price * (1 - OUTCOME_FIXED_SL_PCT / 100)
    else:  # SHORT
        sl_price = entry_price * (1 + OUTCOME_FIXED_SL_PCT / 100)

    for _, row in candles.iterrows():
        high = float(row["high"])
        low = float(row["low"])

        if direction == "LONG":
            favorable_extreme = high
            adverse_extreme = low
            favorable_pct = (favorable_extreme - entry_price) / entry_price * 100
        else:  # SHORT
            favorable_extreme = low
            adverse_extreme = high
            favorable_pct = (entry_price - favorable_extreme) / entry_price * 100

        if favorable_pct > max_favorable_pct:
            max_favorable_pct = favorable_pct
        for lvl in OUTCOME_PROFIT_LEVELS_PCT:
            if max_favorable_pct >= lvl:
                levels_reached[lvl] = True

        hit = (adverse_extreme <= sl_price) if direction == "LONG" else (adverse_extreme >= sl_price)
        if hit:
            sl_hit = True
            break

    return {
        "sl_hit": sl_hit,
        "max_favorable_pct": round(max_favorable_pct, 3),
        "levels_reached": {f"level_{lvl}pct": levels_reached[lvl] for lvl in OUTCOME_PROFIT_LEVELS_PCT},
    }


async def resolve_pending_signals(state: dict, session, klines_semaphore, now: datetime) -> None:
    """5 perces gyertya-historikum alapján lezárja azokat a függő
    jelzéseket, amelyeknél letelt a kiértékelési ablak
    (OUTCOME_EVAL_WINDOW_MINUTES). Jelzésenként EGYETLEN klines-lekérést
    végez, csak amikor tényleg esedékes - a köztes köröknél nincs plusz
    terhelés."""
    pending = state.get("pending_outcomes", [])
    if not pending:
        return

    due, still_pending = [], []
    for item in pending:
        try:
            window_end = datetime.fromisoformat(item["window_end_ts"])
        except (KeyError, ValueError):
            continue  # sérült bejegyzés eldobva
        (due if now >= window_end else still_pending).append(item)

    if not due:
        return

    async def _resolve_one(item):
        entry_dt = datetime.fromisoformat(item["entry_ts"])
        window_end_dt = datetime.fromisoformat(item["window_end_ts"])
        symbol, kdf = await fetch_klines(session, klines_semaphore, item["symbol"], ALERT_TIMEFRAME, limit=60)
        if kdf is None or kdf.empty:
            return item, None

        entry_naive = entry_dt.astimezone(timezone.utc).replace(tzinfo=None)
        window_end_naive = window_end_dt.astimezone(timezone.utc).replace(tzinfo=None)
        window_candles = kdf[(kdf["timestamp"] >= entry_naive) & (kdf["timestamp"] <= window_end_naive)]
        if window_candles.empty:
            return item, None

        result = _simulate_trade_outcome(item["direction"], item["entry_price"], window_candles)
        return item, result

    results = await asyncio.gather(*[_resolve_one(item) for item in due], return_exceptions=True)

    for outcome_pair in results:
        if isinstance(outcome_pair, Exception):
            logger.error("Hiba a jelzés kiértékelése közben: %s", outcome_pair)
            continue
        item, result = outcome_pair
        if result is None:
            entry_dt = datetime.fromisoformat(item["entry_ts"])
            age_minutes = (now - entry_dt).total_seconds() / 60
            if age_minutes >= OUTCOME_EVAL_WINDOW_MINUTES + OUTCOME_MAX_STALE_MINUTES:
                _log_signal_outcome({**item, "outcome": "UNKNOWN", "sl_hit": None,
                                      "max_favorable_pct": None, "levels_reached": None,
                                      "resolved_ts": now.isoformat()})
            else:
                still_pending.append(item)  # próbáljuk később újra
            continue
        outcome_label = "LOSS" if result["sl_hit"] else "NEUTRAL"
        _log_signal_outcome({**item, **result, "outcome": outcome_label, "resolved_ts": now.isoformat()})

    state["pending_outcomes"] = still_pending


def _load_log_entries_for_date(date_str: str) -> list:
    """Beolvassa a napló azon sorait, amelyek 'entry_date' mezője a megadott
    (UTC) napra esik."""
    if not SIGNAL_LOG_FILE.exists():
        return []
    entries = []
    try:
        with SIGNAL_LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue  # egy sérült sor ne dobja el az egész összesítőt
                if rec.get("entry_date") == date_str:
                    entries.append(rec)
    except OSError as e:
        logger.error("Nem sikerült beolvasni a jelzés-naplót: %s", e)
    return entries


def _format_daily_summary(date_str: str, entries: list) -> str:
    # ÚJ: az EARLY jelzéstípus visszahozatalával a napi összesítő ismét
    # TÍPUSONKÉNT bontva mutatja a statisztikát - enélkül nem lehetne
    # összehasonlítani, hogy a két logika közül melyik teljesít jobban
    # (ami pont a cél: eldönteni, megéri-e az EARLY jelzés a nagyobb
    # zajszintet).
    SIGNAL_TYPE_ORDER = ["STANDARD", "EARLY"]
    SIGNAL_TYPE_LABELS = {"STANDARD": "⚡ STANDARD", "EARLY": "🌱 EARLY (korai)"}

    lines = [
        f"📊 <b>Napi összesítő</b> ({date_str})",
        "(SL = -1.5%-os fix stop-loss beütött a kiértékelési ablakban; a %-ok",
        "azt mutatják, hány jelzés érte el az adott profitszintet a SL ELŐTT)",
        "━━━━━━━━━━━━━",
    ]

    if not entries:
        lines.append("Nem volt jelzés.")
        return f"\n{chr(10).join(lines)}\n"

    types_present = sorted(
        {r.get("signal_type", "STANDARD") for r in entries},
        key=lambda t: SIGNAL_TYPE_ORDER.index(t) if t in SIGNAL_TYPE_ORDER else 99,
    )

    for sig_type in types_present:
        type_entries = [r for r in entries if r.get("signal_type", "STANDARD") == sig_type]
        total = len(type_entries)
        resolved = [r for r in type_entries if r.get("outcome") != "UNKNOWN" and r.get("sl_hit") is not None]
        n = len(resolved)
        unknown = total - n
        label = SIGNAL_TYPE_LABELS.get(sig_type, sig_type)

        if n == 0:
            lines.append(f"{label}: {total} jelzés (nincs kiértékelhető adat)")
            lines.append("━━━━━━━━━━━━━")
            continue

        sl_hits = sum(1 for r in resolved if r.get("sl_hit"))
        sl_pct = sl_hits / n * 100
        lines.append(f"{label}: {total} jelzés{f' ({unknown} n/a)' if unknown else ''}")
        lines.append(f"SL beütve: {sl_hits}/{n} ({sl_pct:.0f}%)")
        for lvl in OUTCOME_PROFIT_LEVELS_PCT:
            key = f"level_{lvl}pct"
            hit = sum(1 for r in resolved if (r.get("levels_reached") or {}).get(key))
            lines.append(f"+{lvl}% elérve SL előtt: {hit}/{n} ({hit / n * 100:.0f}%)")
        lines.append("━━━━━━━━━━━━━")

    return f"\n{chr(10).join(lines)}\n"


STATE_CLEANUP_STALE_DAYS = 14  # ennyi napja nem látott (pl. delistázott)
                                 # szimbólum bejegyzését töröljük a state-ből


def _cleanup_stale_state_entries(state: dict, now: datetime) -> None:
    """ÚJ: a state fájl per-szimbólum bejegyzései korábban SOSEM törlődtek
    (csak a bennük lévő oi_history lista elemei öregedtek ki) - ha egy
    tokent delistáztak vagy kikerült a jelöltlistából, a bejegyzése
    örökre bent maradt, feleslegesen növelve a fájlt. Mostantól minden
    olyan szimbólum-bejegyzést törlünk, amit STATE_CLEANUP_STALE_DAYS
    napja nem láttunk (a run_single_pass minden kiértékelt szimbólumnál
    frissíti a "last_seen" mezőt). A belső ("_"-tal kezdődő) kulcsokat
    (pl. "_run_lock", "_last_summary_date") ez nem érinti."""
    cutoff = now - timedelta(days=STATE_CLEANUP_STALE_DAYS)
    stale_symbols = []
    for key, entry in state.items():
        if key.startswith("_") or not isinstance(entry, dict):
            continue
        last_seen = entry.get("last_seen")
        if last_seen is None:
            continue  # régebbi, "last_seen" mező nélküli bejegyzés - hagyjuk békén
        try:
            last_seen_dt = datetime.fromisoformat(last_seen)
        except ValueError:
            continue
        if last_seen_dt < cutoff:
            stale_symbols.append(key)

    for key in stale_symbols:
        del state[key]

    if stale_symbols:
        logger.info("State-takarítás: %d elavult (>%d napja nem látott) szimbólum törölve (%s).",
                    len(stale_symbols), STATE_CLEANUP_STALE_DAYS, ", ".join(stale_symbols[:10]))


async def maybe_send_daily_summary(state: dict, now: datetime) -> None:
    """Ha új (HELYI, SUMMARY_TIMEZONE szerinti) nap kezdődött, és eltelt
    DAILY_SUMMARY_MIN_DELAY_MINUTES perc a helyi éjfél óta (hogy az előző nap
    utolsó jelzései is kiértékelődjenek), elküldi az előző nap winrate-
    összesítőjét, majd megjegyzi state-ben, hogy a mai (helyi) napra már
    küldtünk (ne menjen ki még egyszer ugyanaznap)."""
    local_now = now.astimezone(SUMMARY_TIMEZONE)
    today_str = local_now.strftime("%Y-%m-%d")
    last_summary_date = state.get("_last_summary_date")

    if last_summary_date is None:
        # Első futás valaha - nincs mit összesíteni, csak megjegyezzük a mai
        # (helyi) napot, hogy holnap már legyen mihez képest "új nap".
        state["_last_summary_date"] = today_str
        return

    if last_summary_date == today_str:
        return  # ma már küldtünk (vagy még ugyanaz a helyi nap van) - nincs teendő

    local_midnight_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_local_midnight = (local_now - local_midnight_today).total_seconds() / 60
    if minutes_since_local_midnight < DAILY_SUMMARY_MIN_DELAY_MINUTES:
        return  # várunk még egy kicsit helyi éjfél után, hogy a tegnapi
                 # utolsó jelzések is stabilan kiértékelődjenek

    yesterday_str = (local_now - timedelta(days=1)).strftime("%Y-%m-%d")
    entries = _load_log_entries_for_date(yesterday_str)
    if entries:
        summary_msg = _format_daily_summary(yesterday_str, entries)
        await send_telegram_message(summary_msg)
        logger.info("Napi winrate-összesítő elküldve (%s, %d jelzés).", yesterday_str, len(entries))
    else:
        logger.info("Nem volt jelzés %s-n - napi összesítő kihagyva.", yesterday_str)

    state["_last_summary_date"] = today_str
    # A tegnapi (yesterday_str) bejegyzéseket már felhasználtuk fent - minden,
    # ami ennél is régebbi, biztonságosan archiválható.
    _rotate_signal_log(yesterday_str)
    _cleanup_stale_state_entries(state, now)

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
    """ÚJ: ATOMI írás. A korábbi write_text() közvetlenül az élő state
    fájlba írt - ha a folyamat pont írás KÖZBEN szakadt meg (pl. GitHub
    Actions timeout, OOM-kill, áramkimaradás), sérült/félbehagyott JSON
    maradhatott a lemezen, amit a load_state() csendben eldob és üres
    dict-ként kezel - ez ELVESZTI az összes cooldown-, OI-history- és
    pending_outcomes-adatot. Mostantól egy ideiglenes fájlba írunk, majd
    os.replace()-szel (POSIX-on atomi) cseréljük le vele az eredetit -
    így a state fájl minden pillanatban vagy a régi, vagy a teljesen új,
    érvényes tartalmat tartalmazza, sosem félkészet."""
    tmp_path = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, STATE_FILE)
    except OSError as e:
        logger.error("Nem sikerült elmenteni a state fájlt: %s", e)
        # Takarítás: ha a tmp fájl létrejött, de a replace elszállt, ne
        # maradjon ott árva ideiglenes fájl.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass

# ----------------------------------------------------------------------------
# BINGX API HÍVÁSOK
# ----------------------------------------------------------------------------

async def _get_json(session, url, params=None):
    """JAVÍTÁS: korábban MINDEN hibát csendben elnyelt (`except Exception:
    pass`), a konkrét okot sosem logolta - ha egy endpoint tartósan
    hibázott, ebből semmi nem látszott a logban, csak annyi, hogy "nincs
    adat". Mostantól minden sikertelen próbálkozásnál logoljuk a hiba
    típusát/üzenetét (utolsó próbálkozásnál WARNING szinten, hogy ne
    árasszon el a log, ha csak átmeneti hálózati hiba volt). Emellett a
    BingX válasz "code" mezőjét is ellenőrizzük: az API néha 200 OK
    HTTP-státusszal, de belső hibakóddal válaszol (pl. rossz szimbólum,
    rate-limit belső jelzése) - ezt korábban a resp.raise_for_status()
    nem vette észre, mert a HTTP réteg szintjén minden rendben volt.

    ÚJ: a code=100410 ("endpoint trigger frequency limit... disabled
    period") NEM egyetlen kérésre vonatkozó hiba, hanem az egész
    endpointra kirótt, percekig tartó tiltás - ezt a szokásos rövid
    RETRY_BACKOFF-fal újrapróbálni értelmetlen (garantáltan megint elbukik)
    és feleslegesen tovább terheli az amúgy is limitált endpointot. Ehelyett
    egyetlen próbálkozás után megjegyezzük a válaszból kiolvasott feloldási
    időt, és minden további, ugyanerre az endpointra irányuló hívást a
    tiltás lejártáig azonnal, hálózati kérés nélkül elutasítunk."""
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
                # BingX konvenció: code == 0 jelenti a sikert. Ha a mező
                # jelen van és nem 0, az API-szintű hibát jelez, annak
                # ellenére, hogy a HTTP-válasz 200 OK volt.
                if isinstance(data, dict) and data.get("code") not in (None, 0):
                    code = data.get("code")
                    msg = data.get("msg", "")
                    last_error = f"API code={code} msg={msg}"
                    if code == 100410:
                        # A válaszüzenet tartalmazza a feloldás epoch ms
                        # időpontját ("...unblocked after 1787381257499").
                        # Ezt kiolvassuk, és a teljes endpointot addig
                        # hűtjük - további próbálkozás itt és most nem segít.
                        wait_seconds = ENDPOINT_COOLDOWN_MAX_SECONDS
                        m = re.search(r"after (\d+)", msg)
                        if m:
                            unblock_epoch_ms = int(m.group(1))
                            wait_seconds = max(0.0, unblock_epoch_ms / 1000 - time.time())
                            wait_seconds = min(wait_seconds, ENDPOINT_COOLDOWN_MAX_SECONDS)
                        _ENDPOINT_COOLDOWN_UNTIL[endpoint_key] = time.monotonic() + wait_seconds
                        logger.warning(
                            "Endpoint hűtésre kényszerítve %.0f mp-re (code 100410) - %s",
                            wait_seconds, url,
                        )
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
            # ÚJ (v14): a "lastPrice" mezőt is eltároljuk - ez kell a NAPI
            # WINRATE ÖSSZESÍTŐHÖZ (lásd lent), hogy a korábban kiküldött
            # jelzések utóéletét (WIN/LOSS/NEUTRAL) tudjuk követni. Mivel ezt
            # a ticker-lekérést MINDEN körben úgyis lefuttatjuk, ez NEM jelent
            # plusz API-hívást.
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

        payload = data["data"]
        # VÉDEKEZÉS: néhány BingX végpont egyetlen szimbólumra is LISTÁT ad
        # vissza (nem csak dict-et) - ha ez történne itt is, az első elemet
        # vesszük. Ha a struktúra egyáltalán nem az elvárt típus, csendben
        # feladjuk erre a szimbólumra (nem szabad, hogy EGY váratlan válasz
        # miatt az egész kör/futás elszálljon).
        if isinstance(payload, list):
            payload = payload[0] if payload else None
        if not isinstance(payload, dict):
            return symbol, None

        try:
            # A pontos mezőnév BingX végpontonként eltérhet - több lehetséges
            # nevet is megpróbálunk, mielőtt feladnánk.
            raw = payload.get("lastFundingRate")
            if raw is None:
                raw = payload.get("fundingRate")
            if raw is None:
                return symbol, None
            return symbol, float(raw) * 100  # a BingX tizedes-törtet ad vissza -> %-ra váltjuk
        except (TypeError, ValueError):
            return symbol, None
        except Exception:
            # Bármilyen más, előre nem látott hiba esetén se dőljön el emiatt
            # az egész kör - egyszerűen nincs funding infó erre a szimbólumra
            # ebben a körben, a jelzés a hiányzó funding sor nélkül megy ki.
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
        # ÚJ: a korábbi 0.03 mp-es szünet túl rövid volt ahhoz, hogy
        # ténylegesen korlátozza a klines-endpointra irányuló kérés/mp
        # ütemet - lásd a KLINES_REQUEST_PACING_SECONDS fenti kommentjét.
        await asyncio.sleep(KLINES_REQUEST_PACING_SECONDS)
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
        # ÚJ: lásd KLINES_REQUEST_PACING_SECONDS fenti kommentjét - a 0.03 mp
        # túl rövid volt, nem korlátozta ténylegesen a kérés/mp ütemet.
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
# TELEGRAM ÉRTESÍTÉS
# ----------------------------------------------------------------------------

def _send_telegram_message_sync(text: str) -> None:
    """A tényleges (szinkron, requests-alapú) HTTP-hívás. NE hívd
    közvetlenül async kódból - lásd send_telegram_message()."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Hiányzik a TELEGRAM_BOT_TOKEN vagy TELEGRAM_CHAT_ID env változó.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error("Telegram hiba (%s): %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Telegram küldési hiba: %s", e)


async def send_telegram_message(text: str) -> None:
    """JAVÍTÁS: korábban ez egy szinkron `requests.post()` hívás volt,
    ami - annak ellenére, hogy egy asyncio event loop-ban futunk - a
    hívó szálat (és így a TELJES 30 mp-es ciklust) blokkolta a hálózati
    kérés idejére. Mostantól `asyncio.to_thread()`-del egy külön szálon
    fut, hogy a loop eközben más feladatokat (API-hívások a következő
    körhöz stb.) is végezhessen. A hívó oldalon ezért `await` szükséges -
    lásd a hívási pontokat lent."""
    await asyncio.to_thread(_send_telegram_message_sync, text)


DIRECTION_LABELS = {"LONG": "PUMP", "SHORT": "DUMP"}  # belső irány-kód -> megjelenített szöveg


def format_scalp_message(symbol, direction, price, price_change_pct,
                          candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct,
                          htf_trend=None, bounce_confluence=False, near_level_risk=False,
                          rsi=None, macd_status=None, signal_type="STANDARD",
                          funding_rate=None,
                          pace_vol_multiplier=None, elapsed_fraction=None):
    # v18 RÁNCFELVARRÁS: a bot mostantól KIZÁRÓLAG ⚡ STANDARD PUMP/DUMP
    # jelzést küld - a RANGE_BREAKOUT/EMA_SQUEEZE/EMA_REJECTION fejléc-ágak
    # törölve.
    # ÚJ: EARLY (gyorsulás-alapú) jelzéstípus visszahozva, más fejléccel és
    # egy figyelmeztető sorral - lásd az EARLY paraméterek blokk-kommentjét.
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "EARLY":
        header = f"🌱 <b>{symbol}</b> {action} (KORAI)"
    else:
        header = f"⚡ <b>{symbol}</b> {action}"

    early_line = ""
    if signal_type == "EARLY":
        pace_note = f", vetített ütem: {pace_vol_multiplier:.1f}x" if pace_vol_multiplier is not None else ""
        elapsed_note = f" (a gyertya ~{elapsed_fraction * 100:.0f}%-ánál)" if elapsed_fraction is not None else ""
        early_line = (
            f"\n🔬 Korai (gyorsulás-alapú) jelzés{pace_note}{elapsed_note}"
            f"\n⚠️ Nagyobb a hamis jelzés esélye, mint a szokásos jelzésnél"
        )

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

    body = (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{early_line}"
        f"{indicator_line}"
        f"{funding_line}"
        f"{warning_line}"
        f"{bounce_line}"
        f"{risk_line}"
    )
    # ÚJ (szellős dizájn): extra sortörés az elején és a végén, hogy a
    # Telegramon a riasztások ne folyjanak össze.
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# OI REFERENCIAPONT KERESÉSE (VÁLTOZATLAN)
# ----------------------------------------------------------------------------

def find_oi_baseline(history_without_current: list, now: datetime,
                      target_minutes: float = None, min_minutes: float = None,
                      max_minutes: float = None) -> Optional["OiBaseline"]:
    """ÚJ: opcionális target/min/max paraméterek - alapértelmezésben a
    globális OI_TARGET/MIN/MAX_WINDOW_MINUTES értékeket használja
    (VÁLTOZATLAN viselkedés a STANDARD jelzéshez), de az EARLY jelzés egy
    RÖVIDEBB ("gyors") ablakkal is meghívja ugyanezt a függvényt, hogy az
    OI ütemét (nem csak az 5 perces szintjét) is meg tudja nézni."""
    target = OI_TARGET_WINDOW_MINUTES if target_minutes is None else target_minutes
    min_w = OI_MIN_WINDOW_MINUTES if min_minutes is None else min_minutes
    max_w = OI_MAX_WINDOW_MINUTES if max_minutes is None else max_minutes
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if min_w <= age_min <= max_w:
            diff = abs(age_min - target)
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


def evaluate_candle(kdf: pd.DataFrame, now: Optional[datetime] = None) -> Optional["CandleEval"]:
    """Az ÉLŐ (még nyitott) gyertyát értékeli ki a megelőző VOLUME_MA_PERIOD db
    LEZÁRT gyertya átlagához képest. Az irányt az élő gyertya nyitó- és
    jelenlegi ára határozza meg.

    ÚJ: 'pace_vol_multiplier' és 'elapsed_fraction' - a "gyorsulás-alapú"
    (EARLY) jelzéshez. A korábbi (STANDARD) vol_multiplier az élő gyertya
    EDDIG összegyűlt teljes volumenét hasonlítja az átlaghoz - ez definíció
    szerint csak akkor éri el a küszöböt, ha a gyertya idejének nagy része
    már eltelt (a volumen "fel kellett gyűljön"). A pace_vol_multiplier
    ehelyett AZT vetíti előre: "ha a mostani ütem (volumen/eltelt idő)
    kitart a gyertya hátralévő részében is, hányszorosa lenne az átlagnak
    a teljes gyertya?" - így egy hirtelen beindulást akár a gyertya
    elején/közepén is jelezhet, nem kell megvárni a teljes felhalmozódást."""
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

    elapsed_fraction = None
    pace_vol_multiplier = None
    if now is not None and "timestamp" in kdf.columns:
        try:
            live_open_ts = live["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc)
            now_utc = now.astimezone(timezone.utc)
            elapsed_seconds = (now_utc - live_open_ts).total_seconds()
            # Alsó korlát: 20 másodperc alatt a mérés túl zajos ahhoz, hogy
            # belőle egy teljes gyertyára extrapoláljunk - ilyenkor nem
            # számolunk pace-értéket (marad None, az EARLY szűrő ezt kihagyja).
            if elapsed_seconds >= 20:
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
        "rsi": rsi_val,
        "macd_status": macd_status,
        "signal_type": "STANDARD",
        "elapsed_fraction": round(elapsed_fraction, 3) if elapsed_fraction is not None else None,
        "pace_vol_multiplier": round(pace_vol_multiplier, 2) if pace_vol_multiplier is not None else None,
    }

# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR (a belső 30 mp-es ciklus egy "üteme")
# ----------------------------------------------------------------------------

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, funding_cache: dict, now: datetime):
    # ÚJ: a connector limit MAX_CONCURRENT_REQUESTS + KLINES_MAX_CONCURRENT_REQUESTS
    # összegére nőtt, hogy a két KÜLÖN szemafor (általános + klines-specifikus)
    # ne ütközzön/szűküljön vissza feleslegesen ugyanazon a TCP-kapcsolat-poolon.
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + KLINES_MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        # ÚJ: lásd a KLINES_MAX_CONCURRENT_REQUESTS fenti kommentjét - a
        # klines-endpoint (5m ÉS 1h HTF is ide tartozik) ÖNÁLLÓ, jóval
        # szűkebb szemafort kap, függetlenül az OI/funding/ticker hívásoktól.
        klines_semaphore = asyncio.Semaphore(KLINES_MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, htf_cache, funding_cache

        if valid_contracts is None:
            valid_contracts = await fetch_valid_contract_symbols(session)

        # ÚJ (v17): a korábban kiküldött, még függőben lévő jelzések
        # kiértékelése - MOST MÁR az 5 perces gyertyák high/low-ját nézve, nem
        # egyetlen pillanatnyi ticker-árat (lásd a resolve_pending_signals()
        # feletti blokk-kommentet a módszertani váltás okáról).
        await resolve_pending_signals(state, session, klines_semaphore, now)

        candidates = []
        for s, info in tickers.items():
            if not (MIN_VOLUME_USDT <= info["quote_volume_24h"] <= MAX_VOLUME_USDT):
                continue
            if not is_probably_crypto(s):
                continue
            if valid_contracts is not None and s not in valid_contracts:
                continue
            candidates.append(s)

        missing_htf = [s for s in candidates if s not in htf_cache]
        # ÚJ: lásd a HTF_FETCH_BATCH_SIZE fenti kommentjét - egy friss futás
        # ELSŐ körében korábban az ÖSSZES (akár 100+) jelölt HTF-trendje
        # egyszerre indult el, ami rendszeresen kiváltotta a BingX
        # endpoint-szintű "trigger frequency limit" tiltását (code 100410).
        # Mostantól körönként csak egy adagot kérünk le - a maradék a
        # következő körökben pótlódik, mire a teljes htf_cache feltöltődik.
        if len(missing_htf) > HTF_FETCH_BATCH_SIZE:
            missing_htf = missing_htf[:HTF_FETCH_BATCH_SIZE]
        # JAVÍTÁS: a funding rate ritkán (jellemzően óránál ritkábban) változik
        # érdemben, ezért - ugyanúgy, mint a HTF trendet - CSAK EGYSZER kérjük
        # le szimbólumonként egy futáson belül, nem minden 30 mp-es körben.
        # Enélkül feleslegesen ~50%-kal nőne az API-hívások száma körönként.
        missing_funding = [s for s in candidates if s not in funding_cache]

        # JAVÍTÁS: korábban az OI, a gyertyák és a HTF-trend lekérdezése 3
        # EGYMÁS UTÁNI (szekvenciális) await-blokkban történt, ami feleslegesen
        # megnyújtotta a kör futásidejét - főleg az első körben, amikor a teljes
        # HTF-cache még üres. Mostantól mindhárom EGYSZERRE, egy közös
        # gather()-ben fut, a MAX_CONCURRENT_REQUESTS szemafor így is korlátozza
        # az egyidejű valós hálózati kéréseket, csak nem kell egymásra várniuk.
        # ÚJ: a Funding Rate lekérdezése is ugyanebbe a gather()-be került (csak
        # a még nem cache-elt szimbólumokra), hogy párhuzamosan fusson és ne
        # lassítsa/terhelje feleslegesen a kört.
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, klines_semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, klines_semaphore, s) for s in missing_htf]
        funding_tasks = [fetch_funding_rate(session, semaphore, s) for s in missing_funding]

        # VÉDEKEZÉS: return_exceptions=True, hogy EGY váratlan hiba (pl. egy
        # előre nem látott API-válasz-formátum egyetlen szimbólumra) ne tudja
        # elszállítani az egész kört/futást - az érintett feladat eredménye
        # ilyenkor egy Exception-példány lesz, amit lent egyszerűen kiszűrünk.
        oi_results, kline_results, htf_results, funding_results = await asyncio.gather(
            asyncio.gather(*oi_tasks, return_exceptions=True),
            asyncio.gather(*kline_tasks, return_exceptions=True),
            asyncio.gather(*htf_tasks, return_exceptions=True),
            asyncio.gather(*funding_tasks, return_exceptions=True),
        )

        if htf_results:
            for item in htf_results:
                if isinstance(item, BaseException):
                    continue
                s, htf_data = item
                if htf_data is not None and htf_data.get("trend") is not None:
                    htf_cache[s] = htf_data

        if funding_results:
            for item in funding_results:
                if isinstance(item, BaseException):
                    continue
                s, fr = item
                if fr is not None:
                    funding_cache[s] = fr

    oi_map = {}
    for item in oi_results:
        if isinstance(item, BaseException):
            continue
        s, oi = item
        if oi is not None:
            oi_map[s] = oi

    klines_map = {}
    for item in kline_results:
        if isinstance(item, BaseException):
            continue
        s, df = item
        if df is not None:
            klines_map[s] = df

    alerts_sent = 0
    evaluated = 0
    htf_warned = 0
    sr_warned = 0
    for symbol in candidates:
        candle = evaluate_candle(klines_map.get(symbol), now=now)
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
        # ÚJ: "utoljára látva" időbélyeg minden kiértékelt szimbólumhoz -
        # ezt használja a _cleanup_stale_state_entries(), hogy a listáról
        # (pl. delistázás miatt) lekerült szimbólumok bejegyzései ne
        # maradjanak örökre a state fájlban.
        entry["last_seen"] = now.isoformat()

        oi_baseline = find_oi_baseline(entry["oi_history"][:-1], now)
        if oi_baseline is None or oi_baseline["oi"] <= 0:
            continue

        oi_change_pct = (oi_now - oi_baseline["oi"]) / oi_baseline["oi"] * 100
        funding_rate = funding_cache.get(symbol)

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

        # ÚJ: EARLY (gyorsulás-alapú) jelzés - lásd a fájl elején az
        # "EARLY (gyorsulás-alapú) jelzés paraméterei" blokk-kommentet.
        # Csak akkor vizsgáljuk, ha a STANDARD (még) nem tüzelt ugyanerre a
        # mozgásra - így nem kap a felhasználó két jelzést ugyanarról.
        is_setup_early = False
        oi_fast_change_pct = None
        if not is_setup:
            elapsed_fraction = candle.get("elapsed_fraction")
            pace_vol_multiplier = candle.get("pace_vol_multiplier")
            if (
                elapsed_fraction is not None
                and EARLY_MIN_ELAPSED_FRACTION <= elapsed_fraction <= EARLY_MAX_ELAPSED_FRACTION
                and pace_vol_multiplier is not None
                and pace_vol_multiplier >= EARLY_MIN_PACE_VOL_MULT
                and candle["candle_vol_usdt"] >= EARLY_MIN_CANDLE_VOL_USDT
                and abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            ):
                oi_fast_baseline = find_oi_baseline(
                    entry["oi_history"][:-1], now,
                    target_minutes=OI_FAST_TARGET_WINDOW_MINUTES,
                    min_minutes=OI_FAST_MIN_WINDOW_MINUTES,
                    max_minutes=OI_FAST_MAX_WINDOW_MINUTES,
                )
                if oi_fast_baseline is not None and oi_fast_baseline["oi"] > 0:
                    oi_fast_change_pct = (oi_now - oi_fast_baseline["oi"]) / oi_fast_baseline["oi"] * 100
                    is_setup_early = oi_fast_change_pct >= EARLY_MIN_OI_FAST_INCREASE

        cooldown_ok = True
        if entry.get("last_alert_ts"):
            last_alert_dt = datetime.fromisoformat(entry["last_alert_ts"])
            if (now - last_alert_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                cooldown_ok = False

        fired_signal_type = "STANDARD" if is_setup else ("EARLY" if is_setup_early else None)

        if fired_signal_type and cooldown_ok:
            # JAVÍTÁS: a htf_warned/sr_warned számlálókat korábban FÜGGETLENÜL
            # növeltük attól, hogy tényleg kiment-e bármilyen jelzés - ez azt
            # eredményezte, hogy a log "X kiküldött jelzés ment trenddel
            # szemben" üzenete FÉLREVEZETŐ volt: a "kiküldött jelzés" szó
            # ellenére valójában egyetlen olyan szimbólumot számolt, amelynek
            # az élő gyertya iránya épp szemben állt a HTF trenddel - TELJESEN
            # függetlenül attól, hogy egyáltalán tüzelt-e a STANDARD/EARLY
            # feltétel. Emiatt a log akár 60+ "figyelmeztetést" is mutathatott
            # ugyanabban a körben, amikor a ténylegesen kiküldött jelzések
            # száma (alerts_sent) 0 volt - pontosan ez okozta a most látott
            # "0 riasztás, de 65 kiküldött jelzés trenddel szemben" ellentmondást.
            # Mostantól a számláló CSAK a ténylegesen elküldött jelzéseknél nő.
            if against_trend:
                htf_warned += 1
            if near_level_risk:
                sr_warned += 1
            # EARLY jelzésnél az oi_change_pct helyett a "gyors" (rövidebb
            # ablakos) OI-változást mutatjuk az üzenetben - ez tükrözi
            # ténylegesen, mi váltotta ki a jelzést.
            display_oi_change_pct = oi_fast_change_pct if fired_signal_type == "EARLY" else oi_change_pct
            msg = format_scalp_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                oi_now, display_oi_change_pct, htf_trend=htf_trend,
                bounce_confluence=bounce_confluence, near_level_risk=near_level_risk,
                rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                signal_type=fired_signal_type,
                funding_rate=funding_rate,
                pace_vol_multiplier=candle.get("pace_vol_multiplier"),
                elapsed_fraction=candle.get("elapsed_fraction"),
            )
            await send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            # v18: a jelzést a napi winrate-összesítőhöz is regisztráljuk. A
            # SL mostantól EGYSZERŰ, FIX -1.5%-os elmozdulás (lásd
            # OUTCOME_FIXED_SL_PCT és resolve_pending_signals) - az előző
            # ATR/swing-alapú számítást (compute_sl_tp) a felhasználó kérésére
            # töröltük, a statisztika egyszerűbb és átláthatóbb lett tőle.
            register_pending_signal(state, symbol, fired_signal_type, candle["direction"], candle["price"], now)
            trend_note = " ⚠️ TRENDDEL SZEMBEN" if against_trend else ""
            bounce_note = " 🎯 SZINT-VISSZAPATTANÁS" if bounce_confluence else ""
            logger.info("JELZÉS küldve [%s]: %s [%s] (Ár %+.2f%%, Vol %.1fx átlag, OI %+.2f%%, 1h trend: %s)%s%s",
                        fired_signal_type, symbol, candle["direction"], candle["price_change_pct"],
                        candle["vol_multiplier"], display_oi_change_pct, htf_trend or "ismeretlen",
                        trend_note, bounce_note)

    if htf_warned:
        logger.info("  (ebben a körben %d kiküldött jelzés ment trenddel szemben - figyelmeztetéssel)", htf_warned)
    if sr_warned:
        logger.info("  (ebben a körben %d kiküldött jelzés ment támasz/ellenállás ellen - figyelmeztetéssel)", sr_warned)

    # ÚJ (v14): ha új UTC nap kezdődött (és eltelt egy kis idő éjfél óta),
    # elküldi az előző nap winrate-összesítőjét. A state-alapú gate miatt
    # (lásd maybe_send_daily_summary) naponta csak egyszer megy ki, akárhány
    # 30 mp-es körben is fut le ez a függvény.
    await maybe_send_daily_summary(state, now)

    return alerts_sent, evaluated, valid_contracts, htf_cache, funding_cache

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
            logger.warning("Egy másik futás már aktívnak tűnik (zár kora: %.1f perc) - "
                            "ez a példány csendben kilép, hogy elkerüljük az átfedést/dupla riasztást.",
                            lock_age_minutes)
            return
        else:
            logger.warning("A talált zár elavultnak (beragadtnak) tűnik - felülírjuk és folytatjuk.")

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
    funding_cache = {}   # symbol -> funding rate (%), futáson belül újrahasznosítva (ritkán változik)
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
            alerts, evaluated, valid_contracts, htf_cache, funding_cache = await asyncio.wait_for(
                run_single_pass(state, valid_contracts, htf_cache, funding_cache, now),
                timeout=remaining_budget,
            )
        except asyncio.TimeoutError:
            logger.warning("[%d. kör] Túllépte az időkeretet (%.0f mp), megszakítva. "
                            "A state addig elért állapotát elmentjük, a ciklus leáll.",
                            pass_num, remaining_budget)
            save_state(state)
            break

        total_alerts += alerts
        save_state(state)  # minden kör után mentünk, ne vesszen el adat félbeszakadás esetén

        logger.info("[%d. kör] %d pár kiértékelve, %d riasztás (összesen eddig: %d riasztás).",
                    pass_num, evaluated, alerts, total_alerts)

        pass_elapsed = time.monotonic() - pass_start
        remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
        if remaining_total <= 0:
            break

        sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
        sleep_time = min(sleep_time, remaining_total)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    logger.info("Ciklus vége: %d kör lefutott, összesen %d riasztás. "
                "A szkript rendesen leáll - a következő külső cron-hívás friss példányt indít.",
                pass_num, total_alerts)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
                        "az értesítés küldése ki lesz hagyva, csak a state fájl frissül.")
    asyncio.run(main())
