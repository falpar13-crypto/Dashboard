"""
BingX Perpetual - Daytrade Felhalmozás-figyelő (1h idősík)
====================================================================
"""

import asyncio
import gzip
import json
import logging
import os
import re
import time
import uuid
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
logger = logging.getLogger("daytrade_checker")

# ----------------------------------------------------------------------------
# 1) DAYTRADE PARAMÉTEREK
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1h"      
CANDLE_DURATION_SECONDS = 3600  
# SZIGORÍTVA: 7.0 -> 5.0 - kevésbé engedjük be a már jócskán kifutott mozgásokat
MAX_PRICE_CHANGE = 5.0      
# SZIGORÍTVA: 3.0 -> 4.0 - nagyobb OI-elmozdulás kell a valódi felhalmozáshoz
MIN_OI_INCREASE = 4.0       
# SZIGORÍTVA: 50 000 -> 75 000 - arányosan a MIN_VOL_MULTIPLIER emeléséhez
MIN_CANDLE_VOL_USDT = 75_000  

VOLUME_MA_PERIOD = 12       
# SZIGORÍTVA: 1.8 -> 2.5 - ez volt a leggyengébb pont; a scalp botban is 2.5x
# a bevált, szigorú küszöb, és semmi nem indokolta, hogy itt lazább legyen
MIN_VOL_MULTIPLIER = 2.5    

# SZIGORÍTVA: 3.5 -> 5.0 - az EARLY egy VETÍTETT (extrapolált) szám, tehát
# eleve zajosabb, mint a STANDARD - alacsony küszöbbel könnyen "belövi"
# magát egy random kilengés is egy 1 órás gyertya elején. A scalp botban is
# 5.0x a bevált érték ugyanerre a célra.
EARLY_MIN_PACE_VOL_MULT = 5.0    
ENABLE_EARLY_SIGNALS = False  # ÚJ: kikapcsolva - a mai audit-adat szerint az EARLY
# típus gyengén teljesített (28.6% pontosság), ráadásul minden kiváltott
# jelzés extra nyitott audit-tételt jelent, ami lassítja a futást. A kódot
# nem töröltük, könnyen visszakapcsolható, ha a kép megváltozik.
EARLY_MIN_ELAPSED_FRACTION = 0.1  
EARLY_MAX_ELAPSED_FRACTION = 0.5   
# SZIGORÍTVA: 20 000 -> 35 000 - arányosan a MIN_CANDLE_VOL_USDT emeléséhez
EARLY_MIN_CANDLE_VOL_USDT = 35_000  

OI_FAST_TARGET_WINDOW_MINUTES = 15
OI_FAST_MIN_WINDOW_MINUTES = 5
OI_FAST_MAX_WINDOW_MINUTES = 30
# SZIGORÍTVA: 1.5 -> 2.0
EARLY_MIN_OI_FAST_INCREASE = 2.0   

FUNDING_SQUEEZE_THRESHOLD_PCT = 0.01

# ÚJ: Funding rate DELTA (gyorsulás-figyelő). A fenti statikus küszöb csak
# azt nézi, hogy a funding rate ÉPP MOST szélsőséges-e. Ez itt azt méri,
# MENNYIT MOZDULT egy rövid ablakban - egy gyorsan negatívba forduló
# funding rate (rövid idő alatt) short-squeeze előjele lehet MÉG AKKOR IS,
# ha az abszolút érték önmagában még nem érné el a statikus küszöböt. Ez
# csak TÁJÉKOZTATÓ kiegészítés az üzenetben, nem szűrőfeltétel - ugyanúgy,
# ahogy a bounce_confluence/near_level_risk sem blokkol semmit.
FUNDING_HISTORY_TARGET_MINUTES = 60
FUNDING_HISTORY_MIN_MINUTES = 20
FUNDING_HISTORY_MAX_MINUTES = 180
FUNDING_ACCEL_THRESHOLD_PCT = 0.015   # ennyi előjeles %-pontot mozduljon a fenti ablakban, hogy kiemeljük

TOTAL_RUN_BUDGET_SECONDS = 520   
PASS_INTERVAL_SECONDS = 30       

# ----------------------------------------------------------------------------
# ÚJ: WEBSOCKET ÉLŐ GYERTYA-FIGYELŐ (opcionális, kapcsolható)
# ----------------------------------------------------------------------------
# Lásd az alert_checker.py azonos blokk-kommentjét a teljes indoklásért.
# Rövid összefoglaló: a REST-polling (30 mp-enkénti lekérdezés) helyett/
# mellett a BingX websocket-jén keresztül szinte valós időben kapjuk az
# élő (még nyitott) 1h gyertya adatait - főleg az EARLY jelzésnek segít.
#
# BIZTONSÁGI KAPCSOLÓ: alapértelmezetten KIKAPCSOLVA. Bekapcsolás: GitHub
# Actions Variable USE_WEBSOCKET_KLINES=true (lásd a workflow yml-t).
USE_WEBSOCKET_KLINES = os.environ.get("USE_WEBSOCKET_KLINES", "false").strip().lower() in ("1", "true", "yes")
WS_URL = "wss://open-api-swap.bingx.com/swap-market"
WS_KLINE_INTERVAL_MAP = {
    "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "12h": "12h",
    "1d": "1day", "3d": "3day", "1w": "1week", "1M": "1month",
}
WS_KLINE_INTERVAL = WS_KLINE_INTERVAL_MAP.get(ALERT_TIMEFRAME, "1h")
WS_SYMBOLS_PER_CONNECTION = 190   # a BingX doksik szerinti feliratkozási limit alatt tartva
WS_CONNECT_TIMEOUT_SECONDS = 15
WS_RECONNECT_DELAY_SECONDS = 3
# ha ennél régebbi a legutóbb kapott WS-adat egy symbolra, inkább a
# REST-fetch-elt (kicsit "régebbi", de megbízható) élő gyertyát használjuk
WS_MAX_STALENESS_SECONDS = 20


# ----------------------------------------------------------------------------
# 0) ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"
FUNDING_RATE_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/premiumIndex"
DEPTH_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/depth"

# ÚJ: Orderbook imbalance (vékony ask/bid oldal figyelő). Csak akkor
# kérdezzük le, amikor egy jelzés éppen kimenne (lásd fetch_orderbook_imbalance
# hívási pontját a fő ciklusban) - NEM minden jelöltre minden pass-ban -,
# mert ez egy viszonylag "drágább" hívás, és ritkán van rá szükség (csak a
# ténylegesen kiküldött jelzéseknél). Tisztán tájékoztató, nem szűr.
ORDERBOOK_LEVELS_LIMIT = 20
ORDERBOOK_IMBALANCE_THRESHOLD = 1.8   # bid/ask notional-arány, ami felett/alatt kiemeljük

STATE_FILE = Path(__file__).parent / "daytrade_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "daytrade_alert_log.jsonl"

# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV, SL/TP-MENTES SIGNAL-AUDIT RENDSZER
# ----------------------------------------------------------------------------
# A felhasználó kifejezett kérésére: a meglévő jelzés-generáló logikát ez
# NEM módosítja, csak KIEGÉSZÍTI naplózással és utólagos, több időablakos
# (5m/15m/30m/1h/2h/4h) irány-pontosság + MFE/MAE-elemzéssel. NEM
# fix SL/TP-t szimulál (az a meglévő register_pending_signal/
# resolve_pending_signals rendszer, amit szintén NEM bántunk) - ehelyett
# objektíven méri, mennyire mozdult az ár a jelzett irányba, és mennyire
# ellene, félretéve bármilyen konkrét kereskedési tervet.
AUDIT_SIGNALS_FILE = Path(__file__).parent / "daytrade_audit_signals.jsonl"
AUDIT_RESULTS_FILE = Path(__file__).parent / "daytrade_audit_results.jsonl"

# Az időablakok, amikben az irány-pontosságot és MFE/MAE-t mérjük.
AUDIT_WINDOWS_MINUTES = [("5m", 5), ("15m", 15), ("30m", 30), ("1h", 60), ("2h", 120), ("4h", 240)]
AUDIT_MAX_WINDOW_MINUTES = AUDIT_WINDOWS_MINUTES[-1][1]

# Time-to-move mérföldkövek (%) - mennyi idő alatt éri el az ár ezeket a
# kedvező irányú elmozdulásokat a jelzés után.
AUDIT_TIME_TO_MOVE_LEVELS_PCT = [0.5, 1.0, 2.0, 3.0]

# ÚJ: minősítési küszöbök - SZÁNDÉKOSAN külön konstansként, hogy később
# könnyen finomhangolható legyen (ahogy a specifikáció kérte). A
# minősítés a 4h-s (leghosszabb, "végleges") ablak directional_return és
# MAE értékén alapul.
AUDIT_VERY_GOOD_MIN_RETURN_PCT = 2.0
AUDIT_VERY_GOOD_MAX_MAE_PCT = 1.0
AUDIT_GOOD_MIN_RETURN_PCT = 0.5
AUDIT_BAD_MAX_RETURN_PCT = -1.0

# Napi riport küldési órája (helyi idő, Europe/Budapest - lásd
# SUMMARY_TIMEZONE lentebb) - a nap UTOLSÓ lezárt futásakor ez után küldi el.
AUDIT_DAILY_REPORT_HOUR = 23

# ÚJ (hibajavítás - torlódás elkerülése): körönként LEGFELJEBB ennyi
# nyitott jelzést oldunk fel egyszerre - élesben kiderült, hogy sok
# felhalmozódott nyitott jelzésnél (magas napi jelzésszámnál) a
# feloldási lépés futásideje korlátlanul nőhetett, ami a cron-
# intervallumon (5-10 perc) túlnyúlva GitHub Actions-torlódást okozott,
# és MAGÁT A JELZÉS-KÜLDÉST is késleltette. A LEGRÉGEBBI jelzések
# élveznek elsőbbséget, a többi a következő körben folytatódik.
MAX_AUDIT_RESOLVE_PER_RUN = 30

# ÚJ: SYMBOL-TILTÁS 3 EGYMÁS UTÁNI BAD MINŐSÍTÉS UTÁN - a felhasználóval
# egyeztetett védőmechanizmus. Ha egy symbolra ennél a BOTNÁL (nem
# összevontan a többivel) egymás után ennyi VÉGLEGES (leghosszabb elért
# ablakú) minősítés mind "BAD", a symbol erre a botra nézve ennyi órára
# letiltásra kerül - nem kap új jelzést, amíg a tiltás le nem jár.
BAN_AFTER_CONSECUTIVE_BAD = 3
BAN_DURATION_HOURS = 24
SYMBOL_OUTCOME_HISTORY_MAX = 10  # ennyi legutóbbi minősítést tartunk meg symbolonként


# ----------------------------------------------------------------------------
# ÚJ: BOT-KÖZI MEGERŐSÍTÉS (cross-bot confirmation)
# ----------------------------------------------------------------------------
# Ez a bot a SAJÁT, egyedi nevű fájljába ír egy sort minden kiküldött
# jelzésnél - ezt a workflow yml (a jelenlegi futás ELEJÉN, olvasásra)
# letölti a másik két bot state-branch-éről. Ez a bot csak OLVASSA a másik
# kettőt, írni csak a SAJÁT fájljába ír - nincs közös írási célpont, tehát
# nincs git push-ütközési kockázat (ugyanaz az elv, ami miatt eredetileg
# is külön state-branch-et kapott mindhárom bot).
CROSS_SIGNAL_FILE = Path(__file__).parent / "daytrade_recent_signals.jsonl"
OTHER_BOT_SIGNAL_FILES = {
    "SCALP": Path(__file__).parent / "scalp_recent_signals.jsonl",
    "KASZKÁD": Path(__file__).parent / "cascade_recent_signals.jsonl",
}
CROSS_BOT_WINDOW_MINUTES = 45     # ennyi percen belüli másik-bot jelzés számít megerősítésnek
CROSS_SIGNAL_RETENTION_HOURS = 6  # a SAJÁT fájl ennél régebbi sorait eldobja íráskor, ne nőjön korlátlanul

def _append_cross_bot_signal(symbol: str, direction: str, signal_type: str, now: datetime) -> None:
    """A SAJÁT (bot-specifikus nevű) fájlba ír egy sort - ezt olvassák majd
    a MÁSIK botok a kereszt-megerősítéshez."""
    cutoff = now - timedelta(hours=CROSS_SIGNAL_RETENTION_HOURS)
    rows = []
    if CROSS_SIGNAL_FILE.exists():
        try:
            with CROSS_SIGNAL_FILE.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        if datetime.fromisoformat(rec["ts"]) >= cutoff:
                            rows.append(rec)
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
        except OSError:
            pass
    rows.append({"ts": now.isoformat(), "symbol": symbol, "direction": direction, "signal_type": signal_type})
    try:
        with CROSS_SIGNAL_FILE.open("w", encoding="utf-8") as f:
            for rec in rows:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("Kereszt-bot jelzés-fájl írása sikertelen: %s", e)


def get_cross_bot_confirmations(symbol: str, direction: str, now: datetime) -> list:
    """A MÁSIK botok (workflow által előtte letöltött) jelzés-fájljait
    nézi végig, és visszaadja azok listáját, amik az elmúlt
    CROSS_BOT_WINDOW_MINUTES percben UGYANARRA a symbolra, UGYANABBA az
    irányba jeleztek. Ha egy fájl nem létezik (pl. a másik bot még sosem
    futott, vagy a fetch lépés kimaradt), csendben kihagyja - hiányzó
    megerősítés NEM hiba, csak nincs plusz infó."""
    cutoff = now - timedelta(minutes=CROSS_BOT_WINDOW_MINUTES)
    confirmations = []
    for label, path in OTHER_BOT_SIGNAL_FILES.items():
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError:
            continue
        best_ts = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("symbol") != symbol or rec.get("direction") != direction:
                    continue
                ts = datetime.fromisoformat(rec["ts"])
                if ts < cutoff:
                    continue
                if best_ts is None or ts > best_ts:
                    best_ts = ts
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        if best_ts is not None:
            age_minutes = (now - best_ts).total_seconds() / 60
            confirmations.append(f"{label} ({age_minutes:.0f} perce)")
    return confirmations


MIN_VOLUME_USDT = 3_000_000  # 1M -> 3M: shitcoin-szűrés, a napi audit-adat alapján
MAX_VOLUME_USDT = 150_000_000

NON_CRYPTO_PREFIXES = ("NCSK", "NCFX")

def is_probably_crypto(symbol: str) -> bool:
    base = symbol.split("-")[0]
    if any(base.startswith(p) for p in NON_CRYPTO_PREFIXES):
        return False
    if "USD" in base:
        return False
    return True

OI_TARGET_WINDOW_MINUTES = 60
OI_MIN_WINDOW_MINUTES = 30
OI_MAX_WINDOW_MINUTES = 120
MAX_HISTORY_AGE_MINUTES = 360

# SZIGORÍTVA: 120 -> 240 perc (4 óra) - egy napon belüli mozgás sokáig
# tarthat, nem akarunk 2 óránként újra jelzést kapni ugyanarra a folytatódó
# trendre (ez is hozzájárulhatott a "sok jelzés" érzethez)
ALERT_COOLDOWN_MINUTES = 240

HIGHER_TIMEFRAME = "4h"       
HTF_KLINES_LIMIT = 100        
REQUIRE_HTF_ALIGNMENT = True  

# ÚJ: HH/HL/LH/LL (swing-struktúra) alapú trendfelismerés - LECSERÉLI az
# EMA(50)-alapú módszert (ugyanaz a csere, mint a scalp botban/alert_checker.py-
# ban). Az EMA(50) 4h gyertyákon LASSÚ: 50*4h = 200 óra (kb. 8.3 nap) kell,
# mire stabilizálódik, és egy trendváltás után is hosszan "elmarad" a valós
# ártól. A price-action (swing-struktúra) megközelítés a tényleges
# csúcsokat/mélypontokat nézi:
#   - UP: az utolsó két swing csúcs egyre magasabb (Higher High) ÉS az
#     utolsó két swing mélypont egyre magasabb (Higher Low).
#   - DOWN: az utolsó két swing csúcs egyre alacsonyabb (Lower High) ÉS az
#     utolsó két swing mélypont egyre alacsonyabb (Lower Low).
#   - Minden más eset (pl. HH+LL vagy LH+HL - vegyes szerkezet) NEUTRAL.
SWING_FRACTAL_LEGS = 2   # ennyi gyertyát nézünk MINDKÉT oldalon egy swing
                           # csúcs/mélypont azonosításához ("5 gyertyás fraktál")

MAX_CONCURRENT_REQUESTS = 16   
KLINES_MAX_CONCURRENT_REQUESTS = 4
KLINES_REQUEST_PACING_SECONDS = 0.2  

REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

HTF_FETCH_BATCH_SIZE = 20

_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150  

KLINES_LIMIT = 120
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SUMMARY_TIMEZONE = ZoneInfo("Europe/Budapest")

ENABLE_LEGACY_SLTP_SUMMARY = False  # ÚJ: kikapcsolva - a régi, fix SL/TP-
# szimulációs napi összesítő redundánssá vált a sokkal részletesebb,
# objektív audit-riport mellett, és ÖNMAGÁBAN is jelentős, felesleges
# API-terhelést jelentett (minden nyitott "régi" jelzéshez külön
# lekérdezés). A kód nem lett törölve, könnyen visszakapcsolható.

# ----------------------------------------------------------------------------
# KIÉRTÉKELÉS (NAPI ÖSSZESÍTŐ)
# ----------------------------------------------------------------------------
OUTCOME_EVAL_WINDOW_MINUTES = 1440      
OUTCOME_FIXED_SL_PCT = 4.0            
OUTCOME_PROFIT_LEVELS_PCT = [2.0, 5.0, 8.0, 12.0]  
OUTCOME_MAX_STALE_MINUTES = 120        

DAILY_SUMMARY_MIN_DELAY_MINUTES = 35  

class CandleEval(TypedDict):
    price: float
    price_change_pct: float
    vol_multiplier: float
    candle_vol_usdt: float
    direction: str          
    rsi: Optional[float]
    macd_status: Optional[str]
    signal_type: str        
    elapsed_fraction: Optional[float]      
    pace_vol_multiplier: Optional[float]   

class OiBaseline(TypedDict):
    ts: str
    oi: float

def _rotate_signal_log(before_date_str: str) -> None:
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
                    keep_lines.append(line)  
                    continue
                entry_date = rec.get("entry_date")
                if entry_date and entry_date < before_date_str:
                    month_key = entry_date[:7]  
                    archive_by_month.setdefault(month_key, []).append(line)
                else:
                    keep_lines.append(line)

        if not archive_by_month:
            return  

        for month_key, lines in archive_by_month.items():
            archive_path = SIGNAL_LOG_FILE.parent / f"daytrade_alert_log_{month_key}.jsonl.bak"
            with archive_path.open("a", encoding="utf-8") as f:
                f.writelines(lines)

        tmp_path = SIGNAL_LOG_FILE.with_suffix(SIGNAL_LOG_FILE.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            f.writelines(keep_lines)
        os.replace(tmp_path, SIGNAL_LOG_FILE)
    except OSError as e:
        logger.error("Napló-rotáció sikertelen: %s", e)

def _log_signal_outcome(record: dict) -> None:
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error("Nem sikerült írni a jelzés-naplóba: %s", e)

# ----------------------------------------------------------------------------
# ÚJ: HISTORIKUS KIMENETEL-STATISZTIKA
# ----------------------------------------------------------------------------
# A napi winrate-összesítő (lásd lentebb) már régóta feldolgozza a
# SIGNAL_LOG_FILE-t, de csak NAPI bontásban jelenít meg összesítést. Ez a
# funkció ugyanabból az adatból egy jelzés KIKÜLDÉSE ELŐTT ad egy konkrét,
# "hasonló korábbi jelzések hogyan alakultak" számot - pl. "az elmúlt 30
# nap hasonló EARLY LONG jelzéseinek 62%-a érte el a +2%-ot". Ez NEM
# szűrőfeltétel (nem blokkolja a jelzést), csak tájékoztató kontextus és
# egy szerény hatás a meggyőződés-pontszámra.
HISTORICAL_STATS_LOOKBACK_DAYS = 30
HISTORICAL_STATS_MIN_SAMPLES = 10   # ennél kevesebb releváns múltbeli minta esetén nem mutatunk semmit

def compute_historical_stats(signal_type: str, direction: str, now: datetime) -> Optional[dict]:
    """A SAJÁT (ugyanezen bot) korábbi, LEZÁRT (nem UNKNOWN) jelzéseinek
    naplójából statisztikát számol az azonos signal_type + irány
    kombinációra, BÁRMELY symbolon (egy adott symbolra általában túl
    kevés minta gyűlne össze ahhoz, hogy megbízható legyen)."""
    if not SIGNAL_LOG_FILE.exists():
        return None
    cutoff = now - timedelta(days=HISTORICAL_STATS_LOOKBACK_DAYS)
    matched = []
    try:
        with SIGNAL_LOG_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("signal_type") != signal_type or rec.get("direction") != direction:
                    continue
                if rec.get("outcome") not in ("LOSS", "NEUTRAL"):
                    continue  # UNKNOWN vagy még nem lezárt jelzés - kihagyjuk
                try:
                    entry_ts = datetime.fromisoformat(rec["entry_ts"])
                except (KeyError, ValueError):
                    continue
                if entry_ts < cutoff:
                    continue
                matched.append(rec)
    except OSError:
        return None

    if len(matched) < HISTORICAL_STATS_MIN_SAMPLES:
        return None

    total = len(matched)
    first_level = OUTCOME_PROFIT_LEVELS_PCT[0] if OUTCOME_PROFIT_LEVELS_PCT else None
    reached_first_level = 0
    if first_level is not None:
        level_key = f"level_{first_level}pct"
        reached_first_level = sum(1 for r in matched if (r.get("levels_reached") or {}).get(level_key))
    loss_count = sum(1 for r in matched if r.get("outcome") == "LOSS")
    favorable_values = [r["max_favorable_pct"] for r in matched if r.get("max_favorable_pct") is not None]
    avg_favorable = sum(favorable_values) / len(favorable_values) if favorable_values else None

    return {
        "total": total,
        "win_rate_pct": round(reached_first_level / total * 100, 1) if first_level is not None else None,
        "loss_rate_pct": round(loss_count / total * 100, 1),
        "avg_max_favorable_pct": round(avg_favorable, 2) if avg_favorable is not None else None,
        "profit_level_pct": first_level,
    }


def register_pending_signal(state: dict, symbol: str, signal_type: str, direction: str, entry_price: float, now: datetime) -> None:
    pending = state.setdefault("pending_outcomes", [])
    pending.append({
        "id": f"{symbol}_{signal_type}_{now.strftime('%Y%m%dT%H%M%S')}",
        "symbol": symbol,
        "signal_type": signal_type,
        "direction": direction,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "entry_date": now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d"), 
        "window_end_ts": (now + timedelta(minutes=OUTCOME_EVAL_WINDOW_MINUTES)).isoformat(),
    })


# ----------------------------------------------------------------------------
# ÚJ: OBJEKTÍV SIGNAL-AUDIT MOTOR (lásd a fájl elején a blokk-kommentet)
# ----------------------------------------------------------------------------
def register_signal_audit(state: dict, symbol: str, direction: str, signal_type: str,
                            score: Optional[int], entry_price: float, now: datetime,
                            meta: Optional[dict] = None) -> str:
    """Egy ÚJ, a meglévő pending_outcomes-tól TELJESEN FÜGGETLEN nyilvántartásba
    veszi fel a jelzést - ez táplálja az objektív, SL/TP-mentes auditot.
    Visszaadja az egyedi signal_id-t.

    ÚJ: a `meta` egy tetszőleges, bot-specifikus mérhető bemeneteket
    tartalmazó dict (pl. OI-növekedés, HTF-trend egyezés, RSI-divergencia
    megléte) - ez táplálja a küszöb-hangolási javaslat rendszert (lásd
    generate_threshold_suggestions() lentebb)."""
    signal_id = str(uuid.uuid4())
    windows = {
        label: {"target_minutes": minutes, "resolved": False, "future_price": None,
                 "directional_return_pct": None, "mfe_pct": None, "mae_pct": None,
                 "classification": None, "resolved_ts": None}
        for label, minutes in AUDIT_WINDOWS_MINUTES
    }
    audit_pending = state.setdefault("_audit_pending", [])
    audit_pending.append({
        "signal_id": signal_id,
        "symbol": symbol,
        "direction": direction,
        "signal_type": signal_type,
        "score": score,
        "timeframe": ALERT_TIMEFRAME,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "windows": windows,
        "time_to_move": {str(lvl): None for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT},
    })
    _append_log_to(AUDIT_SIGNALS_FILE, {
        "signal_id": signal_id, "ts": now.isoformat(), "symbol": symbol,
        "direction": direction, "signal_type": signal_type, "score": score,
        "timeframe": ALERT_TIMEFRAME, "entry_price": entry_price,
        "meta": meta or {},
    })
    return signal_id


def _append_log_to(path: Path, record: dict) -> None:
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _record_symbol_outcome(state: dict, symbol: str, classification: str, now: datetime) -> None:
    """ÚJ: symbolonkénti minősítés-történet + 3-bad-tiltás - lásd a
    BAN_AFTER_CONSECUTIVE_BAD kommentjét. Botonként KÜLÖN számol (ez a
    state ennek a botnak a saját state-branch-én él, nem osztott a
    másik három bottal)."""
    history = state.setdefault("_symbol_outcome_history", {})
    sym_hist = history.setdefault(symbol, [])
    sym_hist.append(classification)
    if len(sym_hist) > SYMBOL_OUTCOME_HISTORY_MAX:
        sym_hist[:] = sym_hist[-SYMBOL_OUTCOME_HISTORY_MAX:]

    if len(sym_hist) >= BAN_AFTER_CONSECUTIVE_BAD and all(c == "BAD" for c in sym_hist[-BAN_AFTER_CONSECUTIVE_BAD:]):
        bans = state.setdefault("_symbol_ban_until", {})
        ban_until = now + timedelta(hours=BAN_DURATION_HOURS)
        bans[symbol] = ban_until.isoformat()
        logger.warning("SYMBOL TILTÁS: %s - %d egymás utáni BAD minősítés, tiltva %s-ig",
                        symbol, BAN_AFTER_CONSECUTIVE_BAD, ban_until.isoformat())


def is_symbol_banned(state: dict, symbol: str, now: datetime) -> bool:
    """ÚJ: ellenőrzi, hogy a symbol jelenleg tiltva van-e ennél a botnál.
    A lejárt tiltásokat automatikusan kitakarítja."""
    bans = state.get("_symbol_ban_until", {})
    ban_until_str = bans.get(symbol)
    if not ban_until_str:
        return False
    try:
        ban_until = datetime.fromisoformat(ban_until_str)
    except ValueError:
        return False
    if now >= ban_until:
        del bans[symbol]
        return False
    return True


async def resolve_signal_audit(state: dict, session, semaphore, now: datetime) -> None:
    """A függőben lévő jelzéseket a friss árfolyam-adat alapján frissíti:
    MFE/MAE folyamatos követése, time-to-move mérföldkövek rögzítése, és
    az esedékessé vált időablakok (5m/15m/.../4h) lezárása. NEM fix
    SL/TP-t szimulál - tisztán az irány szerinti elmozdulást és az
    ellenirányú kilengést méri."""
    pending = state.get("_audit_pending", [])
    if not pending:
        return

    # ÚJ: körönkénti feldolgozási korlát - lásd MAX_AUDIT_RESOLVE_PER_RUN
    # kommentjét. A legrégebbi (leghamarabb esedékes) jelzések mennek
    # előre, a többi változatlanul visszakerül a listába a következő körre.
    pending_sorted = sorted(pending, key=lambda r: r.get("entry_ts", ""))
    to_process = pending_sorted[:MAX_AUDIT_RESOLVE_PER_RUN]
    deferred = pending_sorted[MAX_AUDIT_RESOLVE_PER_RUN:]
    if deferred:
        logger.info("Audit feloldás: %d jelzés halasztva a következő körre (körönkénti limit: %d).",
                    len(deferred), MAX_AUDIT_RESOLVE_PER_RUN)

    still_pending = list(deferred)
    for rec in to_process:
        try:
            entry_dt = datetime.fromisoformat(rec["entry_ts"])
        except (KeyError, ValueError):
            continue
        age_minutes = (now - entry_dt).total_seconds() / 60

        # Az 1 perces gyertyákból a legpontosabb az MFE/MAE-követés - ha
        # ennyi idő már eltelt a jelzés óta, hogy a limit ne legyen elég,
        # durvább (5 perces) granularitásra váltunk.
        if age_minutes <= 60:
            interval, limit = "1m", min(300, int(age_minutes) + 10)
        else:
            interval, limit = "5m", min(300, int(age_minutes / 5) + 10)

        _, kdf = await fetch_klines(session, semaphore, rec["symbol"], interval, limit)
        if kdf is None or len(kdf) == 0:
            still_pending.append(rec)
            continue

        entry_ts_naive = pd.Timestamp(entry_dt.replace(tzinfo=None))
        after = kdf[kdf["timestamp"] >= entry_ts_naive].reset_index(drop=True)
        if after.empty:
            still_pending.append(rec)
            continue

        direction = rec["direction"]
        entry_price = rec["entry_price"]

        # ÚJ (hibajavítás tesztelés közben): a time-to-move mérföldköveket
        # a TELJES eddig ismert árpályán mérjük (ez helyes, mindegy melyik
        # ablakhoz tartozik - egy abszolút időpont az egész jelzésre nézve).
        for _, row in after.iterrows():
            hi, lo = float(row["high"]), float(row["low"])
            if direction == "LONG":
                move_pct = (hi - entry_price) / entry_price * 100
            else:
                move_pct = (entry_price - lo) / entry_price * 100
            for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
                key = str(lvl)
                if rec["time_to_move"][key] is None and move_pct >= lvl:
                    row_ts = row["timestamp"]
                    if pd.notna(row_ts):
                        elapsed_min = (row_ts.to_pydatetime().replace(tzinfo=timezone.utc) - entry_dt).total_seconds() / 60
                        rec["time_to_move"][key] = round(max(0.0, elapsed_min), 1)

        # ÚJ (hibajavítás): korábban EGY közös, a teljes (esetleg a
        # célidőponton túlnyúló) árpályából számolt MFE/MAE/ár került
        # MINDEN esedékes ablakhoz - ez tesztelés közben kiderült hibát
        # okozott (minden ablak ugyanazt az értéket kapta). Mostantól
        # MINDEN ablak a SAJÁT [entry, entry+ablakhossz] időszeletéből
        # számol - a rövidebb ablakok nem "látnak bele" a hosszabbak
        # adatába, még ha egy késői futás egyszerre oldja is fel őket.
        any_unresolved = False
        for label, minutes in AUDIT_WINDOWS_MINUTES:
            w = rec["windows"][label]
            if w["resolved"]:
                continue
            if age_minutes < minutes:
                any_unresolved = True
                continue

            target_ts = entry_ts_naive + pd.Timedelta(minutes=minutes)
            window_slice = after[after["timestamp"] <= target_ts]
            if window_slice.empty:
                window_slice = after.iloc[:1]
            # a cél-időponthoz legközelebbi (azt el nem érő, vagy első
            # az után következő) gyertya záróára a referencia-ár
            at_or_after_target = after[after["timestamp"] >= target_ts]
            window_price = float(at_or_after_target.iloc[0]["close"]) if not at_or_after_target.empty else float(window_slice.iloc[-1]["close"])

            if direction == "LONG":
                fav = float(window_slice["high"].max())
                adv = float(window_slice["low"].min())
                mfe_pct = (fav - entry_price) / entry_price * 100
                mae_pct = (adv - entry_price) / entry_price * 100
                directional_return = (window_price - entry_price) / entry_price * 100
            else:
                fav = float(window_slice["low"].min())
                adv = float(window_slice["high"].max())
                mfe_pct = (entry_price - fav) / entry_price * 100
                mae_pct = (entry_price - adv) / entry_price * 100
                directional_return = (entry_price - window_price) / entry_price * 100

            classification = _classify_audit_result(directional_return, mae_pct)
            w.update({
                "resolved": True, "future_price": window_price,
                "directional_return_pct": round(directional_return, 3),
                "mfe_pct": round(mfe_pct, 3), "mae_pct": round(mae_pct, 3),
                "classification": classification, "resolved_ts": now.isoformat(),
            })
            _append_log_to(AUDIT_RESULTS_FILE, {
                "signal_id": rec["signal_id"], "symbol": rec["symbol"],
                "direction": direction, "signal_type": rec["signal_type"],
                "score": rec["score"], "entry_ts": rec["entry_ts"],
                "window": label, "directional_return_pct": round(directional_return, 3),
                "mfe_pct": round(mfe_pct, 3), "mae_pct": round(mae_pct, 3),
                "classification": classification,
                "time_to_move": dict(rec["time_to_move"]),
            })

        if any_unresolved and age_minutes <= AUDIT_MAX_WINDOW_MINUTES + 30:
            still_pending.append(rec)
        else:
            # ÚJ: a jelzés VÉGLEGESEN lezárult (minden ablak megvolt, vagy
            # túl régi már) - rögzítjük a végső (leghosszabb elért ablakú)
            # minősítést a symbol-történetbe, ami táplálja a 3-bad-tiltást.
            final_classification = None
            best_idx = -1
            for w_idx, (w_label, _) in enumerate(AUDIT_WINDOWS_MINUTES):
                w = rec["windows"][w_label]
                if w["resolved"] and w_idx > best_idx:
                    final_classification = w["classification"]
                    best_idx = w_idx
            if final_classification is not None:
                _record_symbol_outcome(state, rec["symbol"], final_classification, now)
        # ha minden ablak lezárult (vagy túl régi már), a jelzés lekerül
        # a pending listáról - a végleges adatok már a results logban vannak

    state["_audit_pending"] = still_pending


def _classify_audit_result(directional_return_pct: float, mae_pct: float) -> str:
    """VERY GOOD / GOOD / NEUTRAL / BAD minősítés - lásd az
    AUDIT_*_THRESHOLD konstansok kommentjét a küszöbökről."""
    abs_mae = abs(mae_pct)
    if directional_return_pct >= AUDIT_VERY_GOOD_MIN_RETURN_PCT and abs_mae <= AUDIT_VERY_GOOD_MAX_MAE_PCT:
        return "VERY_GOOD"
    if directional_return_pct >= AUDIT_GOOD_MIN_RETURN_PCT:
        return "GOOD"
    if directional_return_pct <= AUDIT_BAD_MAX_RETURN_PCT:
        return "BAD"
    return "NEUTRAL"


def _classify_audit_result(directional_return_pct: float, mae_pct: float) -> str:
    """VERY GOOD / GOOD / NEUTRAL / BAD minősítés - lásd az
    AUDIT_*_THRESHOLD konstansok kommentjét a küszöbökről."""
    abs_mae = abs(mae_pct)
    if directional_return_pct >= AUDIT_VERY_GOOD_MIN_RETURN_PCT and abs_mae <= AUDIT_VERY_GOOD_MAX_MAE_PCT:
        return "VERY_GOOD"
    if directional_return_pct >= AUDIT_GOOD_MIN_RETURN_PCT:
        return "GOOD"
    if directional_return_pct <= AUDIT_BAD_MAX_RETURN_PCT:
        return "BAD"
    return "NEUTRAL"


def _load_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    records = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return records


def generate_daily_audit_report(now: datetime) -> Optional[str]:
    """A mai (helyi idő szerinti) nap ÖSSZES jelzéséből épít egy
    részletes, objektív, SL/TP-mentes riportot. None, ha nincs elég adat."""
    today_str = now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d")

    signals = _load_jsonl(AUDIT_SIGNALS_FILE)
    results = _load_jsonl(AUDIT_RESULTS_FILE)

    signals_today = {
        s["signal_id"]: s for s in signals
        if s.get("ts") and datetime.fromisoformat(s["ts"]).astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d") == today_str
    }
    if not signals_today:
        return None
    results_today = [r for r in results if r.get("signal_id") in signals_today]
    if not results_today:
        return None

    window_stats = {}
    for label, _ in AUDIT_WINDOWS_MINUTES:
        wr = [r for r in results_today if r.get("window") == label]
        if not wr:
            continue
        correct = sum(1 for r in wr if r["directional_return_pct"] > 0)
        window_stats[label] = {"n": len(wr), "accuracy_pct": round(correct / len(wr) * 100, 1)}

    # A legmesszebbi elért ablak eredményét használjuk "végleges"
    # rekordként jelzésenként (ha a 4h még nincs kész, a legutóbb
    # lezárt, rövidebb ablak áll rendelkezésre helyette).
    final_by_signal = {}
    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
    for r in results_today:
        sid = r["signal_id"]
        if sid not in final_by_signal or window_order.get(r["window"], -1) > window_order.get(final_by_signal[sid]["window"], -1):
            final_by_signal[sid] = r
    finals = list(final_by_signal.values())
    if not finals:
        return None

    total_signals = len(signals_today)
    avg_mfe = sum(r["mfe_pct"] for r in finals) / len(finals)
    avg_mae = sum(r["mae_pct"] for r in finals) / len(finals)
    sorted_mfe = sorted(r["mfe_pct"] for r in finals)
    sorted_mae = sorted(r["mae_pct"] for r in finals)
    median_mfe = sorted_mfe[len(sorted_mfe) // 2]
    median_mae = sorted_mae[len(sorted_mae) // 2]

    class_counts = {"VERY_GOOD": 0, "GOOD": 0, "NEUTRAL": 0, "BAD": 0}
    for r in finals:
        c = r.get("classification")
        if c in class_counts:
            class_counts[c] += 1

    long_results = [r for r in finals if signals_today.get(r["signal_id"], {}).get("direction") == "LONG"]
    short_results = [r for r in finals if signals_today.get(r["signal_id"], {}).get("direction") == "SHORT"]
    long_acc = round(sum(1 for r in long_results if r["directional_return_pct"] > 0) / len(long_results) * 100, 1) if long_results else None
    short_acc = round(sum(1 for r in short_results if r["directional_return_pct"] > 0) / len(short_results) * 100, 1) if short_results else None

    by_symbol = {}
    for r in finals:
        sym = signals_today.get(r["signal_id"], {}).get("symbol", "?")
        by_symbol.setdefault(sym, []).append(r)
    symbol_acc = {
        sym: round(sum(1 for r in rs if r["directional_return_pct"] > 0) / len(rs) * 100, 1)
        for sym, rs in by_symbol.items() if len(rs) >= 2
    }

    by_type = {}
    for r in finals:
        t = r.get("signal_type", "?")
        by_type.setdefault(t, []).append(r)
    type_acc = {
        t: round(sum(1 for r in rs if r["directional_return_pct"] > 0) / len(rs) * 100, 1)
        for t, rs in by_type.items()
    }

    score_buckets = {"50-60": (50, 60), "60-70": (60, 70), "70-80": (70, 80), "80+": (80, 101)}
    score_acc = {}
    for label, (lo, hi) in score_buckets.items():
        bucket = [r for r in finals if r.get("score") is not None and lo <= r["score"] < hi]
        if bucket:
            score_acc[label] = round(sum(1 for r in bucket if r["directional_return_pct"] > 0) / len(bucket) * 100, 1)

    # ÚJ: pontszám-sáv bontás TÍPUSONKÉNT is - a felhasználó helyesen
    # rámutatott, hogy az összesített (pooled) bontás félrevezető lehet:
    # ha egy adott jelzéstípus eleve máshogy oszlik el a pontszám-sávokban,
    # a "magas pontszám rosszabb" jelenség lehet, hogy nem a pontszám-
    # számítás hibája, hanem egy adott típus (pl. DIVERGENCE_REVERSAL)
    # felülreprezentáltsága egy adott sávban. Csak min. 3 mintás
    # sáv/típus kombinációt mutatunk, hogy ne legyen zajos.
    MIN_TYPE_SCORE_BUCKET_SAMPLE = 3
    score_acc_by_type = {}
    for t, rs in by_type.items():
        per_type_scored = [r for r in rs if r.get("score") is not None]
        if not per_type_scored:
            continue
        type_buckets = {}
        for label, (lo, hi) in score_buckets.items():
            bucket = [r for r in per_type_scored if lo <= r["score"] < hi]
            if len(bucket) >= MIN_TYPE_SCORE_BUCKET_SAMPLE:
                acc = round(sum(1 for r in bucket if r["directional_return_pct"] > 0) / len(bucket) * 100, 1)
                type_buckets[label] = (acc, len(bucket))
        if type_buckets:
            score_acc_by_type[t] = type_buckets

    hour_buckets = [("00-04", 0, 4), ("04-08", 4, 8), ("08-12", 8, 12), ("12-16", 12, 16), ("16-20", 16, 20), ("20-24", 20, 24)]
    hour_acc = {}
    for label, lo, hi in hour_buckets:
        bucket = []
        for r in finals:
            sig = signals_today.get(r["signal_id"])
            if not sig:
                continue
            h = datetime.fromisoformat(sig["ts"]).astimezone(SUMMARY_TIMEZONE).hour
            if lo <= h < hi:
                bucket.append(r)
        if bucket:
            hour_acc[label] = round(sum(1 for r in bucket if r["directional_return_pct"] > 0) / len(bucket) * 100, 1)

    ttm_medians = {}
    for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
        vals = sorted(r["time_to_move"][str(lvl)] for r in finals if r.get("time_to_move", {}).get(str(lvl)) is not None)
        if vals:
            ttm_medians[lvl] = vals[len(vals) // 2]

    false_signals = [r for r in finals if abs(r["mae_pct"]) > 1.0 and r["mfe_pct"] < 0.3]

    lines = [f"📊 <b>NAPI SIGNAL PERFORMANCE - {today_str}</b> (DAYTRADE 1h)",
             f"\nJelzések száma: {total_signals}"]

    if window_stats:
        lines.append("\n<b>Irány-pontosság időablakonként:</b>")
        for label, _ in AUDIT_WINDOWS_MINUTES:
            if label in window_stats:
                ws = window_stats[label]
                lines.append(f"  {label}: {ws['accuracy_pct']}% (n={ws['n']})")

    if long_acc is not None or short_acc is not None:
        lines.append("\n<b>Irány szerint:</b>")
        if long_acc is not None:
            lines.append(f"  LONG: {long_acc}% (n={len(long_results)})")
        if short_acc is not None:
            lines.append(f"  SHORT: {short_acc}% (n={len(short_results)})")

    lines.append(f"\n<b>Átlag MFE:</b> {avg_mfe:+.2f}%  <b>Átlag MAE:</b> {avg_mae:+.2f}%")
    lines.append(f"<b>Medián MFE:</b> {median_mfe:+.2f}%  <b>Medián MAE:</b> {median_mae:+.2f}%")
    lines.append(f"\n<b>Minősítés:</b> Very Good: {class_counts['VERY_GOOD']} | Good: {class_counts['GOOD']} | "
                 f"Neutral: {class_counts['NEUTRAL']} | Bad: {class_counts['BAD']}")

    if symbol_acc:
        lines.append("\n<b>Coin szerint (min. 2 jelzés):</b>")
        for sym, acc in sorted(symbol_acc.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  {sym}: {acc}%")

    if type_acc:
        lines.append("\n<b>Jelzéstípus szerint:</b>")
        for t, acc in type_acc.items():
            lines.append(f"  {t}: {acc}%")

    if score_acc:
        lines.append("\n<b>Meggyőződés-pontszám sáv szerint (összesítve):</b>")
        for label in ("50-60", "60-70", "70-80", "80+"):
            if label in score_acc:
                lines.append(f"  {label}: {score_acc[label]}%")

    # ÚJ: típusonkénti pontszám-sáv bontás - lásd a fenti kommentet.
    if score_acc_by_type:
        lines.append("\n<b>Meggyőződés-pontszám sáv szerint, TÍPUSONKÉNT:</b>")
        for t, buckets in score_acc_by_type.items():
            lines.append(f"  <b>{t}</b>:")
            for label in ("50-60", "60-70", "70-80", "80+"):
                if label in buckets:
                    acc, n = buckets[label]
                    lines.append(f"    {label}: {acc}% (n={n})")

    if hour_acc:
        lines.append("\n<b>Napszak szerint:</b>")
        for label, _, _ in hour_buckets:
            if label in hour_acc:
                lines.append(f"  {label}: {hour_acc[label]}%")

    if ttm_medians:
        lines.append("\n<b>Medián idő a kedvező mozgás eléréséhez:</b>")
        for lvl in AUDIT_TIME_TO_MOVE_LEVELS_PCT:
            if lvl in ttm_medians:
                lines.append(f"  +{lvl}%: {ttm_medians[lvl]:.0f} perc")

    if false_signals:
        lines.append(f"\n⚠️ <b>Gyanús (fals) jelzések:</b> {len(false_signals)} db "
                     f"(nagy ellenirányú mozgás, minimális kedvező mozgás)")

    return "\n".join(lines)


# ----------------------------------------------------------------------------
# ÚJ: KÜSZÖB-HANGOLÁSI JAVASLAT RENDSZER
# ----------------------------------------------------------------------------
# A felhasználóval egyeztetett terv: az audit-adatokból (MINDEN eddig
# gyűjtött, nem csak a mai napi) automatikusan javaslatot generál, mely
# küszöböket érdemes lenne megfontolni módosítani - de CSAK akkor mond
# bármit is, ha legalább MIN_SUGGESTION_SAMPLE minta van MINDKÉT
# összehasonlított csoportban, hogy ne kis mintából, zajból következtessen.
MIN_SUGGESTION_SAMPLE = 50
MIN_SUGGESTION_GAP_PCT = 0.3  # a directional_return_pct-ban legalább ennyi különbség kell a javaslathoz

# Mezők, amiket a küszöb-javaslat rendszer megvizsgál ennél a botnál -
# ezek a register_signal_audit() hívásoknál átadott `meta` dict kulcsai.
THRESHOLD_SUGGESTION_FIELDS = [
    ("oi_change_pct", "numeric", "OI-növekedés (%)"),
    ("vol_multiplier", "numeric", "Volumen-szorzó"),
    ("htf_aligned", "bool", "HTF-trend egyezés"),
    ("rsi_divergence", "bool", "RSI-divergencia jelenléte"),
    ("cross_bot_confirmed", "bool", "Bot-közi megerősítés"),
    ("wick_rejection_ratio", "numeric", "Kanóc-elutasítás arány"),
    ("sr_distance_pct", "numeric", "Divergencia-fordulat: távolság a szinttől (%)"),
]


def _compute_group_stats(finals_with_meta: list, field: str, kind: str) -> Optional[dict]:
    """Két csoportra bontja a mintát (bool: igaz/hamis; numeric: medián
    fölött/alatt), és összeveti az átlag directional_return_pct-et és a
    találati arányt. None-t ad vissza, ha bármelyik csoport túl kicsi."""
    pairs = [(r, m.get(field)) for r, m in finals_with_meta if m.get(field) is not None]
    if len(pairs) < MIN_SUGGESTION_SAMPLE * 2:
        return None

    if kind == "bool":
        group_hi = [r for r, v in pairs if v]
        group_lo = [r for r, v in pairs if not v]
        split_value = None
    else:
        vals = sorted(v for _, v in pairs)
        split_value = vals[len(vals) // 2]
        group_hi = [r for r, v in pairs if v >= split_value]
        group_lo = [r for r, v in pairs if v < split_value]

    if len(group_hi) < MIN_SUGGESTION_SAMPLE or len(group_lo) < MIN_SUGGESTION_SAMPLE:
        return None

    avg_hi = sum(r["directional_return_pct"] for r in group_hi) / len(group_hi)
    avg_lo = sum(r["directional_return_pct"] for r in group_lo) / len(group_lo)
    win_hi = sum(1 for r in group_hi if r["directional_return_pct"] > 0) / len(group_hi) * 100
    win_lo = sum(1 for r in group_lo if r["directional_return_pct"] > 0) / len(group_lo) * 100

    return {
        "field": field, "kind": kind, "split_value": split_value,
        "n_hi": len(group_hi), "n_lo": len(group_lo),
        "avg_return_hi": avg_hi, "avg_return_lo": avg_lo,
        "win_rate_hi": win_hi, "win_rate_lo": win_lo,
        "gap": avg_hi - avg_lo,
    }


def generate_threshold_suggestions() -> Optional[str]:
    """MINDEN eddig gyűjtött adatot használ (nem csak a mai napot), mert
    megbízható következtetéshez elég nagy mintaszám kell - ez idővel,
    ahogy gyűlik az adat, egyre erősebb lesz. None, ha még nincs elég
    adat BÁRMELYIK mezőhöz."""
    signals = _load_jsonl(AUDIT_SIGNALS_FILE)
    results = _load_jsonl(AUDIT_RESULTS_FILE)
    if not signals or not results:
        return None

    signals_by_id = {s["signal_id"]: s for s in signals}
    window_order = {label: i for i, (label, _) in enumerate(AUDIT_WINDOWS_MINUTES)}
    final_by_signal = {}
    for r in results:
        sid = r["signal_id"]
        if sid not in final_by_signal or window_order.get(r["window"], -1) > window_order.get(final_by_signal[sid]["window"], -1):
            final_by_signal[sid] = r

    finals_with_meta = []
    for sid, r in final_by_signal.items():
        sig = signals_by_id.get(sid)
        if not sig:
            continue
        finals_with_meta.append((r, sig.get("meta", {}) or {}))

    if len(finals_with_meta) < MIN_SUGGESTION_SAMPLE * 2:
        return None

    suggestions = []
    for field, kind, label in THRESHOLD_SUGGESTION_FIELDS:
        stat = _compute_group_stats(finals_with_meta, field, kind)
        if stat is None:
            continue
        if abs(stat["gap"]) < MIN_SUGGESTION_GAP_PCT:
            continue
        if kind == "bool":
            if stat["avg_return_hi"] > stat["avg_return_lo"]:
                suggestions.append(
                    f"• <b>{label}</b>: amikor IGAZ, jobb az átlag hozam "
                    f"({stat['avg_return_hi']:+.2f}% vs {stat['avg_return_lo']:+.2f}%, "
                    f"találati arány {stat['win_rate_hi']:.0f}% vs {stat['win_rate_lo']:.0f}%, "
                    f"n={stat['n_hi']}/{stat['n_lo']})"
                )
            else:
                suggestions.append(
                    f"• <b>{label}</b>: amikor HAMIS, jobb az átlag hozam "
                    f"({stat['avg_return_lo']:+.2f}% vs {stat['avg_return_hi']:+.2f}%, "
                    f"találati arány {stat['win_rate_lo']:.0f}% vs {stat['win_rate_hi']:.0f}%, "
                    f"n={stat['n_lo']}/{stat['n_hi']}) - érdemes megvizsgálni, miért"
                )
        else:
            if stat["avg_return_hi"] > stat["avg_return_lo"]:
                suggestions.append(
                    f"• <b>{label}</b>: a medián ({stat['split_value']:.2f}) FÖLÖTTI jelzések jobban "
                    f"teljesítenek ({stat['avg_return_hi']:+.2f}% vs {stat['avg_return_lo']:+.2f}%, "
                    f"találati arány {stat['win_rate_hi']:.0f}% vs {stat['win_rate_lo']:.0f}%, "
                    f"n={stat['n_hi']}/{stat['n_lo']}) - érdemes lehet a küszöböt a medián közelébe emelni"
                )
            else:
                suggestions.append(
                    f"• <b>{label}</b>: a medián ({stat['split_value']:.2f}) ALATTI jelzések jobban "
                    f"teljesítenek ({stat['avg_return_lo']:+.2f}% vs {stat['avg_return_hi']:+.2f}%, "
                    f"találati arány {stat['win_rate_lo']:.0f}% vs {stat['win_rate_hi']:.0f}%, "
                    f"n={stat['n_lo']}/{stat['n_hi']}) - meglepő, ellenőrizd, miért ront a magas érték"
                )

    if not suggestions:
        return None

    lines = [f"🔧 <b>KÜSZÖB-HANGOLÁSI JAVASLATOK</b> (összesen {len(finals_with_meta)} lezárt jelzés alapján)",
             "⚠️ Statisztikai összefüggések, nem garantált okozati kapcsolatok - "
             "mérlegeld, mielőtt bármit módosítasz.\n"]
    lines.extend(suggestions)
    return "\n".join(lines)


async def maybe_send_daily_audit_report(state: dict, now: datetime) -> None:
    local_now = now.astimezone(SUMMARY_TIMEZONE)
    today_str = local_now.strftime("%Y-%m-%d")
    if local_now.hour < AUDIT_DAILY_REPORT_HOUR:
        return
    if state.get("_audit_report_sent_date") == today_str:
        return
    report = generate_daily_audit_report(now)
    if report:
        # ÚJ: küszöb-hangolási javaslatok csatolása, ha van elég adat.
        suggestions = generate_threshold_suggestions()
        if suggestions:
            report = f"{report}\n\n{suggestions}"
        await send_telegram_message(report)
        logger.info("Napi signal-audit riport elküldve.")
    state["_audit_report_sent_date"] = today_str


def _simulate_trade_outcome(direction: str, entry_price: float, candles: pd.DataFrame) -> dict:
    levels_reached = {lvl: False for lvl in OUTCOME_PROFIT_LEVELS_PCT}
    max_favorable_pct = 0.0
    sl_hit = False

    if direction == "LONG":
        sl_price = entry_price * (1 - OUTCOME_FIXED_SL_PCT / 100)
    else:  
        sl_price = entry_price * (1 + OUTCOME_FIXED_SL_PCT / 100)

    for _, row in candles.iterrows():
        high = float(row["high"])
        low = float(row["low"])

        if direction == "LONG":
            favorable_extreme = high
            adverse_extreme = low
            favorable_pct = (favorable_extreme - entry_price) / entry_price * 100
        else:  
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
    pending = state.get("pending_outcomes", [])
    if not pending:
        return

    due, still_pending = [], []
    for item in pending:
        try:
            window_end = datetime.fromisoformat(item["window_end_ts"])
        except (KeyError, ValueError):
            continue  
        (due if now >= window_end else still_pending).append(item)

    if not due:
        return

    async def _resolve_one(item):
        entry_dt = datetime.fromisoformat(item["entry_ts"])
        window_end_dt = datetime.fromisoformat(item["window_end_ts"])
        symbol, kdf = await fetch_klines(session, klines_semaphore, item["symbol"], "1h", limit=48)
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
                still_pending.append(item)  
            continue
        outcome_label = "LOSS" if result["sl_hit"] else "NEUTRAL"
        _log_signal_outcome({**item, **result, "outcome": outcome_label, "resolved_ts": now.isoformat()})

    state["pending_outcomes"] = still_pending

def _load_log_entries_for_date(date_str: str) -> list:
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
                    continue  
                if rec.get("entry_date") == date_str:
                    entries.append(rec)
    except OSError as e:
        logger.error("Nem sikerült beolvasni a jelzés-naplót: %s", e)
    return entries

def _format_daily_summary(date_str: str, entries: list) -> str:
    SIGNAL_TYPE_ORDER = ["STANDARD", "EARLY"]
    SIGNAL_TYPE_LABELS = {"STANDARD": "🦅 DAYTRADE STD", "EARLY": "🌅 DAYTRADE EARLY"}

    lines = [
        f"📊 <b>[DAYTRADE] Összesítő</b> ({date_str})",
        "(SL = -4.0%-os fix stop-loss 24 órán belül; a %-ok",
        "azt mutatják, hány jelzés érte el a profitszintet SL ELŐTT)",
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
            lines.append(f"{label}: {total} jelzés (nincs adat)")
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

STATE_CLEANUP_STALE_DAYS = 14  

def _cleanup_stale_state_entries(state: dict, now: datetime) -> None:
    cutoff = now - timedelta(days=STATE_CLEANUP_STALE_DAYS)
    stale_symbols = []
    for key, entry in state.items():
        if key.startswith("_") or not isinstance(entry, dict):
            continue
        last_seen = entry.get("last_seen")
        if last_seen is None:
            continue  
        try:
            last_seen_dt = datetime.fromisoformat(last_seen)
        except ValueError:
            continue
        if last_seen_dt < cutoff:
            stale_symbols.append(key)

    for key in stale_symbols:
        del state[key]

async def maybe_send_daily_summary(state: dict, now: datetime) -> None:
    local_now = now.astimezone(SUMMARY_TIMEZONE)
    today_str = local_now.strftime("%Y-%m-%d")
    last_summary_date = state.get("_last_summary_date")

    if last_summary_date is None:
        state["_last_summary_date"] = today_str
        return

    if last_summary_date == today_str:
        return  

    local_midnight_today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_local_midnight = (local_now - local_midnight_today).total_seconds() / 60
    if minutes_since_local_midnight < DAILY_SUMMARY_MIN_DELAY_MINUTES:
        return  

    yesterday_str = (local_now - timedelta(days=1)).strftime("%Y-%m-%d")
    entries = _load_log_entries_for_date(yesterday_str)
    if entries:
        summary_msg = _format_daily_summary(yesterday_str, entries)
        await send_telegram_message(summary_msg)

    state["_last_summary_date"] = today_str
    _rotate_signal_log(yesterday_str)
    _cleanup_stale_state_entries(state, now)

# ----------------------------------------------------------------------------
# ÁLLAPOT KEZELÉSE
# ----------------------------------------------------------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}

def save_state(state: dict) -> None:
    tmp_path = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, STATE_FILE)
    except OSError:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass

# ----------------------------------------------------------------------------
# API HÍVÁSOK
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

async def fetch_funding_rate(session, semaphore, symbol):
    async with semaphore:
        data = await _get_json(session, FUNDING_RATE_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.03)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        payload = data["data"]
        if isinstance(payload, list):
            payload = payload[0] if payload else None
        if not isinstance(payload, dict):
            return symbol, None
        try:
            raw = payload.get("lastFundingRate")
            if raw is None:
                raw = payload.get("fundingRate")
            if raw is None:
                return symbol, None
            return symbol, float(raw) * 100  
        except (TypeError, ValueError, Exception):
            return symbol, None

async def fetch_orderbook_imbalance(symbol: str, limit: int = ORDERBOOK_LEVELS_LIMIT) -> Optional[dict]:
    """Csak akkor hívjuk, amikor egy jelzés éppen kimenne (ritka), ezért
    egy rövid életű, ÖNÁLLÓ session-t nyit - nem éri meg emiatt megnyújtani
    a fő pass session élettartamát, ami minden candidate-re lefutna.
    Visszaadja a bid/ask notional (ár*mennyiség) arányát a legfelső `limit`
    orderbook-szinten, vagy None-t hiba esetén."""
    try:
        async with aiohttp.ClientSession() as session:
            data = await _get_json(session, DEPTH_ENDPOINT, params={"symbol": symbol, "limit": str(limit)})
    except Exception as e:
        logger.warning("Orderbook lekérési hiba (%s): %s", symbol, e)
        return None
    if not data or "data" not in data or not data["data"]:
        return None
    payload = data["data"]
    try:
        bids = payload.get("bids", []) or []
        asks = payload.get("asks", []) or []
        bid_notional = sum(float(p) * float(q) for p, q in bids[:limit])
        ask_notional = sum(float(p) * float(q) for p, q in asks[:limit])
        if bid_notional <= 0 or ask_notional <= 0:
            return None
        return {"bid_ask_ratio": bid_notional / ask_notional, "bid_notional": bid_notional, "ask_notional": ask_notional}
    except (TypeError, ValueError):
        return None

SR_LOOKBACK_PERIOD = 60     
SR_PROXIMITY_PCT = 1.0      

# ÚJ: DIVERGENCE_REVERSAL - önálló jelzéstípus, a STANDARD/EARLY-től
# TELJESEN FÜGGETLEN. A fő trigger itt NEM a volumen-kiugrás, hanem az
# RSI-divergencia (a bot már eddig is számolta, csak pontszám-tényezőként
# használta - most a divergencia maga a belépési ok), MEGERŐSÍTVE azzal,
# hogy a divergencia egy valódi (swing-pont alapú) támasz/ellenállás
# közelében történik. Cél: a mai TAO-USDT esethez hasonló ellentmondásos
# jelzések elkapása MÁSKÉNT - nem tiltással, hanem egy alternatív,
# divergencia-vezérelt setup-ként, amit az audit-rendszer saját
# jelzéstípusként külön mér, így adatból derül ki, jobban teljesít-e.
DIVERGENCE_REVERSAL_SR_PROXIMITY_PCT = 1.5  # a divergenciának ennyi %-on belül kell lennie a szinthez
DIVERGENCE_REVERSAL_MIN_BODY_PCT = 0.15     # a megerősítő gyertyának legalább ennyi %-os testet kell mutatnia
DIVERGENCE_REVERSAL_MIN_CANDLE_VOL_USDT = 20_000  # laza likviditási alapszint - NEM növekedési küszöb


def _find_swing_points(closed: pd.DataFrame, legs: int = SWING_FRACTAL_LEGS) -> list:
    """Fraktál-alapú swing csúcs/mélypont keresés: az i. gyertya akkor
    számít swing csúcsnak, ha a high-ja SZIGORÚAN a legmagasabb a
    [i-legs, i+legs] ablakban (hasonlóan a mélypontra a low-val). Egyedi
    (nem holtversenyes) szélsőértéket keresünk. Visszatér:
    [(index, ár, 'H'|'L'), ...] időrendben."""
    highs = closed["high"].to_numpy()
    lows = closed["low"].to_numpy()
    n = len(highs)
    points = []
    for i in range(legs, n - legs):
        h_window = highs[i - legs:i + legs + 1]
        if highs[i] == h_window.max() and (h_window == highs[i]).sum() == 1:
            points.append((i, float(highs[i]), "H"))
        l_window = lows[i - legs:i + legs + 1]
        if lows[i] == l_window.min() and (l_window == lows[i]).sum() == 1:
            points.append((i, float(lows[i]), "L"))
    points.sort(key=lambda p: p[0])
    return points


def _build_zigzag(swing_points: list) -> list:
    """A nyers swing-pontokból váltakozó (H, L, H, L, ...) zigzag-sorozatot
    épít: ha két egymást követő pont ugyanolyan típusú, csak a
    SZÉLSŐSÉGESEBBET tartjuk meg."""
    zigzag = []
    for idx, price, typ in swing_points:
        if zigzag and zigzag[-1][2] == typ:
            if typ == "H" and price > zigzag[-1][1]:
                zigzag[-1] = (idx, price, typ)
            elif typ == "L" and price < zigzag[-1][1]:
                zigzag[-1] = (idx, price, typ)
        else:
            zigzag.append((idx, price, typ))
    return zigzag


def _classify_structure_trend(zigzag: list) -> Optional[str]:
    """Az utolsó két swing csúcsot és az utolsó két swing mélypontot nézve
    dönti el a trendet."""
    swing_highs = [price for _, price, typ in zigzag if typ == "H"]
    swing_lows = [price for _, price, typ in zigzag if typ == "L"]
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return None  # nincs elég azonosított swing egy megbízható döntéshez

    higher_high = swing_highs[-1] > swing_highs[-2]
    higher_low = swing_lows[-1] > swing_lows[-2]
    lower_high = swing_highs[-1] < swing_highs[-2]
    lower_low = swing_lows[-1] < swing_lows[-2]

    if higher_high and higher_low:
        return "UP"
    if lower_high and lower_low:
        return "DOWN"
    return "NEUTRAL"  # vegyes szerkezet (pl. HH+LL vagy LH+HL) - oldalazás/átmenet


async def fetch_htf_trend(session, semaphore, symbol):
    async with semaphore:
        params = {"symbol": symbol, "interval": HIGHER_TIMEFRAME, "limit": HTF_KLINES_LIMIT}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
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

        closed = df.iloc[:-1]
        min_candles_needed = SWING_FRACTAL_LEGS * 2 + 1
        trend = None
        zigzag = []
        if len(closed) >= min_candles_needed:
            swing_points = _find_swing_points(closed, legs=SWING_FRACTAL_LEGS)
            zigzag = _build_zigzag(swing_points)
            trend = _classify_structure_trend(zigzag)

        # JAVÍTÁS: a régi módszer az utolsó SR_LOOKBACK_PERIOD gyertya
        # NYERS min/max-át adta támasz/ellenállásnak - ez nem valódi,
        # strukturálisan jelentős szintet talál, csak egy ablak-
        # szélsőértéket. Mostantól a MÁR KISZÁMOLT zigzag (swing-pont)
        # adatból a jelenlegi árhoz LEGKÖZELEBBI swing low/high-ot
        # használjuk - ugyanaz a módszer, amit a POC/OB botoknál is
        # bevált, tesztelt megoldásként alkalmazunk.
        support = resistance = None
        if zigzag and len(closed) > 0:
            current_price = float(closed["close"].iloc[-1])
            lows = [p for _, p, t in zigzag if t == "L"]
            highs = [p for _, p, t in zigzag if t == "H"]
            below = [l for l in lows if l < current_price]
            above = [h for h in highs if h > current_price]
            support = max(below) if below else None
            resistance = min(above) if above else None

        return symbol, {"trend": trend, "support": support, "resistance": resistance}

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
# ÚJ: WEBSOCKET ÉLŐ GYERTYA-FIGYELŐ - implementáció
# (azonos logika, mint az alert_checker.py-ban, csak az 1h idősíkra)
# ----------------------------------------------------------------------------

class LiveKlineStore:
    """Symbol -> legfrissebb, websocketen kapott ÉLŐ (még nyitott) gyertya
    adatait tárolja. Egyetlen event loop-ban fut, de a lock a jövőbeli
    biztonság kedvéért (pl. ha valaha több taszkból is írnánk) került be."""
    def __init__(self):
        self._data: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def update(self, symbol: str, candle: dict):
        async with self._lock:
            self._data[symbol] = candle

    def get(self, symbol: str) -> Optional[dict]:
        return self._data.get(symbol)


def _extract_kline_updates(parsed) -> list:
    """A BingX néha nem a doksi-példa szerinti (egyetlen dict, "data"-ban
    egy "K" kulcsú dict) alakban küldi az üzenetet - éles futásban
    ('list' object has no attribute 'get' hiba) kiderült, hogy a "data"
    mező (vagy akár a teljes üzenet) néha LISTA. Ez a függvény minden
    ismert/valószínű alakot normalizál egy egységes
    [(symbol, kline_mezők_dict), ...] listává:
      - parsed lehet dict VAGY lista (több üzenet egy keretben)
      - "data" lehet dict VAGY lista
      - a kline mezők lehetnek "data"/"data"[i] alatt egy "K" kulcs
        alatt, VAGY közvetlenül "data"/"data"[i] szintjén (K nélkül)
    Bármi, ami nem illeszkedik egyik alakra sem, csendben kimarad -
    ez itt egy BEST-EFFORT parser, nem szabad, hogy egy váratlan alak
    miatt az egész kapcsolat összeomoljon."""
    results = []
    items = parsed if isinstance(parsed, list) else [parsed]
    for item in items:
        if not isinstance(item, dict):
            continue
        data_type = item.get("dataType", "")
        if "@kline_" not in data_type:
            continue
        symbol = data_type.split("@", 1)[0]
        data = item.get("data")
        if data is None:
            continue
        data_items = data if isinstance(data, list) else [data]
        for d in data_items:
            if not isinstance(d, dict):
                continue
            k = d.get("K") if isinstance(d.get("K"), dict) else d
            results.append((symbol, k))
    return results


async def _handle_ws_kline_message(payload, store: LiveKlineStore) -> None:
    """A BingX kline push-üzenetének feldolgozása - lásd
    _extract_kline_updates() kommentjét a rugalmas alak-kezelésről."""
    for symbol, k in _extract_kline_updates(payload):
        try:
            candle = {
                "open": float(k.get("o")),
                "high": float(k.get("h")),
                "low": float(k.get("l")),
                "close": float(k.get("c")),
                "volume_base": float(k.get("v") or 0),
                "open_time_ms": int(k.get("t")),
                "received_at_ms": time.time() * 1000,
            }
        except (TypeError, ValueError, AttributeError):
            continue
        await store.update(symbol, candle)


async def _ws_kline_listener(symbols: list, store: LiveKlineStore, stop_event: asyncio.Event) -> None:
    """Egyetlen websocket-kapcsolatot tart fent a megadott symbol-listára,
    a gyertya-adatokat a store-ba írja. Kapcsolat-megszakadás esetén
    automatikusan újracsatlakozik, amíg a stop_event be nem áll (a futás
    végén a _run_main_loop állítja be)."""
    while not stop_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, timeout=WS_CONNECT_TIMEOUT_SECONDS) as ws:
                    for sym in symbols:
                        sub_msg = {
                            "id": f"sub-{sym}-{WS_KLINE_INTERVAL}",
                            "reqType": "sub",
                            "dataType": f"{sym}@kline_{WS_KLINE_INTERVAL}",
                        }
                        await ws.send_json(sub_msg)
                        await asyncio.sleep(0.02)  # ne zúduljon rá egyszerre a szerverre

                    async for msg in ws:
                        if stop_event.is_set():
                            break
                        text = None
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            try:
                                text = gzip.decompress(msg.data).decode("utf-8")
                            except Exception:
                                continue
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            text = msg.data
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        else:
                            continue

                        if text is None:
                            continue
                        if text == "Ping":
                            await ws.send_str("Pong")
                            continue

                        try:
                            parsed = json.loads(text)
                        except json.JSONDecodeError:
                            continue
                        # ÚJ: egy-egy üzenet feldolgozási hibája (pl. egy
                        # váratlan, eddig nem látott üzenet-alak) NE dobja
                        # szét a TELJES kapcsolatot - csak azt az egy
                        # üzenetet hagyjuk ki, a kapcsolat és a többi
                        # symbol feliratkozása élve marad.
                        try:
                            await _handle_ws_kline_message(parsed, store)
                        except Exception as e:
                            logger.debug("WS üzenet feldolgozási hiba (kihagyva): %s", e)
                            continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("WS kline-figyelő megszakadt (%d symbol) - %.0f mp múlva újracsatlakozás: %s",
                            len(symbols), WS_RECONNECT_DELAY_SECONDS, e)
        if stop_event.is_set():
            break
        await asyncio.sleep(WS_RECONNECT_DELAY_SECONDS)


async def _start_ws_kline_listeners(stop_event: asyncio.Event):
    """Egyszeri, futás-eleji candidate-lista lekérdezés, majd a listát
    WS_SYMBOLS_PER_CONNECTION-es darabokra bontva egy-egy websocket-
    kapcsolatot (taszkot) indít mindegyikre. A candidate-lista a futás
    TELJES idejére rögzített marad - ha közben egy új symbol lépne be a
    24h volumen-szűrőn, arra nem lesz élő WS-adat ebben a futásban (a
    REST-alapú útra esik vissza, ami a jelenlegi, változatlan viselkedés)."""
    try:
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            tickers = await fetch_all_tickers(session)
            valid_contracts = await fetch_valid_contract_symbols(session)
    except Exception as e:
        logger.warning("WS-figyelőhöz szükséges candidate-lista lekérése sikertelen - WS kikapcsolva ebben a futásban: %s", e)
        return None, []

    if not tickers:
        return None, []

    candidates = []
    for s, info in tickers.items():
        if not (MIN_VOLUME_USDT <= info["quote_volume_24h"] <= MAX_VOLUME_USDT):
            continue
        if not is_probably_crypto(s):
            continue
        if valid_contracts is not None and s not in valid_contracts:
            continue
        candidates.append(s)

    if not candidates:
        return None, []

    store = LiveKlineStore()
    tasks = []
    for i in range(0, len(candidates), WS_SYMBOLS_PER_CONNECTION):
        chunk = candidates[i:i + WS_SYMBOLS_PER_CONNECTION]
        tasks.append(asyncio.create_task(_ws_kline_listener(chunk, store, stop_event)))

    logger.info("WS kline-figyelő elindítva: %d symbol, %d kapcsolat (%s idősík).",
                len(candidates), len(tasks), WS_KLINE_INTERVAL)
    return store, tasks


def _patch_live_candle_with_ws(kdf: pd.DataFrame, symbol: str, store: Optional["LiveKlineStore"]) -> tuple:
    """Ha van elég friss (lásd WS_MAX_STALENESS_SECONDS) websocket-adat
    erre a symbolra, ÉS az ugyanarra a gyertya-periódusra vonatkozik (a
    nyitási időbélyeg egyezik, különben rossz gyertyát patchelnénk egy
    időszak-váltás pillanatában), felülírja a DataFrame UTOLSÓ (élő)
    sorának close/high/low/volume mezőit a frissebb WS-értékekkel. A
    lezárt gyertyák és a timestamp VÁLTOZATLANOK maradnak, tehát az
    evaluate_candle() logikáján semmit nem kell módosítani ehhez.

    Visszatér: (kdf, patched: bool) - a bool-t a run_single_pass() a
    kör végi diagnosztikai összesítőhöz (log-sor) használja."""
    if store is None or kdf is None or len(kdf) == 0:
        return kdf, False
    live_ws = store.get(symbol)
    if live_ws is None:
        return kdf, False

    age_seconds = (time.time() * 1000 - live_ws["received_at_ms"]) / 1000
    if age_seconds > WS_MAX_STALENESS_SECONDS:
        return kdf, False

    last_idx = kdf.index[-1]
    try:
        last_ts = kdf.loc[last_idx, "timestamp"].to_pydatetime().replace(tzinfo=timezone.utc)
        ws_open_dt = datetime.fromtimestamp(live_ws["open_time_ms"] / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return kdf, False
    if abs((last_ts - ws_open_dt).total_seconds()) > 5:
        # a WS és a REST nem ugyanarra a gyertya-periódusra mutat (pl.
        # épp most zárult le/nyílt egy új gyertya) - inkább kihagyjuk,
        # mint hogy rossz sort patcheljünk
        return kdf, False

    kdf = kdf.copy()
    # Védekezés: ha a REST-forrás DataFrame-ben ezek az oszlopok bármiért
    # nem float dtype-ok lennének, a WS-ből jövő float érték beírása
    # pandas-hibát dobna ("Invalid value for dtype int64") - ezért itt
    # explicit float64-re kényszerítjük ezt a négy oszlopot.
    for col in ("close", "high", "low", "volume"):
        kdf[col] = kdf[col].astype("float64")
    kdf.loc[last_idx, "close"] = live_ws["close"]
    if live_ws["high"] > kdf.loc[last_idx, "high"]:
        kdf.loc[last_idx, "high"] = live_ws["high"]
    if live_ws["low"] < kdf.loc[last_idx, "low"]:
        kdf.loc[last_idx, "low"] = live_ws["low"]
    kdf.loc[last_idx, "volume"] = live_ws["volume_base"]
    return kdf, True

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


# ----------------------------------------------------------------------------
# ÚJ: MEGGYŐZŐDÉS-PONTSZÁM (confidence score)
# ----------------------------------------------------------------------------
# A cél NEM a kereskedés automatizálása (nincs SL/TP, nincs pozíció-kezelés),
# hanem hogy a már meglévő, egyenként megjelenő jelzőket (HTF-trend, S/R,
# funding, orderbook, MACD, RSI, volumen-erő) EGY összesített, gyorsan
# értelmezhető számmá/címkévé sűrítsük - a döntést továbbra is a
# felhasználó hozza meg, ez csak segít gyorsabban átlátni, hány jelző
# mutat EGY IRÁNYBA.
#
# A pontszám egy 50-ös (semleges) alapértékről indul, és minden
# rendelkezésre álló jelző +/- pontot ad hozzá attól függően, hogy
# EGYEZIK vagy ELLENTMOND-e a jelzés irányával. Fontos: ha egy adott
# jelző (pl. orderbook, mert csak a kimenő jelzésekhez kérdezzük le)
# egyszerűen nincs adat, azt a jelzőt NEM számítjuk bele (se pluszban,
# se mínuszban) - a hiányzó adat nem büntetés.
CONFIDENCE_BASELINE = 50
CONFIDENCE_STRONG_THRESHOLD = 70
CONFIDENCE_WEAK_THRESHOLD = 40

def compute_confidence_score(direction, htf_trend=None, bounce_confluence=False,
                               near_level_risk=False, funding_rate=None,
                               funding_delta_pct=None, orderbook_info=None,
                               macd_status=None, rsi=None, vol_multiplier=None,
                               cross_bot_confirmations=None, divergence=None,
                               vwap_relation=None, historical_stats=None,
                               wick_rejection_ratio=None, oi_change_pct=None) -> tuple:
    """Visszatér: (score: int 0-100, label: str, factors: list[str]).
    A factors lista a pontszám összetevőit sorolja fel - ez KERÜL bele az
    üzenetbe is, hogy ne "fekete doboz" számként érkezzen, hanem lásd is,
    MIÉRT annyi, amennyi."""
    score = CONFIDENCE_BASELINE
    factors = []

    if htf_trend == "UP" and direction == "LONG":
        score += 15; factors.append("+15 HTF trend egyezik")
    elif htf_trend == "DOWN" and direction == "SHORT":
        score += 15; factors.append("+15 HTF trend egyezik")
    elif htf_trend == "UP" and direction == "SHORT":
        score -= 15; factors.append("-15 HTF trend ellenez")
    elif htf_trend == "DOWN" and direction == "LONG":
        score -= 15; factors.append("-15 HTF trend ellenez")

    if bounce_confluence:
        score += 10; factors.append("+10 támasz/ellenállás egyezik")
    if near_level_risk:
        score -= 10; factors.append("-10 szemközti szint közelében (elutasítás-kockázat)")

    if funding_rate is not None:
        if direction == "LONG" and funding_rate <= -FUNDING_SQUEEZE_THRESHOLD_PCT:
            score += 10; factors.append("+10 short squeeze funding")
        elif direction == "SHORT" and funding_rate >= FUNDING_SQUEEZE_THRESHOLD_PCT:
            score += 10; factors.append("+10 long squeeze funding")

    if funding_delta_pct is not None:
        if direction == "LONG" and funding_delta_pct <= -FUNDING_ACCEL_THRESHOLD_PCT:
            score += 8; factors.append("+8 gyorsuló funding az irány felé")
        elif direction == "SHORT" and funding_delta_pct >= FUNDING_ACCEL_THRESHOLD_PCT:
            score += 8; factors.append("+8 gyorsuló funding az irány felé")

    if orderbook_info is not None:
        ratio = orderbook_info["bid_ask_ratio"]
        if ratio >= ORDERBOOK_IMBALANCE_THRESHOLD:
            if direction == "LONG":
                score += 10; factors.append("+10 orderbook egyezik (vékony ask)")
            else:
                score -= 10; factors.append("-10 orderbook ellenez (vékony ask, de SHORT)")
        elif ratio <= 1 / ORDERBOOK_IMBALANCE_THRESHOLD:
            if direction == "SHORT":
                score += 10; factors.append("+10 orderbook egyezik (vékony bid)")
            else:
                score -= 10; factors.append("-10 orderbook ellenez (vékony bid, de LONG)")

    if macd_status:
        bullish = macd_status in ("Bullish Cross", "Bullish")
        bearish = macd_status in ("Bearish Cross", "Bearish")
        if bullish and direction == "LONG":
            score += 8; factors.append("+8 MACD egyezik")
        elif bearish and direction == "SHORT":
            score += 8; factors.append("+8 MACD egyezik")
        elif bullish and direction == "SHORT":
            score -= 8; factors.append("-8 MACD ellenez")
        elif bearish and direction == "LONG":
            score -= 8; factors.append("-8 MACD ellenez")

    if rsi is not None:
        if direction == "LONG" and rsi >= 75:
            score -= 5; factors.append("-5 RSI túlvett (fordulat-kockázat)")
        elif direction == "SHORT" and rsi <= 25:
            score -= 5; factors.append("-5 RSI túladott (fordulat-kockázat)")

    # JAVÍTÁS (adat alapján, 2026-09-06): korábban a KIEMELKEDŐEN magas
    # volumen-szorzó (+5) jutalmat kapott. A küszöb-hangolási rendszer
    # 510 lezárt jelzésen (55/55 mintán) az ELLENKEZŐJÉT mutatta: a
    # medián (2.65) ALATTI szorzójú jelzések teljesítettek jobban
    # (+1.05% vs +0.57%, 53% vs 38% találati arány) - ugyanaz a mintázat,
    # mint amit a scalp-nál (alert_checker.py) már korábban megfordítottunk.
    if vol_multiplier is not None and vol_multiplier >= 2 * MIN_VOL_MULTIPLIER:
        score -= 8; factors.append("-8 szokatlanul magas volumen (lehetséges kifulladás - climax gyertya)")

    # ÚJ (adat alapján, 2026-09-06): az OI-növekedés eddig NEM számított
    # bele a pontszámba (csak a tüzelési küszöbnél). A küszöb-hangolási
    # rendszer 510 lezárt jelzésen (55/55 mintán) azt mutatta, hogy a
    # medián (6.95%) ALATTI OI-növekedésű jelzések jobban teljesítenek
    # (+1.00% vs +0.63%, 44% vs 47% találati arány - utóbbi szám
    # megtévesztő, az átlag hozam a lényeg). Szimmetrikusan a volumen-
    # szorzóval, a SZOKATLANUL magas OI-ugrást is inkább kockázatnak,
    # mint megerősítésnek tekintjük mostantól.
    if oi_change_pct is not None and oi_change_pct >= 2 * MIN_OI_INCREASE:
        score -= 8; factors.append("-8 szokatlanul magas OI-ugrás (lehetséges kifulladás)")

    # ÚJ: bot-közi megerősítés - lásd get_cross_bot_confirmations() kommentjét.
    # Erősebb súlyú, mint az egyedi jelzők, mert ez egy TELJESEN FÜGGETLEN
    # botból/adatforrásból jövő megerősítés, nem csak a sajátunk egy másik
    # metrikája - de max +20-ra korlátozzuk, hogy egyetlen tényező se
    # tudja önmagában "átbillenteni" a pontszámot 0/100-ra.
    if cross_bot_confirmations:
        bonus = min(20, 12 * len(cross_bot_confirmations))
        score += bonus
        factors.append(f"+{bonus} bot-közi megerősítés ({len(cross_bot_confirmations)}x)")

    # ÚJ: RSI/ár divergencia - lásd detect_rsi_divergence() kommentjét.
    if divergence == "BULLISH" and direction == "LONG":
        score += 10; factors.append("+10 RSI bullish divergencia egyezik")
    elif divergence == "BEARISH" and direction == "SHORT":
        score += 10; factors.append("+10 RSI bearish divergencia egyezik")
    elif divergence == "BULLISH" and direction == "SHORT":
        score -= 10; factors.append("-10 RSI bullish divergencia ellenez (lehet forduló)")
    elif divergence == "BEARISH" and direction == "LONG":
        score -= 10; factors.append("-10 RSI bearish divergencia ellenez (lehet forduló)")

    # ÚJ: VWAP-viszony - sok day trader csak akkor keres LONG-ot, ha az ár
    # a (gördülő) VWAP fölött van, és fordítva SHORT-nál.
    if vwap_relation == "ABOVE" and direction == "LONG":
        score += 7; factors.append("+7 ár a VWAP fölött (egyezik)")
    elif vwap_relation == "BELOW" and direction == "SHORT":
        score += 7; factors.append("+7 ár a VWAP alatt (egyezik)")
    elif vwap_relation == "BELOW" and direction == "LONG":
        score -= 7; factors.append("-7 ár a VWAP alatt (ellenez)")
    elif vwap_relation == "ABOVE" and direction == "SHORT":
        score -= 7; factors.append("-7 ár a VWAP fölött (ellenez)")

    # ÚJ: historikus kimenetel-statisztika - lásd compute_historical_stats()
    # kommentjét. Szerényebb súly (max +-8), mert ez nem az AKTUÁLIS
    # jelzésről, hanem a TÍPUS múltbeli átlagos teljesítményéről mond
    # valamit - fontos kontextus, de nem helyettesíti a friss jelzőket.
    if historical_stats is not None and historical_stats.get("win_rate_pct") is not None:
        win_rate = historical_stats["win_rate_pct"]
        if win_rate >= 65:
            score += 8; factors.append(f"+8 historikusan erős típus ({win_rate:.0f}% találati arány)")
        elif win_rate <= 35:
            score -= 8; factors.append(f"-8 historikusan gyenge típus ({win_rate:.0f}% találati arány)")

    # ÚJ: kanóc-elutasítás - lásd az evaluate_candle() blokk-kommentjét a
    # teljes indoklásért. Ez a felhasználó által konkrétan megfigyelt
    # mintázat (jelzés kimegy, a KÖVETKEZŐ gyertya visszazár alá) ellen
    # véd - erősebb súllyal, mert ez egy közvetlenül megfigyelt,
    # visszatérő probléma volt, nem csak elméleti feltételezés.
    if wick_rejection_ratio is not None:
        if wick_rejection_ratio >= 1.5:
            score -= 20; factors.append(f"-20 erős kanóc-elutasítás a jelző gyertyán ({wick_rejection_ratio:.1f}x test)")
        elif wick_rejection_ratio >= 0.8:
            score -= 10; factors.append(f"-10 kanóc-elutasítás a jelző gyertyán ({wick_rejection_ratio:.1f}x test)")

    score = max(0, min(100, score))
    if score >= CONFIDENCE_STRONG_THRESHOLD:
        label = "🟢"  # ÚJ: csak a szín-pont, szöveg/szám nélkül (a felhasználó kérésére)
    elif score >= CONFIDENCE_WEAK_THRESHOLD:
        label = "🟡"
    else:
        label = "🔴"
    return score, label, factors


def format_daytrade_message(symbol, direction, price, price_change_pct, candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct, htf_trend=None, bounce_confluence=False, near_level_risk=False, rsi=None, macd_status=None, signal_type="STANDARD", funding_rate=None, pace_vol_multiplier=None, elapsed_fraction=None, funding_delta_pct=None, orderbook_info=None, cross_bot_confirmations=None, divergence=None, vwap=None, vwap_relation=None, vwap_diff_pct=None, historical_stats=None, wick_rejection_ratio=None):
    action = DIRECTION_LABELS.get(direction, direction)

    # ÚJ: a meggyőződés-pontszám (compute_confidence_score) mostantól CSAK
    # egy szín-pontként (🟢/🟡/🔴) jelenik meg a fejlécben - a felhasználó
    # kérésére a szám és a részletes tényezőlista (ami félrevezetően
    # "objektívnek" tűnhetett, holott a súlyok nem backteszteltek)
    # KIKERÜLT az üzenetből. A pontszám maga változatlanul számít a
    # háttérben, csak nem jelenítjük meg a részleteit.
    score, score_label, score_factors = compute_confidence_score(
        direction, htf_trend=htf_trend, bounce_confluence=bounce_confluence,
        near_level_risk=near_level_risk, funding_rate=funding_rate,
        funding_delta_pct=funding_delta_pct, orderbook_info=orderbook_info,
        macd_status=macd_status, rsi=rsi, vol_multiplier=vol_multiplier,
        cross_bot_confirmations=cross_bot_confirmations, divergence=divergence,
        vwap_relation=vwap_relation, historical_stats=historical_stats,
        wick_rejection_ratio=wick_rejection_ratio, oi_change_pct=oi_change_pct,
    )

    if signal_type == "EARLY":
        header = f"🌅 <b>[DAYTRADE] {symbol}</b> {action} (KORAI 1H) {score_label}"
    elif signal_type == "DIVERGENCE_REVERSAL":
        header = f"🔀 <b>[DAYTRADE] {symbol}</b> {action} (DIVERGENCIA-FORDULAT 1H) {score_label}"
    else:
        header = f"🦅 <b>[DAYTRADE] {symbol}</b> {action} (STANDARD 1H) {score_label}"

    cross_line = ""
    if cross_bot_confirmations:
        cross_line = f"\n🔥 Megerősítve: {', '.join(cross_bot_confirmations)}"

    divergence_line = ""
    if divergence == "BULLISH":
        divergence_line = "\n🔀 RSI bullish divergencia (ár lower-low, RSI higher-low - eséskifulladás jele)"
    elif divergence == "BEARISH":
        divergence_line = "\n🔀 RSI bearish divergencia (ár higher-high, RSI lower-high - felfutás-kifulladás jele)"

    # ÚJ: historikus kimenetel-statisztika sor.
    historical_line = ""
    if historical_stats is not None:
        hs = historical_stats
        win_txt = f"{hs['win_rate_pct']:.0f}%" if hs.get("win_rate_pct") is not None else "n/a"
        avg_txt = f"{hs['avg_max_favorable_pct']:+.2f}%" if hs.get("avg_max_favorable_pct") is not None else "n/a"
        historical_line = (
            f"\n📊 Historikus ({hs['total']} hasonló, {HISTORICAL_STATS_LOOKBACK_DAYS}nap): "
            f"{win_txt} érte el a +{hs['profit_level_pct']}%-ot, átlag max {avg_txt}, SL-találat {hs['loss_rate_pct']:.0f}%"
        )

    vwap_line = ""
    if vwap is not None and vwap_relation is not None:
        rel_txt = "fölött" if vwap_relation == "ABOVE" else "alatt"
        note = " ✅ egyezik az iránnyal" if (
            (vwap_relation == "ABOVE" and direction == "LONG") or (vwap_relation == "BELOW" and direction == "SHORT")
        ) else " ⚠️ iránnyal szemben"
        diff_txt = f" ({vwap_diff_pct:+.2f}%)" if vwap_diff_pct is not None else ""
        vwap_line = f"\n📏 VWAP: {vwap:.6f} - az ár a VWAP {rel_txt}{diff_txt}{note}"

    early_line = ""
    if signal_type == "EARLY":
        pace_note = f", vetített ütem: {pace_vol_multiplier:.1f}x" if pace_vol_multiplier is not None else ""
        elapsed_note = f" (a gyertya ~{elapsed_fraction * 100:.0f}%-ánál)" if elapsed_fraction is not None else ""
        early_line = f"\n🔬 Korai jelzés{pace_note}{elapsed_note}"
    elif signal_type == "DIVERGENCE_REVERSAL":
        early_line = (
            f"\nℹ️ Önálló setup - NEM volumen-alapú: RSI-divergencia + valódi "
            f"(swing-pont alapú) szint-közelség adja a jelzést, a STANDARD/EARLY "
            f"logikától teljesen függetlenül."
        )

    warning_line = ""
    against_trend = ((direction == "LONG" and htf_trend == "DOWN") or (direction == "SHORT" and htf_trend == "UP"))
    if against_trend:
        warning_line = f"\n⚠️ Trenddel szemben (4h: {htf_trend})"

    bounce_line = ""
    if bounce_confluence:
        level_type = "támaszról" if direction == "LONG" else "ellenállásról"
        bounce_line = f"\n🎯 Szint-visszapattanás ({level_type}, 4h-s csatorna)"

    risk_line = ""
    if near_level_risk:
        level_type = "ellenállás" if direction == "LONG" else "támasz"
        risk_line = f"\n⚠️ Közeli {level_type} (4h-s csatorna) - onnan visszapattanhat!"

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

    funding_line = ""
    if funding_rate is not None:
        funding_line = f"\n💸 Funding: {funding_rate:+.4f}%"
        if direction == "LONG" and funding_rate <= -FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 SHORT SQUEEZE"
        elif direction == "SHORT" and funding_rate >= FUNDING_SQUEEZE_THRESHOLD_PCT:
            funding_line += " 💥 LONG SQUEEZE"
        # ÚJ: funding rate gyorsulás - ha a rate az elmúlt kb.
        # FUNDING_HISTORY_TARGET_MINUTES alatt legalább FUNDING_ACCEL_THRESHOLD_PCT
        # %-pontot mozdult, kiemeljük (tájékoztató jelleggel, nem szűr).
        if funding_delta_pct is not None and abs(funding_delta_pct) >= FUNDING_ACCEL_THRESHOLD_PCT:
            arrow = "↓" if funding_delta_pct < 0 else "↑"
            funding_line += f" | ⚡ gyorsuló ({arrow}{abs(funding_delta_pct):.4f}%pont/~{FUNDING_HISTORY_TARGET_MINUTES}p)"

    # ÚJ: Orderbook imbalance sor - csak akkor van adat, ha ténylegesen
    # lekértük (lásd fetch_orderbook_imbalance hívási pontját). Tisztán
    # tájékoztató, nem szűr.
    orderbook_line = ""
    if orderbook_info is not None:
        ratio = orderbook_info["bid_ask_ratio"]
        if ratio >= ORDERBOOK_IMBALANCE_THRESHOLD:
            note = " ✅ egyezik az iránnyal" if direction == "LONG" else " ⚠️ iránnyal szemben (erős vétel a shorttal szemben)"
            orderbook_line = f"\n📗 Orderbook: vékony ask / vastag bid ({ratio:.1f}x){note}"
        elif ratio <= 1 / ORDERBOOK_IMBALANCE_THRESHOLD:
            inv_ratio = 1 / ratio
            note = " ✅ egyezik az iránnyal" if direction == "SHORT" else " ⚠️ iránnyal szemben (erős eladás a longgal szemben)"
            orderbook_line = f"\n📕 Orderbook: vékony bid / vastag ask ({inv_ratio:.1f}x){note}"

    body = (
        f"{header}\n"
        f"💰 Ár: {price:.6f} ({price_change_pct:+.2f}%)\n"
        f"📊 Vol: {candle_vol_usdt:,.0f} USDT ({vol_multiplier:.1f}x átlag)\n"
        f"🧲 OI: {oi_value:,.0f} ({oi_change_pct:+.2f}%)"
        f"{early_line}"
        f"{indicator_line}"
        f"{funding_line}"
        f"{orderbook_line}"
        f"{cross_line}"
        f"{divergence_line}"
        f"{vwap_line}"
        f"{historical_line}"
        f"{warning_line}"
        f"{bounce_line}"
        f"{risk_line}"
    )
    return f"\n{body}\n"



def find_oi_baseline(history_without_current: list, now: datetime, target_minutes: float = None, min_minutes: float = None, max_minutes: float = None) -> Optional["OiBaseline"]:
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

def find_funding_baseline(history_without_current: list, now: datetime, target_minutes: float = None, min_minutes: float = None, max_minutes: float = None) -> Optional[dict]:
    """A find_oi_baseline() funding-rate megfelelője: a target_minutes-hez
    legközelebbi, [min_minutes, max_minutes] ablakba eső korábbi funding
    rate bejegyzést adja vissza, amiből a delta (gyorsulás) számolható."""
    target = FUNDING_HISTORY_TARGET_MINUTES if target_minutes is None else target_minutes
    min_w = FUNDING_HISTORY_MIN_MINUTES if min_minutes is None else min_minutes
    max_w = FUNDING_HISTORY_MAX_MINUTES if max_minutes is None else max_minutes
    best, best_diff = None, None
    for h in history_without_current:
        age_min = (now - datetime.fromisoformat(h["ts"])).total_seconds() / 60
        if min_w <= age_min <= max_w:
            diff = abs(age_min - target)
            if best_diff is None or diff < best_diff:
                best, best_diff = h, diff
    return best

def compute_rsi_series(close_series: pd.Series) -> Optional[pd.Series]:
    """Ugyanaz az RSI-számítás, mint compute_rsi_macd()-ban, de a TELJES
    sorozatot adja vissza (nem csak az utolsó értéket) - a divergencia-
    kereséshez kell, hogy korábbi swing-pontokon is meg tudjuk nézni az
    RSI értékét, nem csak a jelenlegit."""
    if len(close_series) < 35:
        return None
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi_series = 100 - (100 / (1 + rs))
    rsi_series = rsi_series.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    return rsi_series


DIVERGENCE_LOOKBACK_PERIOD = 40   # ennyi LEZÁRT gyertyán belül keresünk swing-pontokat
DIVERGENCE_SWING_LEGS = 4          # 2 -> 4: a felhasználó élő megfigyelése alapján
# (PLUME-USDT eset) a 2-es érték túl érzékeny volt - apró, jelentéktelen
# blip-eket is "swing-pontnak" fogadott el, amiket vizuálisan nem lehetett
# a charton valódi mélypontként/csúcsként azonosítani. A 4-es szigorúbb:
# egy pontnak 9 gyertya közül kell a legszélsőségesebbnek lennie.

def detect_rsi_divergence(closed: pd.DataFrame, rsi_series: pd.Series,
                            legs: int = DIVERGENCE_SWING_LEGS,
                            lookback: int = DIVERGENCE_LOOKBACK_PERIOD,
                            current_price: Optional[float] = None) -> Optional[str]:
    """RSI/ár divergencia keresése az utolsó `lookback` LEZÁRT gyertyán
    (a még formálódó élő gyertyát szándékosan kihagyjuk, mert a high/low
    még változhat, zajossá tenné a swing-detektálást).

    - "BEARISH": az ár újabb (magasabb) csúcsot ír, de az RSI ALACSONYABB
      csúcsot ír ugyanott -> a felfutás "kifullad", fordulat-kockázat.
    - "BULLISH": az ár újabb (mélyebb) mélypontot ír, de az RSI MAGASABB
      mélypontot ír ugyanott -> az esés "kifullad", fordulat-kockázat.
    - None: nincs elég swing-pont, vagy nincs divergencia.

    JAVÍTÁS (élesben megfigyelt hiba): a fraktál-alapú swing-keresés csak
    UTÓLAG, `legs` gyertyával később erősít meg egy csúcsot/mélypontot -
    ha az ár AZÓTA (a jelenlegi, élő gyertyáig) MÁR TÚLLÉPTE azt a
    csúcsot/mélypontot, amivel összehasonlítottunk, a divergencia
    ELAVULT: a valódi "tető"/"alj" még nem alakult ki, a mozgás még tart.
    Ilyenkor NEM adunk vissza divergenciát, még ha a két RÉGI swing-pont
    között technikailag fennállna is a mintázat - lásd a `current_price`
    paramétert, ami ezt az ellenőrzést végzi."""
    if closed is None or rsi_series is None or len(closed) < lookback:
        return None
    window = closed.iloc[-lookback:].reset_index(drop=True)
    rsi_window = rsi_series.iloc[-lookback:].reset_index(drop=True)
    swing_points = _find_swing_points(window, legs=legs)
    highs = [(idx, price) for idx, price, typ in swing_points if typ == "H"]
    lows = [(idx, price) for idx, price, typ in swing_points if typ == "L"]

    if len(highs) >= 2:
        (idx1, price1), (idx2, price2) = highs[-2], highs[-1]
        rsi1, rsi2 = rsi_window.iloc[idx1], rsi_window.iloc[idx2]
        # ÚJ: elavultság-ellenőrzés - ha az élő ár már túllépte a
        # legutóbbi (highs[-1]) csúcsot, ez a divergencia elavult.
        stale = current_price is not None and current_price > price2
        if pd.notna(rsi1) and pd.notna(rsi2) and price2 > price1 and rsi2 < rsi1 and not stale:
            return "BEARISH"

    if len(lows) >= 2:
        (idx1, price1), (idx2, price2) = lows[-2], lows[-1]
        rsi1, rsi2 = rsi_window.iloc[idx1], rsi_window.iloc[idx2]
        # ÚJ: elavultság-ellenőrzés - ha az élő ár már a legutóbbi
        # (lows[-1]) mélypont ALÁ esett, ez a divergencia elavult.
        stale = current_price is not None and current_price < price2
        if pd.notna(rsi1) and pd.notna(rsi2) and price2 < price1 and rsi2 > rsi1 and not stale:
            return "BULLISH"

    return None


def compute_rsi_macd(close_series: pd.Series):
    if len(close_series) < 35:
        return None, None
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi_series = 100 - (100 / (1 + rs))
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

def compute_vwap(kdf: pd.DataFrame) -> Optional[float]:
    """Gördülő VWAP (Volume Weighted Average Price) a TELJES megkapott
    gyertya-ablakon (lezártak + élő), typical price (H+L+C)/3 súlyozva a
    volumennel. FONTOS: ez NEM a klasszikus, UTC nap-eleji nullázódású
    VWAP - mivel a botok csak egy korlátozott számú gyertyát kérnek le
    (nem a teljes napot), egy GÖRDÜLŐ VWAP-ot számolunk a rendelkezésre
    álló ablakon. Ez semmilyen új API-hívást nem igényel (a kdf már
    úgyis megvan), és gyakorlati szempontból hasonló infót ad: az ár a
    közelmúlt volumen-súlyozott átlagárához képest hol áll."""
    if kdf is None or len(kdf) == 0:
        return None
    typical_price = (kdf["high"] + kdf["low"] + kdf["close"]) / 3
    vol = kdf["volume"]
    total_vol = vol.sum()
    if total_vol is None or pd.isna(total_vol) or total_vol <= 0:
        return None
    vwap = (typical_price * vol).sum() / total_vol
    return float(vwap) if pd.notna(vwap) else None


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

    # ÚJ: KANÓC-ELUTASÍTÁS ellenőrzés. A felhasználó konkrét megfigyelése:
    # "kimegy egy pump jelzés egy gyertyára, a KÖVETKEZŐ gyertya már
    # visszazár alá" - ennek klasszikus előjele, ha a JELZŐ gyertyának
    # már MOST, a jelzés pillanatában is aránytalanul nagy kanóca van a
    # test irányával ELLENTÉTES oldalon (pl. LONG-nál nagy felső kanóc a
    # test fölött) - ez azt jelenti, hogy a piac MÁR a gyertyán belül
    # visszaverte a nyomást, csak ez a nyitó-záró %-ból és a volumenből
    # önmagában nem látszik. A meggyőződés-pontszámba számít be (a
    # színes pontot befolyásolja), nem ad új szöveges sort az üzenetbe.
    live_high = float(live["high"])
    live_low = float(live["low"])
    live_open = float(live["open"])
    body = abs(current_price - live_open)
    body_safe = max(body, current_price * 0.0005)  # nullával osztás elleni védelem nagyon kis testű gyertyáknál
    if direction == "LONG":
        rejection_wick = live_high - max(live_open, current_price)
    else:
        rejection_wick = min(live_open, current_price) - live_low
    wick_rejection_ratio = max(0.0, rejection_wick) / body_safe

    rsi_val, macd_status = compute_rsi_macd(kdf["close"])

    # ÚJ: RSI/ár divergencia - kizárólag LEZÁRT gyertyákon (lásd
    # detect_rsi_divergence() kommentjét). Tájékoztató jellegű, nem szűr -
    # a meggyőződés-pontszámba számít be, illetve az üzenetben megjelenik.
    rsi_series_closed = compute_rsi_series(closed["close"])
    divergence = detect_rsi_divergence(closed, rsi_series_closed, current_price=current_price) if rsi_series_closed is not None else None

    # ÚJ: VWAP-viszony - lásd compute_vwap() kommentjét.
    vwap = compute_vwap(kdf)
    vwap_relation = None
    vwap_diff_pct = None
    if vwap is not None and vwap > 0:
        vwap_diff_pct = (current_price - vwap) / vwap * 100
        vwap_relation = "ABOVE" if current_price > vwap else "BELOW"

    elapsed_fraction = None
    pace_vol_multiplier = None
    if now is not None and "timestamp" in kdf.columns:
        try:
            live_open_ts = live["timestamp"].to_pydatetime().replace(tzinfo=timezone.utc)
            now_utc = now.astimezone(timezone.utc)
            elapsed_seconds = (now_utc - live_open_ts).total_seconds()
            if elapsed_seconds >= 60:
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
        "divergence": divergence,
        "wick_rejection_ratio": round(wick_rejection_ratio, 2),
        "vwap": vwap,
        "vwap_relation": vwap_relation,
        "vwap_diff_pct": round(vwap_diff_pct, 2) if vwap_diff_pct is not None else None,
        "signal_type": "STANDARD",
        "elapsed_fraction": round(elapsed_fraction, 3) if elapsed_fraction is not None else None,
        "pace_vol_multiplier": round(pace_vol_multiplier, 2) if pace_vol_multiplier is not None else None,
    }

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, funding_cache: dict, now: datetime, ws_store: Optional["LiveKlineStore"] = None):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS + KLINES_MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        klines_semaphore = asyncio.Semaphore(KLINES_MAX_CONCURRENT_REQUESTS)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, htf_cache, funding_cache

        if valid_contracts is None:
            valid_contracts = await fetch_valid_contract_symbols(session)

        if ENABLE_LEGACY_SLTP_SUMMARY:
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
        if len(missing_htf) > HTF_FETCH_BATCH_SIZE:
            missing_htf = missing_htf[:HTF_FETCH_BATCH_SIZE]
        
        missing_funding = [s for s in candidates if s not in funding_cache]

        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in candidates]
        kline_tasks = [fetch_klines(session, klines_semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, klines_semaphore, s) for s in missing_htf]
        funding_tasks = [fetch_funding_rate(session, semaphore, s) for s in missing_funding]

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

        # ÚJ: nyomon követjük, mely szimbólumoknál kaptunk FRISS (ebben a
        # körben most lekért) funding rate-et - a funding_history-ba csak
        # ezeket írjuk be, különben a 30 mp-enkénti pass-ok a state-ben
        # feleslegesen (és a cache miatt szinte mindig azonos értékkel)
        # duplikálnák a bejegyzéseket, mert a funding_cache egy teljes
        # futáson (kb. 8 percen) belül újrahasznosított.
        funding_freshly_fetched = set()
        if funding_results:
            for item in funding_results:
                if isinstance(item, BaseException):
                    continue
                s, fr = item
                if fr is not None:
                    funding_cache[s] = fr
                    funding_freshly_fetched.add(s)

    oi_map = {item[0]: item[1] for item in oi_results if not isinstance(item, BaseException) and item[1] is not None}
    klines_map = {item[0]: item[1] for item in kline_results if not isinstance(item, BaseException) and item[1] is not None}

    alerts_sent = 0
    evaluated = 0
    # ÚJ (diagnosztika): lásd az alert_checker.py azonos blokk-kommentjét.
    pass_diagnostics = []
    ws_patched_count = 0  # ÚJ (ideiglenes debug): hányszor patchelt ténylegesen a WS-adat ebben a körben

    for symbol in candidates:
        # ÚJ: symbol-tiltás ellenőrzése MINDENEK ELŐTT - ha 3 egymás utáni
        # BAD minősítést kapott ennél a botnál, 24 órára kihagyjuk, hogy
        # ne pazaroljunk rá feleslegesen erőforrást.
        if is_symbol_banned(state, symbol, now):
            continue

        kdf = klines_map.get(symbol)
        # ÚJ: ha van rá friss websocket-adat, az élő (utolsó, még nyitott)
        # gyertya sorát frissebb, valós idejű close/high/low/volume értékekre
        # cseréljük - lásd _patch_live_candle_with_ws() kommentjét.
        kdf, ws_patched = _patch_live_candle_with_ws(kdf, symbol, ws_store)
        if ws_patched:
            ws_patched_count += 1
        candle = evaluate_candle(kdf, now=now)
        oi_now = oi_map.get(symbol)
        if candle is None or oi_now is None:
            continue
        evaluated += 1

        entry = state.setdefault(symbol, {"oi_history": [], "last_alert_ts": None})
        entry["oi_history"].append({"ts": now.isoformat(), "oi": oi_now})
        cutoff = now - timedelta(minutes=MAX_HISTORY_AGE_MINUTES)
        entry["oi_history"] = [h for h in entry["oi_history"] if datetime.fromisoformat(h["ts"]) >= cutoff]
        entry["last_seen"] = now.isoformat()

        oi_baseline = find_oi_baseline(entry["oi_history"][:-1], now)
        if oi_baseline is None or oi_baseline["oi"] <= 0:
            continue

        oi_change_pct = (oi_now - oi_baseline["oi"]) / oi_baseline["oi"] * 100
        funding_rate = funding_cache.get(symbol)

        # ÚJ: funding rate history + delta (gyorsulás). Csak akkor írunk a
        # history-ba, ha ebben a körben FRISSEN kaptuk a funding rate-et
        # (lásd funding_freshly_fetched fentebb) - így a history kb.
        # futásonként (10 percenként) egyszer bővül, nem 30 mp-enként.
        entry.setdefault("funding_history", [])
        if symbol in funding_freshly_fetched and funding_rate is not None:
            entry["funding_history"].append({"ts": now.isoformat(), "rate": funding_rate})
            entry["funding_history"] = [h for h in entry["funding_history"] if datetime.fromisoformat(h["ts"]) >= cutoff]

        funding_delta_pct = None
        if funding_rate is not None and entry["funding_history"]:
            # ha most FRISSEN raktuk be a jelenlegi pontot, azt ki kell
            # zárni a baseline-keresésből (ő maga nem lehet a saját múltja)
            hist_for_baseline = entry["funding_history"][:-1] if symbol in funding_freshly_fetched else entry["funding_history"]
            funding_baseline = find_funding_baseline(hist_for_baseline, now)
            if funding_baseline is not None:
                funding_delta_pct = funding_rate - funding_baseline["rate"]

        htf_data = htf_cache.get(symbol, {})
        htf_trend = htf_data.get("trend")
        support = htf_data.get("support")
        resistance = htf_data.get("resistance")

        against_trend = REQUIRE_HTF_ALIGNMENT and ((candle["direction"] == "LONG" and htf_trend == "DOWN") or (candle["direction"] == "SHORT" and htf_trend == "UP"))
        
        price = candle["price"]
        near_support = support is not None and support > 0 and abs(price - support) / support * 100 <= SR_PROXIMITY_PCT
        near_resistance = resistance is not None and resistance > 0 and abs(price - resistance) / resistance * 100 <= SR_PROXIMITY_PCT

        near_level_risk = ((candle["direction"] == "LONG" and near_resistance) or (candle["direction"] == "SHORT" and near_support))
        bounce_confluence = ((candle["direction"] == "LONG" and near_support) or (candle["direction"] == "SHORT" and near_resistance))

        is_setup = (
            abs(candle["price_change_pct"]) <= MAX_PRICE_CHANGE
            and oi_change_pct >= MIN_OI_INCREASE
            and candle["vol_multiplier"] >= MIN_VOL_MULTIPLIER
            and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
        )

        is_setup_early = False
        oi_fast_change_pct = None
        if ENABLE_EARLY_SIGNALS and not is_setup:
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

        if fired_signal_type is None:
            failed_conditions = []
            if abs(candle["price_change_pct"]) > MAX_PRICE_CHANGE:
                failed_conditions.append(f"ár-mozgás túl nagy ({candle['price_change_pct']:+.2f}%, max {MAX_PRICE_CHANGE}%)")
            if oi_change_pct < MIN_OI_INCREASE:
                failed_conditions.append(f"OI-növekedés túl kicsi ({oi_change_pct:+.2f}%, min {MIN_OI_INCREASE}%)")
            if candle["vol_multiplier"] < MIN_VOL_MULTIPLIER:
                failed_conditions.append(f"Vol-szorzó túl kicsi ({candle['vol_multiplier']:.2f}x, min {MIN_VOL_MULTIPLIER}x)")
            if candle["candle_vol_usdt"] < MIN_CANDLE_VOL_USDT:
                failed_conditions.append(f"gyertya-volumen túl kicsi ({candle['candle_vol_usdt']:,.0f} USDT, min {MIN_CANDLE_VOL_USDT:,.0f})")
            if not failed_conditions:
                failed_conditions.append("cooldown alatt" if not cooldown_ok else "ismeretlen (minden STANDARD-küszöb teljesült?)")
            pass_diagnostics.append({
                "symbol": symbol,
                "price_change_pct": candle["price_change_pct"],
                "oi_change_pct": oi_change_pct,
                "vol_multiplier": candle["vol_multiplier"],
                "candle_vol_usdt": candle["candle_vol_usdt"],
                "failed": failed_conditions,
            })

        if fired_signal_type and cooldown_ok:
            display_oi_change_pct = oi_fast_change_pct if fired_signal_type == "EARLY" else oi_change_pct
            # ÚJ: orderbook imbalance csak MOST, a ténylegesen kimenő
            # jelzéshez kérdezzük le - lásd fetch_orderbook_imbalance
            # kommentjét arról, miért nem minden candidate-re fut le ez.
            orderbook_info = await fetch_orderbook_imbalance(symbol)
            # ÚJ: bot-közi megerősítés ellenőrzése - lásd
            # get_cross_bot_confirmations() kommentjét.
            cross_bot_confirmations = get_cross_bot_confirmations(symbol, candle["direction"], now)
            # ÚJ: historikus kimenetel-statisztika lekérdezése - lásd
            # compute_historical_stats() kommentjét.
            historical_stats = compute_historical_stats(fired_signal_type, candle["direction"], now)
            msg = format_daytrade_message(
                symbol, candle["direction"], candle["price"], candle["price_change_pct"],
                candle["candle_vol_usdt"], candle["vol_multiplier"],
                oi_now, display_oi_change_pct, htf_trend=htf_trend,
                bounce_confluence=bounce_confluence, near_level_risk=near_level_risk,
                rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                signal_type=fired_signal_type,
                funding_rate=funding_rate,
                pace_vol_multiplier=candle.get("pace_vol_multiplier"),
                elapsed_fraction=candle.get("elapsed_fraction"),
                funding_delta_pct=funding_delta_pct,
                orderbook_info=orderbook_info,
                cross_bot_confirmations=cross_bot_confirmations,
                divergence=candle.get("divergence"),
                vwap=candle.get("vwap"), vwap_relation=candle.get("vwap_relation"),
                vwap_diff_pct=candle.get("vwap_diff_pct"),
                historical_stats=historical_stats,
                wick_rejection_ratio=candle.get("wick_rejection_ratio"),
            )
            await send_telegram_message(msg)
            entry["last_alert_ts"] = now.isoformat()
            alerts_sent += 1
            if ENABLE_LEGACY_SLTP_SUMMARY:
                register_pending_signal(state, symbol, fired_signal_type, candle["direction"], candle["price"], now)
            # ÚJ: objektív, SL/TP-mentes signal-audit regisztrálása - lásd
            # a fájl elején lévő blokk-kommentet. A pontszámot itt
            # ÚJRASZÁMOLJUK (nem módosítjuk a jelzés-generálást, csak a
            # már meglévő bemenetekből egy már úgyis kiszámolt értéket
            # kérünk le újra, kizárólag naplózási célra).
            audit_score, _, _ = compute_confidence_score(
                candle["direction"], htf_trend=htf_trend, bounce_confluence=bounce_confluence,
                near_level_risk=near_level_risk, funding_rate=funding_rate,
                funding_delta_pct=funding_delta_pct, orderbook_info=orderbook_info,
                macd_status=candle.get("macd_status"), rsi=candle.get("rsi"),
                vol_multiplier=candle["vol_multiplier"], cross_bot_confirmations=cross_bot_confirmations,
                divergence=candle.get("divergence"), vwap_relation=candle.get("vwap_relation"),
                historical_stats=historical_stats, wick_rejection_ratio=candle.get("wick_rejection_ratio"),
                oi_change_pct=display_oi_change_pct,
            )
            register_signal_audit(state, symbol, candle["direction"], fired_signal_type,
                                    audit_score, candle["price"], now,
                                    meta={
                                        "oi_change_pct": display_oi_change_pct,
                                        "vol_multiplier": candle["vol_multiplier"],
                                        "htf_aligned": (htf_trend == candle["direction"]) if htf_trend else None,
                                        "rsi_divergence": candle.get("divergence") is not None,
                                        "cross_bot_confirmed": bool(cross_bot_confirmations),
                                        "wick_rejection_ratio": candle.get("wick_rejection_ratio"),
                                    })
            # ÚJ: a SAJÁT jelzésünket is elmentjük a kereszt-bot fájlba,
            # hogy a MÁSIK két bot lássa a következő futásukban.
            _append_cross_bot_signal(symbol, candle["direction"], fired_signal_type, now)

        # --------------------------------------------------------------
        # ÚJ: DIVERGENCE_REVERSAL - önálló, a STANDARD/EARLY-től TELJESEN
        # FÜGGETLEN jelzéstípus. Lásd a fájl elején lévő blokk-kommentet.
        # A fő trigger az RSI-divergencia (nem a volumen-kiugrás),
        # MEGERŐSÍTVE egy valódi (swing-pont alapú) támasz/ellenállás
        # közelségével, plusz egy megerősítő gyertyával, ami már a
        # divergencia szerinti irányba mozog.
        # --------------------------------------------------------------
        divergence = candle.get("divergence")
        divergence_direction = "LONG" if divergence == "BULLISH" else ("SHORT" if divergence == "BEARISH" else None)

        if divergence_direction is not None:
            div_near_support = support is not None and support > 0 and abs(price - support) / support * 100 <= DIVERGENCE_REVERSAL_SR_PROXIMITY_PCT
            div_near_resistance = resistance is not None and resistance > 0 and abs(price - resistance) / resistance * 100 <= DIVERGENCE_REVERSAL_SR_PROXIMITY_PCT
            div_confluence = (
                (divergence_direction == "LONG" and div_near_support)
                or (divergence_direction == "SHORT" and div_near_resistance)
            )

            is_setup_divergence = (
                div_confluence
                and candle["direction"] == divergence_direction
                and abs(candle["price_change_pct"]) >= DIVERGENCE_REVERSAL_MIN_BODY_PCT
                and candle["candle_vol_usdt"] >= DIVERGENCE_REVERSAL_MIN_CANDLE_VOL_USDT
            )

            div_cooldown_ok = True
            if entry.get("last_divergence_alert_ts"):
                last_div_dt = datetime.fromisoformat(entry["last_divergence_alert_ts"])
                if (now - last_div_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    div_cooldown_ok = False

            if is_setup_divergence and div_cooldown_ok:
                div_orderbook_info = await fetch_orderbook_imbalance(symbol)
                div_cross_bot = get_cross_bot_confirmations(symbol, divergence_direction, now)
                div_historical_stats = compute_historical_stats("DIVERGENCE_REVERSAL", divergence_direction, now)
                div_near_level_risk = (
                    (divergence_direction == "LONG" and div_near_resistance)
                    or (divergence_direction == "SHORT" and div_near_support)
                )
                div_msg = format_daytrade_message(
                    symbol, divergence_direction, candle["price"], candle["price_change_pct"],
                    candle["candle_vol_usdt"], candle["vol_multiplier"],
                    oi_now, oi_change_pct, htf_trend=htf_trend,
                    bounce_confluence=div_confluence, near_level_risk=div_near_level_risk,
                    rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                    signal_type="DIVERGENCE_REVERSAL",
                    funding_rate=funding_rate, funding_delta_pct=funding_delta_pct,
                    orderbook_info=div_orderbook_info,
                    cross_bot_confirmations=div_cross_bot,
                    divergence=divergence,
                    vwap=candle.get("vwap"), vwap_relation=candle.get("vwap_relation"),
                    vwap_diff_pct=candle.get("vwap_diff_pct"),
                    historical_stats=div_historical_stats,
                    wick_rejection_ratio=candle.get("wick_rejection_ratio"),
                )
                await send_telegram_message(div_msg)
                entry["last_divergence_alert_ts"] = now.isoformat()
                alerts_sent += 1
                if ENABLE_LEGACY_SLTP_SUMMARY:
                    register_pending_signal(state, symbol, "DIVERGENCE_REVERSAL", divergence_direction, candle["price"], now)
                div_audit_score, _, _ = compute_confidence_score(
                    divergence_direction, htf_trend=htf_trend, bounce_confluence=div_confluence,
                    near_level_risk=div_near_level_risk, funding_rate=funding_rate,
                    funding_delta_pct=funding_delta_pct, orderbook_info=div_orderbook_info,
                    macd_status=candle.get("macd_status"), rsi=candle.get("rsi"),
                    vol_multiplier=candle["vol_multiplier"], cross_bot_confirmations=div_cross_bot,
                    divergence=divergence, vwap_relation=candle.get("vwap_relation"),
                    historical_stats=div_historical_stats, wick_rejection_ratio=candle.get("wick_rejection_ratio"),
                )
                register_signal_audit(state, symbol, divergence_direction, "DIVERGENCE_REVERSAL",
                                        div_audit_score, candle["price"], now,
                                        meta={
                                            "sr_level": support if divergence_direction == "LONG" else resistance,
                                            "sr_distance_pct": (abs(price - support) / support * 100) if divergence_direction == "LONG" and support else
                                                                (abs(price - resistance) / resistance * 100) if divergence_direction == "SHORT" and resistance else None,
                                            "htf_aligned": (htf_trend == divergence_direction) if htf_trend else None,
                                            "cross_bot_confirmed": bool(div_cross_bot),
                                        })
                _append_cross_bot_signal(symbol, divergence_direction, "DIVERGENCE_REVERSAL", now)
                logger.info("JELZÉS küldve [DIVERGENCE_REVERSAL]: %s [%s] ár=%.6f szint-táv=%.2f%%",
                            symbol, divergence_direction, candle["price"],
                            (abs(price - support) / support * 100) if divergence_direction == "LONG" and support else
                            (abs(price - resistance) / resistance * 100) if divergence_direction == "SHORT" and resistance else -1.0)

    # ÚJ (IDEIGLENES DEBUG): mutatja, hogy a websocket-adat ténylegesen
    # HATOTT-e a kiértékelésre ebben a körben - lásd az alert_checker.py
    # azonos blokk-kommentjét. Ha bejáratottnak érzed a funkciót, ez a
    # blokk bátran törölhető - nem befolyásol semmilyen jelzési logikát.
    if ws_store is not None and evaluated > 0:
        logger.info("  [WS debug] élő gyertya websocket-adattal frissítve: %d/%d symbolnál.",
                    ws_patched_count, evaluated)

    if pass_diagnostics:
        pass_diagnostics.sort(key=lambda d: abs(d["price_change_pct"]), reverse=True)
        for d in pass_diagnostics[:3]:
            logger.info("  [nem tüzelt] %s: ár %+.2f%%, OI %+.2f%%, vol %.1fx, gyertya-vol %.0f USDT -> %s",
                        d["symbol"], d["price_change_pct"], d["oi_change_pct"], d["vol_multiplier"],
                        d["candle_vol_usdt"], "; ".join(d["failed"]))

    if ENABLE_LEGACY_SLTP_SUMMARY:
        await maybe_send_daily_summary(state, now)
    return alerts_sent, evaluated, valid_contracts, htf_cache, funding_cache

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
    htf_cache = {}
    funding_cache = {}

    # ÚJ: websocket élő gyertya-figyelő indítása (ha USE_WEBSOCKET_KLINES=true).
    # Lásd az alert_checker.py azonos blokk-kommentjét a teljes indoklásért.
    ws_store = None
    ws_tasks: list = []
    ws_stop_event = asyncio.Event()
    if USE_WEBSOCKET_KLINES:
        try:
            ws_store, ws_tasks = await _start_ws_kline_listeners(ws_stop_event)
        except Exception as e:
            logger.warning("WS-figyelő indítása sikertelen, REST-polling-ra esünk vissza: %s", e)
            ws_store, ws_tasks = None, []

    # ÚJ: signal-audit feloldás + napi riport - EGYSZER a futás elején,
    # nem minden pass-ban (elkerülve a felesleges API-terhelést). Lásd a
    # fájl elején lévő "OBJEKTÍV, SL/TP-MENTES SIGNAL-AUDIT RENDSZER"
    # blokk-kommentet.
    try:
        audit_connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=audit_connector) as audit_session:
            audit_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            await resolve_signal_audit(state, audit_session, audit_semaphore, datetime.now(timezone.utc))
        await maybe_send_daily_audit_report(state, datetime.now(timezone.utc))
        save_state(state)
    except Exception as e:
        logger.warning("Signal-audit feloldás/riport sikertelen (a fő ciklus ettől függetlenül folytatódik): %s", e)

    try:
        while True:
            elapsed_total = time.monotonic() - loop_start
            if elapsed_total >= TOTAL_RUN_BUDGET_SECONDS:
                break
            pass_start = time.monotonic()
            now = datetime.now(timezone.utc)
            remaining_budget = max(30.0, TOTAL_RUN_BUDGET_SECONDS - elapsed_total)
            try:
                alerts, evaluated, valid_contracts, htf_cache, funding_cache = await asyncio.wait_for(
                    run_single_pass(state, valid_contracts, htf_cache, funding_cache, now, ws_store=ws_store),
                    timeout=remaining_budget,
                )
            except asyncio.TimeoutError:
                logger.warning("Túllépte az időkeretet (%.0f mp), megszakítva.", remaining_budget)
                save_state(state)
                break
            save_state(state)
            logger.info("Kör kész: %d pár kiértékelve, %d riasztás.", evaluated, alerts)
            # JAVÍTÁS: a korábbi "pass_elapsed = time.monotonic() - (time.monotonic() - remaining_budget)"
            # matematikailag mindig kb. remaining_budget-tal volt egyenlő (a két
            # time.monotonic() hívás közti különbség elhanyagolható), ami mivel
            # remaining_budget sosem kisebb 30-nál, azt eredményezte, hogy
            # PASS_INTERVAL_SECONDS - pass_elapsed MINDIG <= 0 volt -> sleep_time
            # MINDIG 0 -> a ciklus SOHA nem várt, szünet nélkül pörgött, ami
            # azonnal rate-limitbe futtatta a botot. A helyes számítás a kör
            # TÉNYLEGES időtartamát nézi (pass_start-tól máig).
            pass_elapsed = time.monotonic() - pass_start
            remaining_total = TOTAL_RUN_BUDGET_SECONDS - (time.monotonic() - loop_start)
            if remaining_total <= 0:
                break
            sleep_time = max(0.0, PASS_INTERVAL_SECONDS - pass_elapsed)
            sleep_time = min(sleep_time, remaining_total)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
    finally:
        # ÚJ: a WS-taszkokat MINDIG leállítjuk, még hiba/timeout esetén is,
        # nehogy "árva" kapcsolatok maradjanak nyitva a folyamat leállása után.
        if ws_tasks:
            ws_stop_event.set()
            for t in ws_tasks:
                t.cancel()
            await asyncio.gather(*ws_tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
