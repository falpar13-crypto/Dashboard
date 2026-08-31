"""
BingX Perpetual - Order Block Figyelő (ob_checker.py)
====================================================================
Önálló, hatodik bot. Ez a felhasználó saját TradingView Pine Script
indikátorának ("OB Engine + ema, sma", © falpar13) Python-átültetése -
kizárólag az order block (OB) - detektáló logikai mag, az EMA/SMA
megjelenítő rész NÉLKÜL (azt a felhasználó kérésére kihagytuk).

MIT JELEZ:
Nem az OB-zóna KIALAKULÁSÁT jelzi (az önmagában nem kereskedési jelzés,
csak egy potenciális jövőbeli szint kijelölése), hanem azt a pillanatot,
amikor az ár VISSZATÉR egy korábban kialakult, MÉG ÉRVÉNYES (nem
mitigált) order block zónába - ez a "smart money concepts" logika
szerinti tényleges belépési/reakció-pillanat.

MÓDSZERTAN (a Pine forráskód 1:1 logikai fordítása):
  1. ATR(dispLen=14) - Wilder-simítással.
  2. Displacement-szűrő: egy adott gyertyánál csak akkor keresünk új
     OB-t, ha a MEGELŐZŐ gyertya önmagában is erős, irányított (ATR*1.2
     szorzót meghaladó) mozgást mutatott.
  3. Impulzus-validáció: egy jelölt anchor-gyertya (max 6 gyertyával
     visszamenőleg) csak akkor számít order blocknak, ha az UTÁNA
     következő (max 3) gyertya együttes elmozdulása meghaladja az
     ATR*1.5 szorzót.
  4. OB-keresés: a legutóbbi ELLENTÉTES színű gyertya az impulzus előtt
     (bullish OB -> utolsó bearish gyertya erős felfutás előtt, és
     fordítva).
  5. Zóna: az anchor-gyertya TELJES (high-low) tartománya (ez az
     indikátor alap "Teljes gyertya" zóna-típusa - a másik három
     zóna-típus, ill. az "Érintés"/"Középérték" mitigáció-típus NINCS
     implementálva ebben az átültetésben, csak az alapbeállítás).
  6. Overlap-eltávolítás: új zóna törli/mitigálja az azonos irányú,
     átfedő korábbi zónákat.
  7. Mitigáció (érvénytelenítés): záróár-alapú (ha a bull zóna alá / a
     bear zóna fölé zár az ár, a zóna megszűnik).

Minden futás a friss gyertya-ablakon TELJESEN ÚJRASZIMULÁLJA ezt a
bar-by-bar folyamatot (nem igényel tartós zóna-állapotot) - csak a
duplikált riasztások elkerüléséhez van egy kis state (lásd lent).
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
logger = logging.getLogger("ob_checker")

# ----------------------------------------------------------------------------
# PARAMÉTEREK (az indikátor alapértelmezett beállításai)
# ----------------------------------------------------------------------------
ALERT_TIMEFRAME = "1h"
HISTORY_CANDLES = 150     # ennyi lezárt gyertyán szimuláljuk végig a bar-by-bar logikát

ATR_LENGTH = 14
DISPLACEMENT_MULT = 1.2
IMPULSE_LEN = 3
IMPULSE_MULT = 1.5
OB_LOOKBACK = 6

# ÚJ: maximum zóna-szélesség - ha egy OB (top-bot) tartománya a mélyponthoz
# képest ennél SZÉLESEBB %-ban, kihagyjuk. Indoklás: egy nagyon tág zónát
# az ár szinte folyamatosan "érint" (a high/low könnyen belelóg), ezért egy
# ilyen zóna "visszatérés" jelzése nem informatív - gyakorlatilag mindig
# igaz lenne, nem egy konkrét, éles szintre való visszatérést jelezne.
MAX_ZONE_WIDTH_PCT = 3.0

# ÚJ: ha egy ÉPP MOST kialakuló zóna átfedésbe kerülne egy ELLENTÉTES
# irányú (bull vs. bear), MÁR AKTÍV zónával, mindkettőt kizárjuk (sem az
# újat nem hozzuk létre, sem a régit nem tartjuk meg). Indoklás: ha
# ugyanazon a szinten van egy bullish ÉS egy bearish OB is, az
# ellentmondásos jel - nem világos, a piac ezen a szinten vevőként vagy
# eladóként viselkedett-e korábban, ezért inkább egyiket sem használjuk.
EXCLUDE_OPPOSITE_DIRECTION_OVERLAP = True

ALERT_COOLDOWN_HOURS = 8   # ugyanarra a zónára ennyi órán belül nem jelez újra

# ÚJ: biztonsági háló - körönként legfeljebb ennyi riasztást küldünk ki,
# még ha ennél sokkal több valódi (nem hamis) találat lenne is egyszerre
# (ez élesben megtörtént: 18 jelzés egy körben). A rangsorolás a
# "meggyőződés" erejét közelíti (kisebb zóna = pontosabb szint, arany
# zónás Fibonacci-visszahúzódás = erősebb megerősítés). A kimaradó
# találatok NEM kapnak cooldown-t, tehát a következő körben újra
# versenyezhetnek - lásd a trend_checker.py azonos mintáját.
MAX_ALERTS_PER_RUN = 6

# ÚJ: Fibonacci-visszahúzódás konfluencia - TISZTÁN TÁJÉKOZTATÓ, nem szűr.
# A zóna kialakulását okozó impulzus-lökés (a zóna szélétől a lökés utáni
# csúcsig/mélypontig) alapján kiszámoljuk, hogy az érintés pillanatában az
# ár hány %-os Fibonacci-visszahúzódásnál van - az ICT/"smart money"
# módszertanban a 61.8-78.6%-os sávot hívják "arany zóná"-nak (OTE -
# Optimal Trade Entry), mert ez a leggyakoribb reakciós terület.
FIB_GOLDEN_ZONE_MIN = 61.8
FIB_GOLDEN_ZONE_MAX = 78.6
# ÚJ: a sima %-szám helyett a standard Fibonacci-szintek egyikéhez
# viszonyítva jelenítjük meg (érthetőbb, mint egy nyers "73.0%").
FIB_STANDARD_LEVELS = [0.0, 23.6, 38.2, 50.0, 61.8, 78.6, 100.0]

def _nearest_fib_level(pct: float) -> float:
    """A megadott %-hoz legközelebbi standard Fibonacci-szintet adja vissza."""
    return min(FIB_STANDARD_LEVELS, key=lambda lvl: abs(lvl - pct))

FIB_SWING_LOOKBACK = 20   # ennyi gyertyával az anchor ELŐTT keressük a lökés
                           # valódi eredetét (swing low/high) - NEM a zóna
                           # saját szélét használjuk erre, mert az szinte
                           # mindig ~90-100%-os retracement-et adna (a zóna
                           # ugyanis definíció szerint a lökés KEZDETE
                           # KÖZELÉBEN van), ami sosem esne az arany zónába

# ÚJ (hibajavítás): "Láthatóság" - a Pine Script bullShowPct/bearShowPct
# beállítása (alapból 25-25%). Lásd a find_active_order_blocks() végén
# lévő blokk-kommentet a teljes indoklásért.
BULL_BEAR_SHOW_PCT = 25

# ÚJ (hibajavítás, majd VISSZAÁLLÍTVA): a felhasználó egy korábbi
# TradingView-s módosítása véletlenül "Csak a gyertyatest"-re állította a
# "Zóna típusa" beállítást - kiderült, hogy ez nem szándékos volt, a
# valódi (helyes) beállítás "Teljes gyertya". Ha valaha tényleg
# átállítanád a TradingView-n, itt is át kell írni ezt az egy konstanst.
# Lehetséges értékek: "full" (Teljes gyertya), "body" (Csak a gyertyatest)
ZONE_TYPE = "full"

def _zone_bounds(idx: int, opens, closes, highs, lows) -> tuple:
    """A megadott anchor-gyertya (idx) zóna-határait adja vissza a
    ZONE_TYPE beállítás szerint."""
    if ZONE_TYPE == "body":
        o, c = float(opens[idx]), float(closes[idx])
        return max(o, c), min(o, c)
    # "full": teljes gyertya (high-low)
    return float(highs[idx]), float(lows[idx])

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

STATE_FILE = Path(__file__).parent / "ob_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "ob_alert_log.jsonl"

MAX_CONCURRENT_REQUESTS = 10
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_ENDPOINT_COOLDOWN_UNTIL: dict[str, float] = {}
ENDPOINT_COOLDOWN_MAX_SECONDS = 150


# ----------------------------------------------------------------------------
# STATE (csak a duplikáció-védelemhez - a zónákat minden futás újraszámolja)
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
# ORDER BLOCK SZIMULÁCIÓ (a Pine kód logikai magjának Python-fordítása)
# ----------------------------------------------------------------------------
def _compute_atr(df: pd.DataFrame, length: int) -> pd.Series:
    """PONTOS Wilder-simítású ATR - ugyanaz a módszer, mint a Pine ta.atr().

    JAVÍTÁS: a korábbi verzió egy pandas ewm()-alapú KÖZELÍTÉST használt,
    aminek a kezdő (seed) értéke hibásan a legelső True Range volt, nem a
    Wilder-módszer szerinti helyes seed (az első `length` db valódi TR
    egyszerű átlaga). Ez a bemelegítési szakaszban (kb. az első 30-40
    gyertyán) SZISZTEMATIKUSAN ALULBECSÜLT ATR-t adott - ami miatt a
    displacement-szűrő (ATR * DISPLACEMENT_MULT) a valóságosnál könnyebben
    teljesült, tehát a szűrő "gyengébben" működött, mint a TradingView-n.
    Ez most a Wilder-algoritmus PONTOS, rekurzív seedelésével készül."""
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    n = len(tr)
    atr = pd.Series(np.nan, index=tr.index, dtype=float)
    if n <= length:
        return atr

    # Wilder-seed: az első `length` db valódi (index 0 NaN, mert nincs
    # előző close) True Range EGYSZERŰ átlaga.
    seed = tr.iloc[1:length + 1].mean()
    if pd.isna(seed):
        return atr
    atr.iloc[length] = seed
    prev = seed
    tr_values = tr.values
    for i in range(length + 1, n):
        prev = (prev * (length - 1) + tr_values[i]) / length
        atr.iloc[i] = prev
    return atr


def find_active_order_blocks(kdf: pd.DataFrame) -> list:
    """A TELJES megkapott (lezárt) gyertya-ablakon bar-by-bar végigfuttatja
    az order block detektálás + overlap-eltávolítás + mitigáció logikáját
    (lásd a fájl elején lévő blokk-kommentet), és visszaadja a jelenleg
    AKTÍV (nem mitigált) zónák listáját, mindegyiknél jelölve, hogy az
    UTOLSÓ (legfrissebb) gyertyán történt-e "touch" (érintés/visszatérés)
    esemény."""
    if kdf is None or len(kdf) < OB_LOOKBACK + IMPULSE_LEN + ATR_LENGTH * 3 + 5:
        return []

    df = kdf.reset_index(drop=True)
    n = len(df)
    opens = df["open"].values
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    atr = _compute_atr(df, ATR_LENGTH).values

    active_zones = []  # dict: anchor_idx, top, bot, bullish, created_pos, touched, touched_pos

    def find_bull_ob(pos: int) -> tuple:
        for i in range(2, OB_LOOKBACK + 1):
            idx = pos - i
            if idx < 0 or pd.isna(atr[idx]):
                continue
            if closes[idx] >= opens[idx]:
                continue
            end = min(idx + IMPULSE_LEN, pos)
            total = 0.0
            for j in range(idx + 1, end + 1):
                total += max(closes[j] - opens[j], 0.0)
            threshold = atr[idx] * IMPULSE_MULT
            if total > threshold:
                # ÚJ: diagnosztikai adatok - lásd a run_once()-ban a
                # naplózást. Ezekből kézzel visszaellenőrizhető, hogy a
                # Pine Script ugyanígy döntött volna-e.
                diag = {
                    "anchor_ohlc": {"o": round(float(opens[idx]), 8), "h": round(float(highs[idx]), 8),
                                     "l": round(float(lows[idx]), 8), "c": round(float(closes[idx]), 8)},
                    "anchor_atr": round(float(atr[idx]), 8),
                    "impulse_sum": round(float(total), 8),
                    "impulse_threshold": round(float(threshold), 8),
                    "impulse_candle_range": [int(idx + 1), int(end)],
                    "ob_offset_i": int(i),
                }
                return idx, diag
        return None, None

    def find_bear_ob(pos: int) -> tuple:
        for i in range(2, OB_LOOKBACK + 1):
            idx = pos - i
            if idx < 0 or pd.isna(atr[idx]):
                continue
            if closes[idx] <= opens[idx]:
                continue
            end = min(idx + IMPULSE_LEN, pos)
            total = 0.0
            for j in range(idx + 1, end + 1):
                total += max(opens[j] - closes[j], 0.0)
            threshold = atr[idx] * IMPULSE_MULT
            if total > threshold:
                diag = {
                    "anchor_ohlc": {"o": round(float(opens[idx]), 8), "h": round(float(highs[idx]), 8),
                                     "l": round(float(lows[idx]), 8), "c": round(float(closes[idx]), 8)},
                    "anchor_atr": round(float(atr[idx]), 8),
                    "impulse_sum": round(float(total), 8),
                    "impulse_threshold": round(float(threshold), 8),
                    "impulse_candle_range": [int(idx + 1), int(end)],
                    "ob_offset_i": int(i),
                }
                return idx, diag
        return None, None

    def remove_overlap(new_top: float, new_bot: float, bullish: bool) -> None:
        active_zones[:] = [
            z for z in active_zones
            if not (z["bullish"] == bullish and new_top >= z["bot"] and new_bot <= z["top"])
        ]

    def _zone_width_ok(zt: float, zb: float) -> bool:
        """ÚJ: max. zóna-szélesség ellenőrzés - lásd MAX_ZONE_WIDTH_PCT
        kommentjét. False, ha a zóna túl tág ahhoz, hogy informatív legyen."""
        if zb <= 0:
            return False
        width_pct = (zt - zb) / zb * 100
        return width_pct <= MAX_ZONE_WIDTH_PCT

    def _ranges_overlap(top_a: float, bot_a: float, top_b: float, bot_b: float) -> bool:
        return top_a >= bot_b and bot_a <= top_b

    def _try_add_zone(zt: float, zb: float, bullish: bool, pos: int, idx: int, diag: dict = None) -> None:
        """ÚJ: központosított zóna-hozzáadás, ami mindkét új szűrőt
        alkalmazza (max. szélesség + ellentétes irányú átfedés kizárása),
        mielőtt egyáltalán bekerülne az aktív zónák közé."""
        if not _zone_width_ok(zt, zb):
            return  # túl tág zóna - nem informatív, kihagyjuk

        remove_overlap(zt, zb, bullish)

        if EXCLUDE_OPPOSITE_DIRECTION_OVERLAP:
            opposite_overlaps = [
                z for z in active_zones
                if z["bullish"] != bullish and _ranges_overlap(zt, zb, z["top"], z["bot"])
            ]
            if opposite_overlaps:
                # Mindkét irányban ellentmondásos szint - sem az újat nem
                # hozzuk létre, SEM a régi, átfedő ellentétes zónát nem
                # tartjuk meg.
                active_zones[:] = [
                    z for z in active_zones
                    if not (z["bullish"] != bullish and _ranges_overlap(zt, zb, z["top"], z["bot"]))
                ]
                return

        active_zones.append({
            "anchor_idx": idx, "top": zt, "bot": zb, "bullish": bullish,
            "created_pos": pos, "touched": False, "touched_pos": None,
            "diag": diag,  # ÚJ: kézi visszaellenőrzéshez szükséges diagnosztika
        })

    # JAVÍTÁS: korábban ATR_LENGTH+1 (kb. 15 gyertya) után rögtön elkezdtük
    # a zóna-keresést, ami még a Wilder-seed hatása alatt állt (nem volt
    # elég ideje "kisimulnia" a rekurzív számításnak) - ez is hozzájárult
    # a pontatlan, túl-engedékeny displacement-szűréshez. Most a zóna-
    # keresés csak azután indul, hogy az ATR-nek volt legalább
    # ATR_LENGTH*3 gyertyányi ideje stabilizálódni.
    start_pos = max(ATR_LENGTH * 3, OB_LOOKBACK + IMPULSE_LEN + 1)
    for pos in range(start_pos, n):
        if pd.isna(atr[pos - 1]):
            continue

        bull_disp_ok = (
            closes[pos - 1] > opens[pos - 1]
            and (closes[pos - 1] - opens[pos - 1]) > atr[pos - 1] * DISPLACEMENT_MULT
        )
        bear_disp_ok = (
            closes[pos - 1] < opens[pos - 1]
            and (opens[pos - 1] - closes[pos - 1]) > atr[pos - 1] * DISPLACEMENT_MULT
        )
        # ÚJ: displacement diagnosztika a naplózáshoz
        disp_diag = {
            "displacement_candle_idx": int(pos - 1),
            "displacement_ohlc": {"o": round(float(opens[pos - 1]), 8), "c": round(float(closes[pos - 1]), 8)},
            "displacement_atr": round(float(atr[pos - 1]), 8),
            "displacement_threshold": round(float(atr[pos - 1] * DISPLACEMENT_MULT), 8),
        }

        if bull_disp_ok:
            idx, ob_diag = find_bull_ob(pos)
            if idx is not None:
                zt, zb = _zone_bounds(idx, opens, closes, highs, lows)
                if zt > zb:
                    full_diag = {**disp_diag, **(ob_diag or {})}
                    _try_add_zone(zt, zb, True, pos, idx, diag=full_diag)
        if bear_disp_ok:
            idx, ob_diag = find_bear_ob(pos)
            if idx is not None:
                zt, zb = _zone_bounds(idx, opens, closes, highs, lows)
                if zt > zb:
                    full_diag = {**disp_diag, **(ob_diag or {})}
                    _try_add_zone(zt, zb, False, pos, idx, diag=full_diag)

        # "touch" (visszatérés) ellenőrzés - a zóna kialakulása UTÁNI
        # gyertyáktól kezdve, az ELSŐ érintéskor jelöljük meg
        for z in active_zones:
            if not z["touched"] and z["created_pos"] < pos:
                if lows[pos] <= z["top"] and highs[pos] >= z["bot"]:
                    z["touched"] = True
                    z["touched_pos"] = pos

                    # ÚJ: Fibonacci-visszahúzódás konfluencia - lásd a
                    # fájl elején lévő FIB_GOLDEN_ZONE_MIN/MAX kommentjét.
                    # A zóna szélétől a lökés utáni csúcsig/mélypontig
                    # (a zóna kialakulása és az érintés közti ablakban)
                    # húzott lökés-szakaszhoz viszonyítjuk az érintéskori árat.
                    window_start = z["created_pos"] + 1
                    window_end = pos
                    fib_pct = None
                    if window_end > window_start:
                        touch_price = float(closes[pos])
                        swing_start = max(0, z["anchor_idx"] - FIB_SWING_LOOKBACK)
                        if z["bullish"]:
                            peak = float(np.max(highs[window_start:window_end + 1]))
                            swing_low = float(np.min(lows[swing_start:z["anchor_idx"] + 1]))
                            fib_range = peak - swing_low
                            if fib_range > 0:
                                fib_pct = (peak - touch_price) / fib_range * 100
                        else:
                            trough = float(np.min(lows[window_start:window_end + 1]))
                            swing_high = float(np.max(highs[swing_start:z["anchor_idx"] + 1]))
                            fib_range = swing_high - trough
                            if fib_range > 0:
                                fib_pct = (touch_price - trough) / fib_range * 100
                    z["fib_retracement_pct"] = round(fib_pct, 1) if fib_pct is not None else None
                    z["fib_nearest_level"] = _nearest_fib_level(fib_pct) if fib_pct is not None else None
                    z["fib_golden_zone"] = (
                        fib_pct is not None and FIB_GOLDEN_ZONE_MIN <= fib_pct <= FIB_GOLDEN_ZONE_MAX
                    )

        # mitigáció (záróár-alapú, az indikátor alap beállítása)
        still_active = []
        for z in active_zones:
            if z["bullish"]:
                mitigated = closes[pos] < z["bot"]
            else:
                mitigated = closes[pos] > z["top"]
            if not mitigated:
                still_active.append(z)
        active_zones = still_active

    # timestamp-eket is hozzáadjuk a kimenethez, kényelmesebb feldolgozáshoz
    for z in active_zones:
        z["anchor_ts"] = df["timestamp"].iloc[z["anchor_idx"]].isoformat()
        if z["touched_pos"] is not None:
            z["touched_ts"] = df["timestamp"].iloc[z["touched_pos"]].isoformat()

    # ÚJ (hibajavítás): "Láthatóság" szűrő - a Pine Script-ben ez a
    # bullShowPct/bearShowPct beállítás (alapból 25%): a TradingView CSAK
    # a jelenlegi árhoz LEGKÖZELEBBI 25%-nyi bull/bear zónát MUTATJA MEG
    # a charton, a többit vizuálisan elrejti (nem törli, csak nem
    # rajzolja ki). A korábbi verzió ezt figyelmen kívül hagyta, ezért
    # olyan zónákra is jelzett, amiket a felhasználó a charton EGYÁLTALÁN
    # NEM LÁTOTT dobozként. Mostantól csak a látható (legközelebbi 25%-ba
    # eső) zónákat vesszük figyelembe touch-jelzéshez.
    if active_zones:
        current_price = float(closes[n - 1])
        bulls = [z for z in active_zones if z["bullish"]]
        bears = [z for z in active_zones if not z["bullish"]]

        def _visible_subset(zone_list: list) -> set:
            if not zone_list:
                return set()
            with_dist = sorted(zone_list, key=lambda z: abs(current_price - (z["top"] + z["bot"]) / 2))
            limit = max(1, -(-len(with_dist) * BULL_BEAR_SHOW_PCT // 100))  # felfelé kerekítés, mint a Pine math.ceil-je
            visible_ids = {id(z) for z in with_dist[:limit]}
            return visible_ids

        visible_bull_ids = _visible_subset(bulls)
        visible_bear_ids = _visible_subset(bears)
        for z in active_zones:
            z["visible_on_chart"] = id(z) in (visible_bull_ids if z["bullish"] else visible_bear_ids)
        active_zones = [z for z in active_zones if z["visible_on_chart"]]

    return active_zones


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


def format_ob_message(symbol: str, zone: dict, price: float) -> str:
    direction = "LONG" if zone["bullish"] else "SHORT"
    action = "BULLISH OB visszateszt 🟩⬆️" if zone["bullish"] else "BEARISH OB visszateszt 🟥⬇️"
    header = f"🧱 <b>[ORDER BLOCK] {symbol}</b> {action}"

    # ÚJ: Fibonacci-visszahúzódás konfluencia sor - lásd az
    # FIB_GOLDEN_ZONE_MIN/MAX kommentjét. Tisztán tájékoztató, nem szűr.
    fib_line = ""
    if zone.get("fib_nearest_level") is not None:
        golden_note = " 🟡 arany zóna (OTE)" if zone.get("fib_golden_zone") else ""
        level = zone["fib_nearest_level"]
        fib_line = f"\n📐 Fibonacci szint: {level/100:.3f} (a lökés {level:.1f}%-át adta vissza){golden_note}"

    body = (
        f"{header}\n"
        f"💰 Jelenlegi ár: {price:.6f}\n"
        f"📦 Zóna: {zone['bot']:.6f} - {zone['top']:.6f}\n"
        f"⏳ Zóna kialakult: {zone['anchor_ts']}"
        f"{fib_line}\n"
        f"ℹ️ Az ár most (újra) belépett egy korábban kialakult, MÉG ÉRVÉNYES "
        f"(nem mitigált) order block zónába - ez a klasszikus 'smart money' "
        f"logika szerinti reakció-/belépési pillanat. Ellenőrizd a chartot, "
        f"mielőtt döntesz - ez nem automatikus vétel/eladás jelzés."
    )
    return f"\n{body}\n", direction


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

    # ÚJ (két lépéses feldolgozás, lásd MAX_ALERTS_PER_RUN kommentjét):
    # ELŐSZÖR összegyűjtjük az összes cooldown-mentes, friss touch-találatot
    # (nem küldünk még semmit), UTÁNA rangsoroljuk, és csak a legjobb
    # MAX_ALERTS_PER_RUN darabot küldjük ki.
    candidates_for_alert = []

    for symbol in candidates:
        kdf = klines_map.get(symbol)
        if kdf is None:
            continue
        evaluated += 1

        zones = find_active_order_blocks(kdf)
        if not zones:
            continue

        last_pos = len(kdf) - 1
        fresh_touches = [z for z in zones if z.get("touched_pos") == last_pos]
        if not fresh_touches:
            continue

        entry = state.setdefault(symbol, {"alerted_zones": {}})
        entry.setdefault("alerted_zones", {})
        current_price = float(kdf.iloc[-1]["close"])

        for zone in fresh_touches:
            zone_key = f"{zone['anchor_ts']}_{zone['bullish']}"
            last_alert_ts = entry["alerted_zones"].get(zone_key)
            if last_alert_ts:
                last_dt = datetime.fromisoformat(last_alert_ts)
                if (now - last_dt) < timedelta(hours=ALERT_COOLDOWN_HOURS):
                    continue

            # Minőségi pontszám a rangsoroláshoz: szűkebb zóna (pontosabb
            # szint) + arany zónás Fibonacci-visszahúzódás = erősebb jelölt.
            zone_width_pct = (zone["top"] - zone["bot"]) / zone["bot"] * 100 if zone["bot"] > 0 else MAX_ZONE_WIDTH_PCT
            quality_score = (MAX_ZONE_WIDTH_PCT - zone_width_pct)
            if zone.get("fib_golden_zone"):
                quality_score += 2.0

            candidates_for_alert.append({
                "symbol": symbol, "zone": zone, "entry": entry,
                "current_price": current_price, "zone_key": zone_key,
                "quality_score": quality_score,
            })

    candidates_for_alert.sort(key=lambda c: c["quality_score"], reverse=True)
    to_send = candidates_for_alert[:MAX_ALERTS_PER_RUN]
    suppressed = candidates_for_alert[MAX_ALERTS_PER_RUN:]

    if suppressed:
        logger.info("Rate-limit: %d találat elnyomva ebben a körben (csak a legjobb %d ment ki). Elnyomva: %s",
                    len(suppressed), MAX_ALERTS_PER_RUN,
                    ", ".join(f"{c['symbol']}" for c in suppressed))

    for c in to_send:
        symbol, zone, entry = c["symbol"], c["zone"], c["entry"]
        msg, direction = format_ob_message(symbol, zone, c["current_price"])
        await send_telegram_message(msg)
        entry["alerted_zones"][c["zone_key"]] = now.isoformat()
        alerts_sent += 1
        # ÚJ: a diagnosztikai adatokat (anchor OHLC, ATR, impulzus-
        # összeg/küszöb, displacement-gyertya adatai) is elmentjük a
        # jelzés-naplóba - ebből KÉZZEL vissza lehet ellenőrizni,
        # hogy a Pine Script feltételei szerint valóban jogos volt-e
        # a zóna, ha legközelebb megint gyanús jelzés érkezne.
        _append_signal_log({
            "ts": now.isoformat(), "symbol": symbol, "direction": direction,
            "zone_top": zone["top"], "zone_bot": zone["bot"],
            "anchor_ts": zone["anchor_ts"],
            "fib_retracement_pct": zone.get("fib_retracement_pct"),
            "diag": zone.get("diag"),
        })
        logger.info("JELZÉS küldve: %s [%s] zóna %.6f-%.6f (kialakult: %s) | diag: %s",
                    symbol, direction, zone["bot"], zone["top"], zone["anchor_ts"], zone.get("diag"))

    # a state ne nőjön korlátlanul - régi zóna-bejegyzések eldobása
    cutoff = now - timedelta(days=14)
    for symbol, entry in state.items():
        if not isinstance(entry, dict) or "alerted_zones" not in entry:
            continue
        entry["alerted_zones"] = {
            k: v for k, v in entry["alerted_zones"].items()
            if datetime.fromisoformat(v) >= cutoff
        }

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
