"""
BingX Perpetual - Kereskedési Belépő Bot (trader_checker.py)
====================================================================
Ez a bot NEM csak irányt jelez (mint a másik hét), hanem KONKRÉT
BELÉPÉSI TERVET ad: belépő ár, stop (invalidáció), célszintek és
pozícióméret - azzal a céllal, hogy később BingX API-n keresztül
automatizálható legyen.

--------------------------------------------------------------------
STRATÉGIA: HTF-TREND + VISSZAHÚZÓDÁSOS BELÉPŐ (trend-continuation)
--------------------------------------------------------------------
Miért EZ a stratégia? Mert a trend-követő, visszahúzódásra belépő
megközelítés a legjobban dokumentált, leginkább robusztus családba
tartozik: nem fordulatot próbál elkapni (ami statisztikailag nehéz),
hanem egy MÁR LÉTEZŐ trendbe száll be egy kedvezőbb áron, ÉS - ami a
legfontosabb - STRUKTURÁLIS stopot ad (a visszahúzódás mélypontja
alatt), nem önkényes százalékot. Így a kockázat/hozam arány nem
becslés, hanem a piac szerkezetéből adódik.

A belépés négy feltétele (MIND kell):

  1. HTF IRÁNY (4h): EMA50 és EMA200 egyértelmű elrendezése + az ár a
     megfelelő oldalon. Ha nincs tiszta trend -> NINCS kereskedés.
     (A legtöbb veszteséget az oldalazó piac okozza - ezt kihagyjuk.)

  2. VISSZAHÚZÓDÁS (1h): az ár visszatért az EMA20-EMA50 "érték-zónába",
     DE nem törte meg a szerkezetet (nem zárt a legutóbbi jelentős
     swing-mélypont alá LONG esetén). Az RSI "resetelődött" (nem
     túlvett/túladott, hanem semleges sávba tért vissza).

  3. TRIGGER (1h zárás): momentum-visszavétel - egy határozott (ATR-hez
     mért, elég nagy testű) gyertya, ami visszazár az EMA20 fölé
     (LONG), átlagos vagy annál nagyobb volumennel.

  4. KOCKÁZAT/HOZAM KAPU: a legközelebbi STRUKTURÁLIS cél (a korábbi
     swing-csúcs, amit az ár már bizonyítottan elért) legalább
     MIN_RISK_REWARD-szorosa legyen a stop-távolságnak. Ha nem éri el,
     a setup KIMARAD, bármilyen szép is egyébként.

SZINTEK:
  - Belépő:  az aktuális ár (a trigger-gyertya zárása)
  - Stop:    a visszahúzódás swing-mélypontja (LONG) / csúcsa (SHORT)
             mínusz/plusz egy ATR-alapú puffer
  - Cél 1:   a korábbi swing-csúcs/mélypont (strukturális cél)
  - Cél 2:   Cél1 + a Cél1-távolság ismét (kiterjesztés)
  - Méret:   a fix kockázati %-ból és a STOP TÁVOLSÁGÁBÓL számolva -
             tehát minden jelzésnél UGYANANNYIT kockáztatsz, függetlenül
             attól, milyen messze van a stop

--------------------------------------------------------------------
FONTOS, MIELŐTT BÁRMIT AUTOMATIZÁLNÁL
--------------------------------------------------------------------
A stratégia szerkezete bevált módszertan, DE a konkrét küszöbszámok
(EMA-periódusok, RSI-sávok, ATR-szorzók, minimum R:R) MÉRLEGELT
ALAPÉRTÉKEK, NEM backtestelt, bizonyított értékek. A bot ezért
minden jelzés kimenetelét NYOMON KÖVETI (lásd resolve_pending_trades),
hogy MÉRHETŐ statisztikád legyen, mielőtt valódi tőkét bíznál rá.
Javasolt: legalább 30-50 lezárt jelzés papíron, mielőtt API-t kötsz rá.
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
logger = logging.getLogger("trader_checker")

# ----------------------------------------------------------------------------
# KOCKÁZATKEZELÉS - EZEKET ÁLLÍTSD A SAJÁT SZÁMAIDRA
# ----------------------------------------------------------------------------
ACCOUNT_SIZE_USDT = 1000.0      # a számlád mérete (a pozícióméret ebből számolódik)
RISK_PER_TRADE_PCT = 1.0        # ennyi %-ot kockáztatsz EGY kereskedésen (1% = konzervatív alap)
MIN_RISK_REWARD = 1.8           # ennél rosszabb kockázat/hozam aránnyal NEM ad jelzést
MEASURED_TARGET1_R = 2.0        # ha nincs strukturális cél (új csúcs/mélypont), ennyi R a Cél1

# ----------------------------------------------------------------------------
# STRATÉGIAI PARAMÉTEREK
# ----------------------------------------------------------------------------
HTF_TIMEFRAME = "4h"            # magasabb idősík: trend-irány meghatározása
ENTRY_TIMEFRAME = "1h"          # belépési idősík: visszahúzódás + trigger
HTF_CANDLES = 250               # EMA200-hoz kell bőven adat
ENTRY_CANDLES = 200

HTF_EMA_FAST = 50
HTF_EMA_SLOW = 200
# ÚJ (tesztelés során talált hiba javítása): pusztán az EMA-sorrend NEM
# elég trend-azonosításhoz - egy teljesen zajos, oldalazó piacon is
# valamilyen sorrendben állnak az EMA-k, így a szűrő "talált" volna
# irányt ott is, ahol nincs. Ezért megköveteljük, hogy a két EMA között
# ÉRDEMI távolság legyen (az árhoz mérve), különben nincs kereskedés.
HTF_MIN_EMA_SEPARATION_PCT = 1.5

ENTRY_EMA_FAST = 20
ENTRY_EMA_SLOW = 50

ATR_LENGTH = 14
RSI_LENGTH = 14

# Visszahúzódás: az árnak be kell érnie az EMA20-EMA50 sávba (ez az
# "érték-zóna"), egy ATR-arányos toleranciával kiszélesítve
PULLBACK_ZONE_ATR_TOLERANCE = 0.5

# RSI "reset" sáv - LONG esetén a visszahúzódás alatt az RSI-nek ide
# kellett esnie (nem túladott pánik, csak egészséges levegővétel)
# (Tesztelés során derült ki, hogy az eredeti 35-60-as sáv túl szűk volt:
# egy egészséges 1h visszahúzódás egy erős 4h trendben simán lemehet
# RSI 30-ig, és gyakran épp az a legjobb belépő - a 35-ös alsó határ
# ezeket indokolatlanul kizárta volna.)
RSI_RESET_MIN_LONG = 28.0
RSI_RESET_MAX_LONG = 62.0
RSI_RESET_MIN_SHORT = 38.0
RSI_RESET_MAX_SHORT = 72.0
PULLBACK_LOOKBACK = 12          # ennyi gyertyán belül kellett a visszahúzódásnak zajlania

# Trigger-gyertya: elég határozott legyen (ne doji), ATR-hez mérve
TRIGGER_MIN_BODY_ATR = 0.35
TRIGGER_MIN_VOL_MULTIPLIER = 0.9   # a megelőző gyertyák átlagához képest

# Stop: a swing-pont mögé ennyi ATR pufferrel (a "zaj-kisöprés" ellen)
STOP_ATR_BUFFER = 0.35
# Stop-távolság épelméjűségi korlátok (az árhoz mérve %-ban)
MIN_STOP_DISTANCE_PCT = 0.5     # ennél szorosabb stop = zajra fog kiütni
MAX_STOP_DISTANCE_PCT = 6.0     # ennél tágabb stop = túl nagy pozíció-kockázat

# Ne lépjünk be, ha az ár már túl messze elszaladt az EMA20-tól
# (kimaradt a mozgás - a jó belépő közel van az értékhez)
MAX_EXTENSION_ATR = 2.0

SWING_FRACTAL_LEGS = 2

# ----------------------------------------------------------------------------
# ÁLTALÁNOS BEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
MIN_VOLUME_USDT = 5_000_000     # magasabb, mint a jelző-botoknál: valódi
                                  # kereskedéshez likviditás kell (slippage!)
MAX_VOLUME_USDT = 500_000_000

MAX_SIGNALS_PER_RUN = 3         # kereskedési jelzésből kevés és jó kell, nem sok
TRADE_COOLDOWN_HOURS = 12       # ugyanarra a symbolra ennyi ideig nem jelez újra

# Kimenetel-követés
OUTCOME_EVAL_WINDOW_HOURS = 48  # ennyi ideig figyeljük, mi lett a jelzésből
OUTCOME_MAX_STALE_HOURS = 12

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

STATE_FILE = Path(__file__).parent / "trader_state.json"
SIGNAL_LOG_FILE = Path(__file__).parent / "trader_log.jsonl"

MAX_CONCURRENT_REQUESTS = 8
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

def _append_log(record: dict) -> None:
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ----------------------------------------------------------------------------
# API
# ----------------------------------------------------------------------------
async def _get_json(session, url, params=None):
    endpoint_key = url
    cooldown_until = _ENDPOINT_COOLDOWN_UNTIL.get(endpoint_key)
    if cooldown_until is not None:
        if time.monotonic() < cooldown_until:
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
                        logger.warning("Endpoint hűtésre kényszerítve %.0f mp-re (code 100410)", wait_seconds)
                        return None
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                return data
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    if last_error is not None:
        logger.warning("Sikertelen API-hívás: %s | url=%s params=%s", last_error, url, params)
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


async def fetch_klines(session, semaphore, symbol, interval, limit):
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
# INDIKÁTOROK
# ----------------------------------------------------------------------------
def compute_atr(df: pd.DataFrame, length: int = ATR_LENGTH) -> pd.Series:
    """Wilder-simítású ATR (pontos, seed = első `length` TR átlaga)."""
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    n = len(tr)
    atr = pd.Series(np.nan, index=tr.index, dtype=float)
    if n <= length:
        return atr
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


def compute_rsi(close: pd.Series, length: int = RSI_LENGTH) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)


def find_swing_points(df: pd.DataFrame, legs: int = SWING_FRACTAL_LEGS) -> list:
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    n = len(highs)
    points = []
    for i in range(legs, n - legs):
        hw = highs[i - legs:i + legs + 1]
        if highs[i] == hw.max() and (hw == highs[i]).sum() == 1:
            points.append((i, float(highs[i]), "H"))
        lw = lows[i - legs:i + legs + 1]
        if lows[i] == lw.min() and (lw == lows[i]).sum() == 1:
            points.append((i, float(lows[i]), "L"))
    points.sort(key=lambda p: p[0])
    return points


# ----------------------------------------------------------------------------
# 1) HTF TREND-IRÁNY
# ----------------------------------------------------------------------------
def determine_htf_bias(htf_df: pd.DataFrame) -> Optional[str]:
    """4h idősíkon: EMA50/EMA200 elrendezés + az ár helyzete.
    Csak EGYÉRTELMŰ trendnél ad irányt - oldalazásnál None (nincs kereskedés)."""
    if htf_df is None or len(htf_df) < HTF_EMA_SLOW + 10:
        return None
    closed = htf_df.iloc[:-1]
    ema_fast = closed["close"].ewm(span=HTF_EMA_FAST, adjust=False).mean()
    ema_slow = closed["close"].ewm(span=HTF_EMA_SLOW, adjust=False).mean()
    price = float(closed["close"].iloc[-1])
    f = float(ema_fast.iloc[-1])
    s = float(ema_slow.iloc[-1])
    if pd.isna(f) or pd.isna(s) or s <= 0:
        return None

    # Trend-erősség: az EMA-k közti távolságnak érdeminek kell lennie,
    # különben csak zaj-szintű sorrendről van szó (lásd a konstans
    # kommentjét). Oldalazó piacon ez a kapu zárja ki a kereskedést.
    separation_pct = abs(f - s) / s * 100
    if separation_pct < HTF_MIN_EMA_SEPARATION_PCT:
        return None

    if f > s and price > f:
        return "LONG"
    if f < s and price < f:
        return "SHORT"
    return None


# ----------------------------------------------------------------------------
# 2-4) BELÉPÉSI SETUP AZ 1h IDŐSÍKON
# ----------------------------------------------------------------------------
def evaluate_entry_setup(entry_df: pd.DataFrame, bias: str) -> Optional[dict]:
    """A teljes belépési logika: visszahúzódás -> trigger -> szintek -> R:R kapu.
    Csak LEZÁRT gyertyákkal dolgozik (nincs félkész gyertya - egy kereskedési
    jelzésnél ez különösen fontos, mert a még változó gyertya félrevezetne)."""
    if entry_df is None or len(entry_df) < ENTRY_EMA_SLOW + PULLBACK_LOOKBACK + 10:
        return None

    closed = entry_df.iloc[:-1].reset_index(drop=True)
    ema_fast = closed["close"].ewm(span=ENTRY_EMA_FAST, adjust=False).mean()
    ema_slow = closed["close"].ewm(span=ENTRY_EMA_SLOW, adjust=False).mean()
    atr_series = compute_atr(closed)
    rsi_series = compute_rsi(closed["close"])

    i = len(closed) - 1  # a legutóbbi LEZÁRT gyertya = a trigger-jelölt
    atr = float(atr_series.iloc[i]) if pd.notna(atr_series.iloc[i]) else None
    if atr is None or atr <= 0:
        return None

    trigger = closed.iloc[i]
    t_open, t_high, t_low, t_close = (float(trigger["open"]), float(trigger["high"]),
                                        float(trigger["low"]), float(trigger["close"]))
    ef = float(ema_fast.iloc[i])
    es = float(ema_slow.iloc[i])

    # --- 3) TRIGGER: határozott, EMA20-at visszavevő gyertya ---
    body = abs(t_close - t_open)
    if body < atr * TRIGGER_MIN_BODY_ATR:
        return None
    avg_vol = float(closed["volume"].iloc[i - 20:i].mean())
    if avg_vol <= 0 or float(trigger["volume"]) < avg_vol * TRIGGER_MIN_VOL_MULTIPLIER:
        return None

    if bias == "LONG":
        if not (t_close > t_open and t_close > ef):
            return None
    else:
        if not (t_close < t_open and t_close < ef):
            return None

    # --- Ne lépjünk be túl kifutott áron ---
    if abs(t_close - ef) > atr * MAX_EXTENSION_ATR:
        return None

    # --- 2) VISSZAHÚZÓDÁS: a triggert megelőző ablakban az árnak be kellett
    # érnie az EMA20-EMA50 érték-zónába, és az RSI-nek resetelődnie ---
    lb_start = max(0, i - PULLBACK_LOOKBACK)
    pullback_window = closed.iloc[lb_start:i]
    if len(pullback_window) < 3:
        return None

    zone_hi = max(ef, es) + atr * PULLBACK_ZONE_ATR_TOLERANCE
    zone_lo = min(ef, es) - atr * PULLBACK_ZONE_ATR_TOLERANCE
    touched_value_zone = bool(
        ((pullback_window["low"] <= zone_hi) & (pullback_window["high"] >= zone_lo)).any()
    )
    if not touched_value_zone:
        return None

    rsi_window = rsi_series.iloc[lb_start:i].dropna()
    if rsi_window.empty:
        return None
    if bias == "LONG":
        rsi_min = float(rsi_window.min())
        if not (RSI_RESET_MIN_LONG <= rsi_min <= RSI_RESET_MAX_LONG):
            return None
    else:
        rsi_max = float(rsi_window.max())
        if not (RSI_RESET_MIN_SHORT <= rsi_max <= RSI_RESET_MAX_SHORT):
            return None

    # --- STOP: a visszahúzódás szélsőértéke + ATR puffer (strukturális) ---
    entry_price = t_close
    if bias == "LONG":
        pullback_extreme = float(pullback_window["low"].min())
        stop_price = min(pullback_extreme, t_low) - atr * STOP_ATR_BUFFER
        risk = entry_price - stop_price
    else:
        pullback_extreme = float(pullback_window["high"].max())
        stop_price = max(pullback_extreme, t_high) + atr * STOP_ATR_BUFFER
        risk = stop_price - entry_price

    if risk <= 0:
        return None
    stop_distance_pct = risk / entry_price * 100
    if not (MIN_STOP_DISTANCE_PCT <= stop_distance_pct <= MAX_STOP_DISTANCE_PCT):
        return None

    # --- CÉL 1: strukturális (korábbi swing, amit az ár már elért) ---
    swings = find_swing_points(closed)
    target1 = None
    if bias == "LONG":
        highs_above = [p for _, p, t in swings if t == "H" and p > entry_price + risk * 0.5]
        if highs_above:
            target1 = min(highs_above)   # a legközelebbi releváns ellenállás
    else:
        lows_below = [p for _, p, t in swings if t == "L" and p < entry_price - risk * 0.5]
        if lows_below:
            target1 = max(lows_below)

    # ÚJ (tesztelés során felismert korlát): ha NINCS strukturális cél -
    # mert az ár épp új csúcsot/mélypontot dönt ("blue sky") -, a bot
    # korábban SOHA nem jelzett volna, pedig épp az a legerősebb trend.
    # Ilyenkor mért ("measured") célra váltunk: fix R-szorzókra. Az
    # üzenetben jelezzük, hogy melyik típusú célról van szó, mert a
    # strukturális cél (amit az ár már bizonyítottan elért) megbízhatóbb.
    target_type = "strukturális"
    if target1 is None:
        target_type = "mért (nincs korábbi szint - új csúcs/mélypont)"
        measured = risk * MEASURED_TARGET1_R
        target1 = entry_price + measured if bias == "LONG" else entry_price - measured

    reward = abs(target1 - entry_price)
    rr = reward / risk
    if rr < MIN_RISK_REWARD:
        return None   # --- 4) KOCKÁZAT/HOZAM KAPU ---

    # --- CÉL 2: kiterjesztés (a Cél1-távolság még egyszer) ---
    target2 = target1 + (target1 - entry_price) if bias == "LONG" else target1 - (entry_price - target1)

    # --- POZÍCIÓMÉRET: fix kockázatból és a stop-távolságból ---
    risk_amount = ACCOUNT_SIZE_USDT * (RISK_PER_TRADE_PCT / 100)
    position_size_usdt = risk_amount / (risk / entry_price)

    return {
        "direction": bias,
        "entry": entry_price,
        "stop": stop_price,
        "target1": float(target1),
        "target2": float(target2),
        "risk_per_unit": risk,
        "stop_distance_pct": round(stop_distance_pct, 2),
        "rr_target1": round(rr, 2),
        "rr_target2": round(abs(target2 - entry_price) / risk, 2),
        "target_type": target_type,
        "risk_amount_usdt": round(risk_amount, 2),
        "position_size_usdt": round(position_size_usdt, 2),
        "atr": atr,
        "rsi": float(rsi_series.iloc[i]) if pd.notna(rsi_series.iloc[i]) else None,
        "trigger_ts": closed["timestamp"].iloc[i].isoformat(),
    }


# ----------------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------------
def _send_telegram_sync(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Hiányzik a TELEGRAM_BOT_TOKEN vagy TELEGRAM_CHAT_ID.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code != 200:
            logger.error("Telegram hiba (%s): %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Telegram küldési hiba: %s", e)

async def send_telegram(text: str) -> None:
    await asyncio.to_thread(_send_telegram_sync, text)


def format_trade_message(symbol: str, setup: dict) -> str:
    d = setup["direction"]
    arrow = "🟩 LONG" if d == "LONG" else "🟥 SHORT"
    header = f"💼 <b>[BELÉPŐ] {symbol}</b> {arrow}"

    body = (
        f"{header}\n"
        f"\n"
        f"📍 <b>Belépő:</b> {setup['entry']:.6f}\n"
        f"🛑 <b>Stop:</b> {setup['stop']:.6f}  ({setup['stop_distance_pct']:.2f}%)\n"
        f"🎯 <b>Cél 1:</b> {setup['target1']:.6f}  ({setup['rr_target1']:.1f}R, {setup['target_type']})\n"
        f"🎯 <b>Cél 2:</b> {setup['target2']:.6f}  ({setup['rr_target2']:.1f}R)\n"
        f"\n"
        f"💵 Pozícióméret: <b>{setup['position_size_usdt']:,.0f} USDT</b> (névérték)\n"
        f"⚠️ Kockázat: {setup['risk_amount_usdt']:.2f} USDT "
        f"({RISK_PER_TRADE_PCT:.1f}% a {ACCOUNT_SIZE_USDT:,.0f} USDT számlából)\n"
        f"\n"
        f"📐 Setup: 4h trend ({d}) + 1h visszahúzódás az érték-zónába, "
        f"majd momentum-visszavétel. RSI: {setup['rsi']:.0f}\n"
        f"⏱️ Trigger-gyertya: {setup['trigger_ts']}\n"
        f"\n"
        f"ℹ️ A stop STRUKTURÁLIS (a visszahúzódás mélypontja mögött), nem "
        f"önkényes %. Ha az ár oda ér, a setup megdőlt - ott ki kell szállni.\n"
        f"⚠️ Ez javaslat, nem tanács. Te döntesz és te viseled a kockázatot."
    )
    return f"\n{body}\n"


# ----------------------------------------------------------------------------
# KIMENETEL-KÖVETÉS (hogy MÉRHETŐ statisztikád legyen automatizálás előtt)
# ----------------------------------------------------------------------------
def register_pending_trade(state: dict, symbol: str, setup: dict, now: datetime) -> None:
    pending = state.setdefault("_pending", [])
    pending.append({
        "symbol": symbol,
        "direction": setup["direction"],
        "entry": setup["entry"],
        "stop": setup["stop"],
        "target1": setup["target1"],
        "target2": setup["target2"],
        "rr_target1": setup["rr_target1"],
        "entry_ts": now.isoformat(),
    })


async def resolve_pending_trades(state: dict, session, semaphore, now: datetime) -> None:
    """Megnézi a korábbi jelzések kimenetelét: stop, Cél1, Cél2, vagy még fut.
    Ez adja a valódi, mérhető teljesítmény-statisztikát."""
    pending = state.get("_pending", [])
    if not pending:
        return

    still_open = []
    for trade in pending:
        try:
            entry_dt = datetime.fromisoformat(trade["entry_ts"])
        except (KeyError, ValueError):
            continue
        age_hours = (now - entry_dt).total_seconds() / 3600
        if age_hours < 1:
            still_open.append(trade)
            continue
        if age_hours > OUTCOME_EVAL_WINDOW_HOURS + OUTCOME_MAX_STALE_HOURS:
            continue  # túl régi, eldobjuk

        _, kdf = await fetch_klines(session, semaphore, trade["symbol"], ENTRY_TIMEFRAME, 100)
        if kdf is None:
            still_open.append(trade)
            continue

        after = kdf[kdf["timestamp"] > pd.Timestamp(entry_dt.replace(tzinfo=None))]
        if after.empty:
            still_open.append(trade)
            continue

        outcome = None
        for _, row in after.iterrows():
            hi, lo = float(row["high"]), float(row["low"])
            if trade["direction"] == "LONG":
                if lo <= trade["stop"]:
                    outcome = "STOP"; break
                if hi >= trade["target2"]:
                    outcome = "TARGET2"; break
                if hi >= trade["target1"]:
                    outcome = "TARGET1"; break
            else:
                if hi >= trade["stop"]:
                    outcome = "STOP"; break
                if lo <= trade["target2"]:
                    outcome = "TARGET2"; break
                if lo <= trade["target1"]:
                    outcome = "TARGET1"; break

        if outcome is None:
            if age_hours >= OUTCOME_EVAL_WINDOW_HOURS:
                outcome = "TIMEOUT"
            else:
                still_open.append(trade)
                continue

        r_result = {"STOP": -1.0, "TARGET1": trade["rr_target1"],
                     "TARGET2": trade["rr_target1"] * 2, "TIMEOUT": 0.0}.get(outcome, 0.0)
        _append_log({**trade, "resolved_ts": now.isoformat(), "outcome": outcome, "r_multiple": r_result})
        logger.info("KIMENETEL: %s [%s] -> %s (%.1fR)", trade["symbol"], trade["direction"], outcome, r_result)

    state["_pending"] = still_open


# ----------------------------------------------------------------------------
# FŐ FUTÁS
# ----------------------------------------------------------------------------
async def run_once(state: dict, now: datetime) -> tuple:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        await resolve_pending_trades(state, session, semaphore, now)

        tickers = await fetch_all_tickers(session)
        if not tickers:
            logger.warning("Nem sikerült ticker adatot lekérni.")
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

        htf_results = await asyncio.gather(
            *[fetch_klines(session, semaphore, s, HTF_TIMEFRAME, HTF_CANDLES) for s in candidates],
            return_exceptions=True,
        )
        htf_map = {r[0]: r[1] for r in htf_results if not isinstance(r, BaseException) and r[1] is not None}

        # Csak azokra kérünk 1h adatot, ahol VAN tiszta HTF trend - ez
        # jelentősen csökkenti az API-terhelést (a symbolok nagy részénél
        # nincs egyértelmű trend, azokkal nem is foglalkozunk tovább).
        biased = {}
        for s, hdf in htf_map.items():
            b = determine_htf_bias(hdf)
            if b:
                biased[s] = b

        entry_results = await asyncio.gather(
            *[fetch_klines(session, semaphore, s, ENTRY_TIMEFRAME, ENTRY_CANDLES) for s in biased],
            return_exceptions=True,
        )
        entry_map = {r[0]: r[1] for r in entry_results if not isinstance(r, BaseException) and r[1] is not None}

    found = []
    evaluated = 0
    for symbol, bias in biased.items():
        edf = entry_map.get(symbol)
        if edf is None:
            continue
        evaluated += 1

        entry_state = state.setdefault(symbol, {"last_trade_ts": None})
        if entry_state.get("last_trade_ts"):
            last_dt = datetime.fromisoformat(entry_state["last_trade_ts"])
            if (now - last_dt) < timedelta(hours=TRADE_COOLDOWN_HOURS):
                continue

        setup = evaluate_entry_setup(edf, bias)
        if setup is None:
            continue

        found.append({
            "symbol": symbol, "setup": setup, "entry_state": entry_state,
            "liquidity": tickers.get(symbol, {}).get("quote_volume_24h", 0.0),
        })

    # Rangsorolás: jobb kockázat/hozam elöl, azonos R:R-nél a likvidebb
    found.sort(key=lambda f: (f["setup"]["rr_target1"], f["liquidity"]), reverse=True)
    to_send = found[:MAX_SIGNALS_PER_RUN]
    if len(found) > len(to_send):
        logger.info("Rate-limit: %d setup elnyomva (csak a legjobb %d ment ki).",
                    len(found) - len(to_send), MAX_SIGNALS_PER_RUN)

    sent = 0
    for f in to_send:
        symbol, setup = f["symbol"], f["setup"]
        await send_telegram(format_trade_message(symbol, setup))
        f["entry_state"]["last_trade_ts"] = now.isoformat()
        register_pending_trade(state, symbol, setup, now)
        sent += 1
        logger.info("BELÉPŐ küldve: %s [%s] entry=%.6f stop=%.6f T1=%.6f (%.1fR)",
                    symbol, setup["direction"], setup["entry"], setup["stop"],
                    setup["target1"], setup["rr_target1"])

    return sent, evaluated


async def main():
    state = load_state()
    now = datetime.now(timezone.utc)
    try:
        sent, evaluated = await run_once(state, now)
        logger.info("Futás kész: %d setup kiértékelve, %d belépő küldve.", evaluated, sent)
    finally:
        save_state(state)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID nincs beállítva - "
                        "az értesítés kimarad, csak a state frissül.")
    asyncio.run(main())
