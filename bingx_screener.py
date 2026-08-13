"""
BingX Perpetual Swap (USDT-M Futures) Crypto Screener
=======================================================
Streamlit dashboard, amely a BingX nyilvános Swap API-ját használva
valós idejű technikai elemzést (RSI, MACD, Volumen, Open Interest)
végez az összes elérhető USDT-perpetual páron.

Futtatás:
    streamlit run bingx_screener.py

Szükséges csomagok: lásd requirements.txt
"""

import asyncio
from datetime import datetime, timezone

import aiohttp
import pandas as pd
import streamlit as st
from ta.momentum import RSIIndicator
from ta.trend import MACD

# ----------------------------------------------------------------------------
# 0) ALAPBEÁLLÍTÁSOK
# ----------------------------------------------------------------------------
BASE_URL = "https://open-api.bingx.com"
CONTRACTS_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/contracts"
KLINES_ENDPOINT = f"{BASE_URL}/openApi/swap/v3/quote/klines"
TICKER_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/ticker"
OI_ENDPOINT = f"{BASE_URL}/openApi/swap/v2/quote/openInterest"

TIMEFRAMES = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

# Egyszerű, statikus kategória-szótár – ez bővíthető / lecserélhető később
# saját, teljesebb mapping-re (pl. külső API-ból vagy CSV-ből betöltve).
CATEGORY_MAP = {
    "BTC": "Layer1", "ETH": "Layer1", "SOL": "Layer1", "ADA": "Layer1",
    "AVAX": "Layer1", "DOT": "Layer1", "NEAR": "Layer1", "APT": "Layer1",
    "SUI": "Layer1", "TON": "Layer1", "ATOM": "Layer1", "TRX": "Layer1",
    "UNI": "DeFi", "AAVE": "DeFi", "MKR": "DeFi", "CRV": "DeFi",
    "SUSHI": "DeFi", "COMP": "DeFi", "SNX": "DeFi", "LDO": "DeFi",
    "DOGE": "Meme", "SHIB": "Meme", "PEPE": "Meme", "FLOKI": "Meme",
    "BONK": "Meme", "WIF": "Meme",
    "ARB": "Layer2", "OP": "Layer2", "MATIC": "Layer2", "STRK": "Layer2",
    "LINK": "Oracle", "BAND": "Oracle",
    "FIL": "Storage", "AR": "Storage",
}
DEFAULT_CATEGORY = "Egyéb"

MAX_CONCURRENT_REQUESTS = 8   # Egyidejű API hívások száma (rate limit védelem)
REQUEST_TIMEOUT = 10          # másodperc
KLINES_LIMIT = 60             # ennyi gyertyát kérünk le / szimbólum
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5           # másodperc, exponenciálisan nő

st.set_page_config(page_title="BingX Perpetual Screener", layout="wide")

# ----------------------------------------------------------------------------
# 1) ASZINKRON API HÍVÁSOK
# ----------------------------------------------------------------------------

async def _get_json(session, url, params=None):
    """Egy GET kérés végrehajtása retry + backoff logikával, rate limit kezeléssel."""
    for attempt in range(RETRY_COUNT):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status == 429:
                    # Rate limit -> várunk és újrapróbáljuk
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    return None


async def fetch_all_symbols(session):
    """Az összes elérhető USDT-perpetual szimbólum lekérdezése."""
    data = await _get_json(session, CONTRACTS_ENDPOINT)
    if not data or "data" not in data:
        return []
    symbols = [
        c["symbol"] for c in data["data"]
        if c.get("symbol", "").endswith("-USDT") and c.get("status", 1) == 1
    ]
    return sorted(symbols)


async def fetch_all_tickers(session):
    """24 órás ticker adatok (ár, volumen) egyben, az összes szimbólumra."""
    data = await _get_json(session, TICKER_ENDPOINT)
    if not data or "data" not in data:
        return {}
    result = {}
    for t in data["data"]:
        result[t["symbol"]] = {
            "last_price": float(t.get("lastPrice", 0) or 0),
            "quote_volume_24h": float(t.get("quoteVolume", 0) or 0),
        }
    return result


async def fetch_klines(session, semaphore, symbol, interval, limit=KLINES_LIMIT):
    async with semaphore:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = await _get_json(session, KLINES_ENDPOINT, params=params)
        await asyncio.sleep(0.05)  # kis extra várakozás -> rate limit védelem
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        df = pd.DataFrame(data["data"])
        expected_cols = {"open", "close", "high", "low", "volume", "time"}
        if not expected_cols.issubset(df.columns):
            return symbol, None
        df = df.rename(columns={"time": "timestamp"})
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)
        return symbol, df


async def fetch_open_interest(session, semaphore, symbol):
    async with semaphore:
        data = await _get_json(session, OI_ENDPOINT, params={"symbol": symbol})
        await asyncio.sleep(0.05)
        if not data or "data" not in data or not data["data"]:
            return symbol, None
        oi = data["data"]
        try:
            return symbol, float(oi.get("openInterest", 0))
        except (TypeError, ValueError):
            return symbol, None


async def gather_market_data(symbols, interval):
    """Az összes szimbólumra lekérdezi a klines és OI adatokat, korlátozott
    párhuzamossággal (szemafor), hogy ne lépjük túl a BingX rate limitjét."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)

        kline_tasks = [fetch_klines(session, semaphore, s, interval) for s in symbols]
        oi_tasks = [fetch_open_interest(session, semaphore, s) for s in symbols]

        klines_results = await asyncio.gather(*kline_tasks)
        oi_results = await asyncio.gather(*oi_tasks)

    klines_map = {s: df for s, df in klines_results if df is not None}
    oi_map = {s: oi for s, oi in oi_results if oi is not None}
    return tickers, klines_map, oi_map


def run_async(coro):
    """Segédfüggvény az async kód futtatásához Streamlit szinkron kontextusában."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

# ----------------------------------------------------------------------------
# 2) INDIKÁTOROK SZÁMÍTÁSA
# ----------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame):
    """RSI, MACD és volumen-változás számítása egy adott szimbólum gyertyáira."""
    if df is None or len(df) < 35:
        return None

    close = df["close"]

    rsi = RSIIndicator(close=close, window=14).rsi()
    macd_calc = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_calc.macd()
    signal_line = macd_calc.macd_signal()

    # --- MACD kereszteződés detektálása (utolsó lezárt 2 gyertya alapján) ---
    macd_status = "Nincs kereszt"
    if len(macd_line) >= 2 and not macd_line.iloc[-2:].isna().any():
        prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
        curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
        if prev_diff < 0 and curr_diff > 0:
            macd_status = "Bullish Cross"
        elif prev_diff > 0 and curr_diff < 0:
            macd_status = "Bearish Cross"
        elif curr_diff > 0:
            macd_status = "Bullish (trend)"
        else:
            macd_status = "Bearish (trend)"

    # --- Volumen növekedés az utolsó két gyertya között ---
    vol_now = df["volume"].iloc[-1]
    vol_prev = df["volume"].iloc[-2]
    vol_change_pct = ((vol_now - vol_prev) / vol_prev * 100) if vol_prev > 0 else 0.0

    return {
        "rsi": round(float(rsi.iloc[-1]), 2) if not pd.isna(rsi.iloc[-1]) else None,
        "macd_status": macd_status,
        "candle_vol_change_pct": round(float(vol_change_pct), 2),
        "last_close": float(close.iloc[-1]),
    }


def get_category(symbol: str) -> str:
    base = symbol.split("-")[0]
    return CATEGORY_MAP.get(base, DEFAULT_CATEGORY)

# ----------------------------------------------------------------------------
# 3) ADAT ÖSSZEÁLLÍTÁSA (cache-elve, hogy ne hívjuk feleslegesen az API-t)
# ----------------------------------------------------------------------------

@st.cache_data(ttl=60, show_spinner=False)
def load_symbols():
    async def _run():
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(connector=connector) as session:
            return await fetch_all_symbols(session)
    return run_async(_run())


@st.cache_data(ttl=45, show_spinner=False)
def build_dashboard_data(symbols_tuple, interval):
    symbols = list(symbols_tuple)
    tickers, klines_map, oi_map = run_async(gather_market_data(symbols, interval))

    rows = []
    for symbol in symbols:
        df = klines_map.get(symbol)
        indicators = compute_indicators(df)
        ticker_info = tickers.get(symbol, {})
        if indicators is None:
            continue

        rows.append({
            "Ticker": symbol,
            "Kategória": get_category(symbol),
            "Ár": ticker_info.get("last_price", indicators["last_close"]),
            "Volume (24h, USDT)": ticker_info.get("quote_volume_24h", 0.0),
            "Gyertya Vol. Változás (%)": indicators["candle_vol_change_pct"],
            "Open Interest": oi_map.get(symbol, None),
            "RSI (14)": indicators["rsi"],
            "MACD Státusz": indicators["macd_status"],
        })

    return pd.DataFrame(rows)

# ----------------------------------------------------------------------------
# 4) SIDEBAR - SZŰRŐK
# ----------------------------------------------------------------------------

st.sidebar.title("⚙️ Screener beállítások")

timeframe_label = st.sidebar.selectbox("Idősík (Timeframe)", list(TIMEFRAMES.keys()), index=1)
interval = TIMEFRAMES[timeframe_label]

max_symbols = st.sidebar.slider(
    "Vizsgált párok maximális száma (teljesítmény miatt)",
    min_value=20, max_value=400, value=120, step=10,
    help="Minél több párt vizsgálunk, annál tovább tart a frissítés az API rate limit miatt."
)

min_volume = st.sidebar.slider(
    "Minimum 24h Volumen (USDT)",
    min_value=0, max_value=50_000_000, value=500_000, step=100_000
)

available_categories = sorted(set(CATEGORY_MAP.values()) | {DEFAULT_CATEGORY})
selected_categories = st.sidebar.multiselect(
    "Kategória szűrő", options=available_categories, default=available_categories
)

search_text = st.sidebar.text_input("Keresés (pl. BTC-USDT)", value="").upper().strip()

if st.sidebar.button("🔄 Adatok frissítése (cache törlése)"):
    st.cache_data.clear()

st.sidebar.caption(
    "Az adatok automatikusan cache-elve vannak (~45-60 mp), hogy elkerüljük "
    "a BingX API rate limit túllépését."
)

# ----------------------------------------------------------------------------
# 5) FŐ TARTALOM
# ----------------------------------------------------------------------------

st.title("📊 BingX Perpetual Swap Screener")
st.caption(
    f"Utolsó frissítés: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} "
    f"| Idősík: {timeframe_label}"
)

with st.spinner("Szimbólumok betöltése..."):
    all_symbols = load_symbols()

if not all_symbols:
    st.error("Nem sikerült lekérni a szimbólum listát a BingX API-ból. Próbáld frissíteni később.")
    st.stop()

symbols_to_scan = tuple(all_symbols[:max_symbols])

with st.spinner(f"{len(symbols_to_scan)} pár elemzése ({timeframe_label})..."):
    df = build_dashboard_data(symbols_to_scan, interval)

if df.empty:
    st.warning("Nincs megjeleníthető adat. Próbáld csökkenteni a szűrők szigorúságát, vagy frissítsd az oldalt.")
    st.stop()

# --- Szűrők alkalmazása ---
filtered = df[df["Volume (24h, USDT)"] >= min_volume]
filtered = filtered[filtered["Kategória"].isin(selected_categories)]
if search_text:
    filtered = filtered[filtered["Ticker"].str.contains(search_text)]

st.markdown(f"**{len(filtered)}** / {len(df)} pár felel meg a szűrési feltételeknek.")

# ----------------------------------------------------------------------------
# 6) VIZUÁLIS FORMÁZÁS ÉS TÁBLÁZAT MEGJELENÍTÉS
# ----------------------------------------------------------------------------

def color_pct(val):
    if pd.isna(val):
        return ""
    color = "#1a7f37" if val > 0 else ("#c0392b" if val < 0 else "")
    return f"color: {color}; font-weight: 600;"


def color_macd(val):
    if val == "Bullish Cross":
        return "background-color: #1a7f37; color: white; font-weight: 700;"
    if val == "Bearish Cross":
        return "background-color: #c0392b; color: white; font-weight: 700;"
    if val == "Bullish (trend)":
        return "color: #1a7f37;"
    if val == "Bearish (trend)":
        return "color: #c0392b;"
    return ""


def color_rsi(val):
    if pd.isna(val):
        return ""
    if val >= 70:
        return "color: #c0392b; font-weight: 600;"
    if val <= 30:
        return "color: #1a7f37; font-weight: 600;"
    return ""


display_df = filtered.sort_values("Volume (24h, USDT)", ascending=False).reset_index(drop=True)

styled = (
    display_df.style
    .map(color_pct, subset=["Gyertya Vol. Változás (%)"])
    .map(color_macd, subset=["MACD Státusz"])
    .map(color_rsi, subset=["RSI (14)"])
    .format({
        "Ár": "{:.6f}",
        "Volume (24h, USDT)": "{:,.0f}",
        "Gyertya Vol. Változás (%)": "{:+.2f}%",
        "Open Interest": "{:,.2f}",
        "RSI (14)": "{:.1f}",
    })
)

st.dataframe(styled, use_container_width=True, height=650)

st.caption(
    "⚠️ Az Open Interest oszlop a pillanatnyi (aktuális) OI értéket mutatja, mivel a BingX "
    "nyilvános API nem biztosít historikus OI adatot minden lekérdezéshez. A volumen-változás "
    "az utolsó két lezárt gyertya közötti százalékos eltérést mutatja a kiválasztott idősíkon."
)
