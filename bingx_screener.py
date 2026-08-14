"""
BingX Perpetual Swap (USDT-M Futures) Crypto Screener
=======================================================
Streamlit dashboard, amely a BingX nyilvános Swap API-ját használva
valós idejű technikai elemzést (RSI, MACD, Ár/Volumen/OI változás)
végez az elérhető USDT-perpetual párokon, CoinGlass-stílusú kártyás
felülettel és beágyazott gyertya-charttal.

Futtatás:
    streamlit run bingx_screener.py

Szükséges csomagok: lásd requirements.txt (ÚJ: plotly)
"""

import asyncio
from datetime import datetime, timezone

import aiohttp
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

TIMEFRAMES = {"15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}

# Egyszerű, statikus kategória-szótár – bővíthető / lecserélhető később.
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

# --- Küszöbértékek a szűrőkhöz (bátran állítsd ízlés szerint) ---
OI_GROWTH_THRESHOLD_PCT = 2.0    # "OI nő" szűrőnél mekkora növekedés számít jelentősnek
PRICE_FLAT_THRESHOLD_PCT = 0.5   # "ár oldalaz" sáv: -X% és +X% között

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
                    await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                    continue
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
    return None


async def fetch_all_symbols(session):
    data = await _get_json(session, CONTRACTS_ENDPOINT)
    if not data or "data" not in data:
        return []
    symbols = [
        c["symbol"] for c in data["data"]
        if c.get("symbol", "").endswith("-USDT") and c.get("status", 1) == 1
    ]
    return sorted(symbols)


async def fetch_all_tickers(session):
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
        await asyncio.sleep(0.05)
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
# 2) INDIKÁTOROK ÉS SZÁZALÉKOS VÁLTOZÁSOK
#    FONTOS: minden Δ számítás KIZÁRÓLAG lezárt gyertyákon dolgozik – az utolsó,
#    még formálódó (nyitott) gyertyát mindig eldobjuk, mielőtt bármit számolnánk.
# ----------------------------------------------------------------------------

def compute_indicators(df: pd.DataFrame):
    if df is None or len(df) < 36:
        return None

    # --- Az utolsó (még nyitott) gyertya eldobása ---
    closed = df.iloc[:-1].copy()
    if len(closed) < 35:
        return None

    close = closed["close"]
    rsi = RSIIndicator(close=close, window=14).rsi()
    macd_calc = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_calc.macd()
    signal_line = macd_calc.macd_signal()

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

    # --- Volumen- és árváltozás: utolsó lezárt vs. előtte lezárt gyertya ---
    vol_now, vol_prev = closed["volume"].iloc[-1], closed["volume"].iloc[-2]
    vol_change_pct = ((vol_now - vol_prev) / vol_prev * 100) if vol_prev > 0 else 0.0

    price_now, price_prev = closed["close"].iloc[-1], closed["close"].iloc[-2]
    price_change_pct = ((price_now - price_prev) / price_prev * 100) if price_prev > 0 else 0.0

    return {
        "rsi": round(float(rsi.iloc[-1]), 2) if not pd.isna(rsi.iloc[-1]) else None,
        "macd_status": macd_status,
        "candle_vol_change_pct": round(float(vol_change_pct), 2),
        "price_change_pct": round(float(price_change_pct), 2),
        "last_close": float(price_now),
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
        indicators = compute_indicators(klines_map.get(symbol))
        ticker_info = tickers.get(symbol, {})
        if indicators is None:
            continue
        rows.append({
            "Ticker": symbol,
            "Kategória": get_category(symbol),
            "Ár": ticker_info.get("last_price", indicators["last_close"]),
            "Ár Változás (%)": indicators["price_change_pct"],
            "Volume (24h, USDT)": ticker_info.get("quote_volume_24h", 0.0),
            "Gyertya Vol. Változás (%)": indicators["candle_vol_change_pct"],
            "Open Interest": oi_map.get(symbol, None),
            "RSI (14)": indicators["rsi"],
            "MACD Státusz": indicators["macd_status"],
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=45, show_spinner=False)
def fetch_symbol_klines_cached(symbol, interval):
    """Egyetlen szimbólum gyertyáinak lekérése – csak akkor hívjuk, amikor a
    felhasználó ténylegesen meg akarja nyitni az adott kártya chartját."""
    async def _run():
        connector = aiohttp.TCPConnector(limit=2)
        sem = asyncio.Semaphore(2)
        async with aiohttp.ClientSession(connector=connector) as session:
            _, df = await fetch_klines(session, sem, symbol, interval, limit=KLINES_LIMIT)
            return df
    return run_async(_run())

# ----------------------------------------------------------------------------
# 4) EGYÉNI DIZÁJN (CoinGlass-stílusú, sötét, mobilra optimalizált)
# ----------------------------------------------------------------------------

CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .block-container {
        padding-top: 0.8rem !important; padding-bottom: 2rem !important;
        padding-left: 0.9rem !important; padding-right: 0.9rem !important;
        max-width: 900px;
    }
    [data-testid="stAppViewContainer"], [data-testid="stApp"] { background: #0a0d13; }
    [data-testid="stSidebar"] { background: #0d1017; border-right: 1px solid #1b202b; }
    [data-testid="stSidebar"] * { color: #c7ccd6; }

    .cg-header { display: flex; align-items: center; justify-content: space-between; padding: 6px 2px 14px 2px; flex-wrap: wrap; gap: 6px; }
    .cg-title { font-family: 'JetBrains Mono', monospace; font-size: 20px; font-weight: 700; color: #f2f4f8; display: flex; align-items: center; gap: 8px; }
    .cg-dot { width: 8px; height: 8px; border-radius: 50%; background: #0ecb81; box-shadow: 0 0 8px #0ecb81; animation: pulse 1.6s infinite; }
    @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.35;} }
    .cg-sub { font-size: 11.5px; color: #6b7385; font-family: 'JetBrains Mono', monospace; }

    .cg-stats { display: flex; gap: 10px; overflow-x: auto; padding: 4px 2px 16px 2px; scrollbar-width: none; }
    .cg-stats::-webkit-scrollbar { display: none; }
    .cg-stat { flex: 0 0 auto; min-width: 108px; background: linear-gradient(180deg, #10141d 0%, #0d1017 100%); border: 1px solid #1b202b; border-radius: 10px; padding: 10px 14px; }
    .cg-stat-label { font-size: 10px; color: #6b7385; text-transform: uppercase; letter-spacing: 0.06em; font-family: 'JetBrains Mono', monospace; margin-bottom: 4px; }
    .cg-stat-value { font-size: 17px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: #f2f4f8; }

    .cg-card { background: #10141d; border: 1px solid #1b202b; border-radius: 12px 12px 0 0; border-bottom: none; padding: 13px 15px 6px 15px; }
    .cg-card-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .cg-ticker { font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 15px; color: #f2f4f8; }
    .cg-cat { font-size: 10px; color: #7b8494; background: #171c26; border: 1px solid #232a38; border-radius: 20px; padding: 2px 8px; margin-left: 8px; font-family: 'JetBrains Mono', monospace; }
    .cg-price { font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 17px; color: #f2f4f8; text-align: right; }
    .cg-price-delta { font-size: 11.5px; font-weight: 600; font-family: 'JetBrains Mono', monospace; text-align: right; margin-top: 1px; }
    .cg-metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 9px; }
    .cg-metric-label { font-size: 9.5px; color: #565e70; text-transform: uppercase; letter-spacing: 0.05em; font-family: 'JetBrains Mono', monospace; margin-bottom: 2px; }
    .cg-metric-value { font-size: 12.5px; font-weight: 600; font-family: 'JetBrains Mono', monospace; }
    .cg-badge { display: inline-block; font-size: 10.5px; font-weight: 700; padding: 3px 9px; border-radius: 5px; font-family: 'JetBrains Mono', monospace; margin-top: 11px; }
    .cg-volbar-track { width: 100%; height: 3px; background: #1b202b; border-radius: 2px; margin-top: 10px; overflow: hidden; }
    .cg-volbar-fill { height: 100%; border-radius: 2px; }
    .cg-green { color: #0ecb81; } .cg-red { color: #f6465d; } .cg-dim { color: #7b8494; }
    .cg-badge-bull-cross { background: #0ecb81; color: #06120c; }
    .cg-badge-bear-cross { background: #f6465d; color: #1a0508; }
    .cg-badge-bull-trend { background: #0ecb8122; color: #0ecb81; border: 1px solid #0ecb8144; }
    .cg-badge-bear-trend { background: #f6465d22; color: #f6465d; border: 1px solid #f6465d44; }
    .cg-badge-neutral { background: #1b202b; color: #7b8494; }

    /* Az expander vizuálisan a kártya alsó feléhez simuljon */
    div[data-testid="stExpander"] { border: 1px solid #1b202b !important; border-top: none !important; border-radius: 0 0 12px 12px !important; background: #0d1017 !important; margin-bottom: 16px; }
    div[data-testid="stExpander"] summary { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #7b8494 !important; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

BADGE_CLASS = {
    "Bullish Cross": "cg-badge-bull-cross",
    "Bearish Cross": "cg-badge-bear-cross",
    "Bullish (trend)": "cg-badge-bull-trend",
    "Bearish (trend)": "cg-badge-bear-trend",
    "Nincs kereszt": "cg-badge-neutral",
}

# ----------------------------------------------------------------------------
# 5) SIDEBAR - SZŰRŐK ÉS RENDEZÉS
# ----------------------------------------------------------------------------

st.sidebar.markdown("### ⚙️ Screener beállítások")

timeframe_label = st.sidebar.selectbox("Idősík (Timeframe)", list(TIMEFRAMES.keys()), index=1)
interval = TIMEFRAMES[timeframe_label]

max_symbols = st.sidebar.slider(
    "Vizsgált párok max. száma", min_value=20, max_value=400, value=120, step=10,
    help="Minél több párt vizsgálunk, annál tovább tart a frissítés az API rate limit miatt."
)

min_volume = st.sidebar.slider(
    "Minimum 24h Volumen (USDT)", min_value=0, max_value=50_000_000, value=500_000, step=100_000
)

available_categories = sorted(set(CATEGORY_MAP.values()) | {DEFAULT_CATEGORY})
selected_categories = st.sidebar.multiselect(
    "Kategória szűrő", options=available_categories, default=available_categories
)

search_text = st.sidebar.text_input("Keresés (pl. BTC-USDT)", value="").upper().strip()

st.sidebar.markdown("---")
st.sidebar.markdown("**Open Interest szűrők**")
only_oi_growth = st.sidebar.checkbox("📈 Csak növekvő OI")
oi_flat_price = st.sidebar.checkbox(
    f"🎯 OI nő (>{OI_GROWTH_THRESHOLD_PCT:.0f}%), ár oldalaz (±{PRICE_FLAT_THRESHOLD_PCT:.1f}%)"
)
st.sidebar.caption(
    "Az OI változás csak a 2. adatfrissítés után jelenik meg (session-alapú "
    "összehasonlítás, mert a BingX publikus API nem ad historikus OI-t)."
)

st.sidebar.markdown("---")
sort_choice = st.sidebar.selectbox(
    "Rendezés",
    ["Legnagyobb Volumen", "Legnagyobb OI Növekedés", "Legalacsonyabb RSI", "Legmagasabb RSI"],
    index=0,
)

if st.sidebar.button("🔄 Adatok frissítése (cache törlése)"):
    st.cache_data.clear()

# ----------------------------------------------------------------------------
# 6) ADAT BETÖLTÉS + OI VÁLTOZÁS (session-alapú összehasonlítás)
# ----------------------------------------------------------------------------

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

if "oi_prev_snapshot" not in st.session_state:
    st.session_state.oi_prev_snapshot = {}
oi_prev = st.session_state.oi_prev_snapshot


def _oi_delta(row):
    prev = oi_prev.get(row["Ticker"])
    curr = row["Open Interest"]
    if prev is None or pd.isna(curr) or prev == 0:
        return None
    return round((curr - prev) / prev * 100, 2)


df["OI Változás (%)"] = df.apply(_oi_delta, axis=1)
for _, r in df.iterrows():
    if not pd.isna(r["Open Interest"]):
        oi_prev[r["Ticker"]] = r["Open Interest"]

# --- Szűrők alkalmazása ---
filtered = df[df["Volume (24h, USDT)"] >= min_volume]
filtered = filtered[filtered["Kategória"].isin(selected_categories)]
if search_text:
    filtered = filtered[filtered["Ticker"].str.contains(search_text)]
if only_oi_growth:
    filtered = filtered[filtered["OI Változás (%)"] > 0]
if oi_flat_price:
    filtered = filtered[
        (filtered["OI Változás (%)"] > OI_GROWTH_THRESHOLD_PCT)
        & (filtered["Ár Változás (%)"] >= -PRICE_FLAT_THRESHOLD_PCT)
        & (filtered["Ár Változás (%)"] <= PRICE_FLAT_THRESHOLD_PCT)
    ]

# --- Rendezés ---
if sort_choice == "Legnagyobb Volumen":
    filtered = filtered.sort_values("Volume (24h, USDT)", ascending=False, na_position="last")
elif sort_choice == "Legnagyobb OI Növekedés":
    filtered = filtered.sort_values("OI Változás (%)", ascending=False, na_position="last")
elif sort_choice == "Legalacsonyabb RSI":
    filtered = filtered.sort_values("RSI (14)", ascending=True, na_position="last")
elif sort_choice == "Legmagasabb RSI":
    filtered = filtered.sort_values("RSI (14)", ascending=False, na_position="last")
filtered = filtered.reset_index(drop=True)

# ----------------------------------------------------------------------------
# 7) FEJLÉC + STATISZTIKA SÁV
# ----------------------------------------------------------------------------

now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
bullish_cross_count = int((filtered["MACD Státusz"] == "Bullish Cross").sum())
bearish_cross_count = int((filtered["MACD Státusz"] == "Bearish Cross").sum())
oi_growth_count = int((filtered["OI Változás (%)"] > 0).sum())
avg_rsi = filtered["RSI (14)"].mean() if len(filtered) else 0

header_html = (
    '<div class="cg-header">'
    '<div class="cg-title"><span class="cg-dot"></span> BINGX SCREENER</div>'
    f'<div class="cg-sub">{now_str} · {timeframe_label} · {len(filtered)}/{len(df)} pár</div>'
    '</div>'
    '<div class="cg-stats">'
    '<div class="cg-stat"><div class="cg-stat-label">Bullish Cross</div>'
    f'<div class="cg-stat-value cg-green">{bullish_cross_count}</div></div>'
    '<div class="cg-stat"><div class="cg-stat-label">Bearish Cross</div>'
    f'<div class="cg-stat-value cg-red">{bearish_cross_count}</div></div>'
    '<div class="cg-stat"><div class="cg-stat-label">Növekvő OI</div>'
    f'<div class="cg-stat-value cg-green">{oi_growth_count}</div></div>'
    '<div class="cg-stat"><div class="cg-stat-label">Átlag RSI</div>'
    f'<div class="cg-stat-value">{avg_rsi:.1f}</div></div>'
    '</div>'
)
st.markdown(header_html, unsafe_allow_html=True)

# ----------------------------------------------------------------------------
# 8) FORMÁZÓ SEGÉDFÜGGVÉNYEK
# ----------------------------------------------------------------------------

def fmt_compact(n):
    if n is None or pd.isna(n):
        return "–"
    n = float(n)
    for unit, div in [("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000)]:
        if abs(n) >= div:
            return f"{n/div:.2f}{unit}"
    return f"{n:.0f}"


def price_fmt(p):
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:,.4f}"
    return f"{p:.6f}"


def pct_fmt(v):
    if v is None or pd.isna(v):
        return "–", "cg-dim"
    sign = "+" if v > 0 else ""
    color = "cg-green" if v > 0 else ("cg-red" if v < 0 else "cg-dim")
    return f"{sign}{v:.2f}%", color


def build_candlestick_figure(kdf: pd.DataFrame, symbol: str, tf_label: str):
    """Interaktív gyertya + volumen chart Plotly-val. A nyitott (utolsó) gyertyát
    itt szándékosan MEGTARTJUK, mert vizuálisan hasznos látni az élő gyertyát."""
    if kdf is None or kdf.empty:
        return None
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28], vertical_spacing=0.04
    )
    fig.add_trace(go.Candlestick(
        x=kdf["timestamp"], open=kdf["open"], high=kdf["high"], low=kdf["low"], close=kdf["close"],
        increasing_line_color="#0ecb81", decreasing_line_color="#f6465d",
        increasing_fillcolor="#0ecb81", decreasing_fillcolor="#f6465d", name=symbol,
    ), row=1, col=1)
    vol_colors = ["#0ecb81" if c >= o else "#f6465d" for o, c in zip(kdf["open"], kdf["close"])]
    fig.add_trace(go.Bar(x=kdf["timestamp"], y=kdf["volume"], marker_color=vol_colors, name="Volume"), row=2, col=1)
    fig.update_layout(
        height=360, margin=dict(l=8, r=8, t=28, b=8),
        paper_bgcolor="#0d1017", plot_bgcolor="#0d1017",
        font=dict(color="#c7ccd6", family="JetBrains Mono, monospace", size=10.5),
        showlegend=False, xaxis_rangeslider_visible=False,
        title=dict(text=f"{symbol} · {tf_label}", font=dict(size=12.5, color="#f2f4f8")),
    )
    fig.update_xaxes(showgrid=False, row=1, col=1)
    fig.update_xaxes(showgrid=False, row=2, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="#1b202b", row=1, col=1)
    fig.update_yaxes(showgrid=False, row=2, col=1)
    return fig

# ----------------------------------------------------------------------------
# 9) KÁRTYALISTA + BEÁGYAZOTT CHART
# ----------------------------------------------------------------------------

if len(filtered) == 0:
    st.info("Nincs a szűrésnek megfelelő pár. Próbálj lazítani a szűrőkön.")
else:
    for _, row in filtered.iterrows():
        symbol = row["Ticker"]

        price_delta_txt, price_delta_color = pct_fmt(row["Ár Változás (%)"])
        vol_delta_txt, vol_delta_color = pct_fmt(row["Gyertya Vol. Változás (%)"])
        oi_delta_txt, oi_delta_color = pct_fmt(row["OI Változás (%)"])

        rsi_val = row["RSI (14)"]
        if pd.isna(rsi_val):
            rsi_color, rsi_txt = "cg-dim", "–"
        elif rsi_val >= 70:
            rsi_color, rsi_txt = "cg-red", f"{rsi_val:.1f}"
        elif rsi_val <= 30:
            rsi_color, rsi_txt = "cg-green", f"{rsi_val:.1f}"
        else:
            rsi_color, rsi_txt = "", f"{rsi_val:.1f}"

        badge_cls = BADGE_CLASS.get(row["MACD Státusz"], "cg-badge-neutral")
        bar_color = "#0ecb81" if row["Gyertya Vol. Változás (%)"] >= 0 else "#f6465d"
        bar_width = min(100, max(4, abs(row["Gyertya Vol. Változás (%)"])))

        # FONTOS: a HTML-t behúzás nélküli, egysorba fűzött darabokból építjük fel,
        # különben a Streamlit Markdown "kódblokknak" nézné (lásd korábbi hibajavítás).
        card_html = (
            '<div class="cg-card">'
            '<div class="cg-card-top">'
            f'<div><span class="cg-ticker">{symbol}</span>'
            f'<span class="cg-cat">{row["Kategória"]}</span></div>'
            '<div>'
            f'<div class="cg-price">${price_fmt(row["Ár"])}</div>'
            f'<div class="cg-price-delta {price_delta_color}">{price_delta_txt}</div>'
            '</div>'
            '</div>'
            '<div class="cg-metrics">'
            '<div><div class="cg-metric-label">Ár Δ</div>'
            f'<div class="cg-metric-value {price_delta_color}">{price_delta_txt}</div></div>'
            '<div><div class="cg-metric-label">Vol 24h</div>'
            f'<div class="cg-metric-value">{fmt_compact(row["Volume (24h, USDT)"])}</div></div>'
            '<div><div class="cg-metric-label">Vol Δ</div>'
            f'<div class="cg-metric-value {vol_delta_color}">{vol_delta_txt}</div></div>'
            '<div><div class="cg-metric-label">OI</div>'
            f'<div class="cg-metric-value">{fmt_compact(row["Open Interest"])}</div></div>'
            '<div><div class="cg-metric-label">OI Δ</div>'
            f'<div class="cg-metric-value {oi_delta_color}">{oi_delta_txt}</div></div>'
            '<div><div class="cg-metric-label">RSI</div>'
            f'<div class="cg-metric-value {rsi_color}">{rsi_txt}</div></div>'
            '</div>'
            f'<span class="cg-badge {badge_cls}">{row["MACD Státusz"]}</span>'
            '<div class="cg-volbar-track">'
            f'<div class="cg-volbar-fill" style="width:{bar_width:.0f}%; background:{bar_color};"></div>'
            '</div>'
            '</div>'
        )
        st.markdown(card_html, unsafe_allow_html=True)

        with st.expander(f"📈 Chart megnyitása – {symbol}"):
            load_key = f"chart_loaded_{symbol}_{interval}"
            already_loaded = st.session_state.get(load_key, False)
            if already_loaded or st.button("Chart betöltése", key=f"btn_{symbol}_{interval}"):
                st.session_state[load_key] = True
                kdf = fetch_symbol_klines_cached(symbol, interval)
                fig = build_candlestick_figure(kdf, symbol, timeframe_label)
                if fig is not None:
                    st.plotly_chart(fig, use_container_width=True, key=f"plot_{symbol}_{interval}")
                else:
                    st.caption("Nincs elég adat a chart megjelenítéséhez.")

st.markdown(
    '<p class="cg-sub" style="margin-top:10px;">⚠️ Az Ár Δ és Vol Δ mindig két '
    'egymást követő LEZÁRT gyertya összehasonlításából számol – a még formálódó '
    'gyertyát figyelmen kívül hagyjuk. Az OI Δ a legutóbbi adatfrissítéshez képesti '
    'változást mutatja (a BingX publikus API nem ad historikus OI-t).</p>',
    unsafe_allow_html=True
)
