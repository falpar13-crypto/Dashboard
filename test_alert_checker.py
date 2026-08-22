# -*- coding: utf-8 -*-
"""
Unit tesztek az alert_checker.py "tiszta" (mellékhatás nélküli) függvényeihez.

ÚJ: korábban egyáltalán nem volt teszt a fájlhoz, pedig épp ezek a
függvények (evaluate_candle, find_oi_baseline, get_active_killzone,
_simulate_trade_outcome) tartalmazzák a pénzügyi szempontból kritikus
logikát - egy elgépelt előjel vagy határfeltétel itt csendben téves
jelzéseket / téves winrate-számokat eredményezhetne.

Futtatás:
    pip install pytest pandas --break-system-packages
    pytest test_alert_checker.py -v
"""
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

sys.path.insert(0, ".")
import alert_checker as ac


# ----------------------------------------------------------------------------
# get_active_killzone
# ----------------------------------------------------------------------------

def test_killzone_london_start_inclusive():
    now = datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc)
    assert ac.get_active_killzone(now) == "London"


def test_killzone_london_end_inclusive():
    now = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert ac.get_active_killzone(now) == "London"


def test_killzone_new_york():
    now = datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc)
    assert ac.get_active_killzone(now) == "New York"


def test_killzone_none_outside_windows():
    now = datetime(2026, 1, 1, 20, 0, tzinfo=timezone.utc)
    assert ac.get_active_killzone(now) is None


def test_killzone_just_before_london():
    now = datetime(2026, 1, 1, 6, 59, tzinfo=timezone.utc)
    assert ac.get_active_killzone(now) is None


# ----------------------------------------------------------------------------
# find_oi_baseline
# ----------------------------------------------------------------------------

def test_find_oi_baseline_picks_closest_to_target():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    history = [
        {"ts": (now - timedelta(minutes=1)).isoformat(), "oi": 100},   # túl friss (< MIN)
        {"ts": (now - timedelta(minutes=5)).isoformat(), "oi": 200},   # pontosan a target
        {"ts": (now - timedelta(minutes=15)).isoformat(), "oi": 300},  # a max ablakon belül, de messzebb
        {"ts": (now - timedelta(minutes=25)).isoformat(), "oi": 400},  # túl régi (> MAX)
    ]
    baseline = ac.find_oi_baseline(history, now)
    assert baseline is not None
    assert baseline["oi"] == 200


def test_find_oi_baseline_returns_none_if_nothing_in_window():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    history = [{"ts": (now - timedelta(minutes=1)).isoformat(), "oi": 100}]
    assert ac.find_oi_baseline(history, now) is None


def test_find_oi_baseline_empty_history():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert ac.find_oi_baseline([], now) is None


# ----------------------------------------------------------------------------
# evaluate_candle
# ----------------------------------------------------------------------------

def _make_klines(closes, volumes, live_open, live_close, live_volume):
    """Segédfüggvény: VOLUME_MA_PERIOD lezárt gyertyát + 1 élő gyertyát épít."""
    rows = []
    for c, v in zip(closes, volumes):
        rows.append({"open": c, "high": c, "low": c, "close": c, "volume": v})
    rows.append({
        "open": live_open,
        "high": max(live_open, live_close),
        "low": min(live_open, live_close),
        "close": live_close,
        "volume": live_volume,
    })
    return pd.DataFrame(rows)


def test_evaluate_candle_none_if_not_enough_data():
    kdf = pd.DataFrame([{"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}])
    assert ac.evaluate_candle(kdf) is None


def test_evaluate_candle_long_direction_and_pct():
    n = ac.VOLUME_MA_PERIOD
    kdf = _make_klines(
        closes=[100.0] * n,
        volumes=[1000.0] * n,
        live_open=100.0,
        live_close=105.0,   # +5% a legutóbbi lezárt záróárhoz képest
        live_volume=3000.0,  # 3x az átlag volumen
    )
    result = ac.evaluate_candle(kdf)
    assert result is not None
    assert result["direction"] == "LONG"
    assert result["price_change_pct"] == pytest.approx(5.0, abs=0.01)
    assert result["vol_multiplier"] == pytest.approx(3.0, abs=0.01)
    assert result["signal_type"] == "STANDARD"


def test_evaluate_candle_short_direction():
    n = ac.VOLUME_MA_PERIOD
    kdf = _make_klines(
        closes=[100.0] * n,
        volumes=[1000.0] * n,
        live_open=100.0,
        live_close=95.0,   # -5%, és close < open -> SHORT
        live_volume=1000.0,
    )
    result = ac.evaluate_candle(kdf)
    assert result is not None
    assert result["direction"] == "SHORT"
    assert result["price_change_pct"] == pytest.approx(-5.0, abs=0.01)


def test_evaluate_candle_none_on_zero_avg_volume():
    n = ac.VOLUME_MA_PERIOD
    kdf = _make_klines(
        closes=[100.0] * n,
        volumes=[0.0] * n,   # átlag volumen 0 -> nem lehet szorzót számolni
        live_open=100.0,
        live_close=105.0,
        live_volume=100.0,
    )
    assert ac.evaluate_candle(kdf) is None


# ----------------------------------------------------------------------------
# _simulate_trade_outcome
# ----------------------------------------------------------------------------

def test_simulate_trade_outcome_long_hits_profit_before_sl():
    entry_price = 100.0
    candles = pd.DataFrame([
        {"high": 102.0, "low": 99.5},   # +2% elérve, SL (-1.5% = 98.5) nem sérül
        {"high": 101.0, "low": 100.5},
    ])
    result = ac._simulate_trade_outcome("LONG", entry_price, candles)
    assert result["sl_hit"] is False
    assert result["levels_reached"]["level_0.5pct"] is True
    assert result["levels_reached"]["level_1.0pct"] is True
    assert result["levels_reached"]["level_2.0pct"] is True
    assert result["levels_reached"]["level_3.0pct"] is False


def test_simulate_trade_outcome_long_hits_sl_immediately():
    entry_price = 100.0
    candles = pd.DataFrame([
        {"high": 100.1, "low": 98.0},  # SL (98.5) rögtön beütve, profit szint nem
    ])
    result = ac._simulate_trade_outcome("LONG", entry_price, candles)
    assert result["sl_hit"] is True
    assert all(v is False for v in result["levels_reached"].values())


def test_simulate_trade_outcome_short_direction():
    entry_price = 100.0
    candles = pd.DataFrame([
        {"high": 100.2, "low": 98.0},  # SHORT: kedvező irány a LOW, +2% elérve
    ])
    result = ac._simulate_trade_outcome("SHORT", entry_price, candles)
    assert result["sl_hit"] is False
    assert result["levels_reached"]["level_2.0pct"] is True


def test_simulate_trade_outcome_stops_simulation_after_sl():
    """Miután az SL beütött, a további gyertyák profitszintjei már nem
    számítanak bele - a szimulációnak meg kell állnia."""
    entry_price = 100.0
    candles = pd.DataFrame([
        {"high": 100.0, "low": 98.0},   # LONG SL (98.5) beütve itt
        {"high": 110.0, "low": 109.0},  # ez már nem számít, a kereskedés "lezárult"
    ])
    result = ac._simulate_trade_outcome("LONG", entry_price, candles)
    assert result["sl_hit"] is True
    assert result["levels_reached"]["level_3.0pct"] is False
