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

v4 VÁLTOZÁS - AUTOMATIKUS VISSZAIGAZOLÁS (v11-BEN ELTÁVOLÍTVA): a v4-v9 között
a bot minden jelzéshez elmentette, melyik gyertyáról volt szó, és amikor az
lezárt, egy második "✅ Megerősítve" / "❌ Visszafordult" / "➖ Semleges zárás"
Telegram-üzenetet is küldött. A felhasználói visszajelzés alapján ez túl sok
zajt (spam-et) okozott, ezért a v11-ben TELJESEN KIKERÜLT a kódból (lásd lent).

v5 VÁLTOZÁS - MAGASABB IDŐSÍK TREND-SZŰRŐ: mostantól a bot megnézi az adott
pár 1 órás trendjét (záróár az 1h EMA50-hez képest) is. Az 1h trendet
takarékosan, futásonként csak egyszer (nem minden 30 mp-es körben) kérdezzük
le és memóriában cache-eljük a futás hátralévő részére.

v6 VÁLTOZÁS - HTF FIGYELMEZTETÉS BLOKKOLÁS HELYETT: a v5-ben a trenddel
szembemenő jelzést egyszerűen NEM küldtük ki. A felhasználói visszajelzés
alapján ez túl szigorúnak bizonyult - mostantól a jelzés MINDIG kimegy, csak
egy "⚠️ Trenddel szemben (1h: DOWN/UP)" figyelmeztető sort kap az üzenet, ha
az irány nem egyezik az 1h trenddel. Így a döntés a felhasználónál marad.

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

v9 VÁLTOZÁS - RSI + MACD INFÓ: a bot most RSI(14)-et és MACD(12,26,9)-et is
számol az 5m adatokból (nincs plusz API-hívás, csak nagyobb limit ugyanarra a
lekérésre). Ez CSAK tájékoztató jellegű sor az üzenetben - nem szűr és nem
blokkol semmit. Az RSI mellett "(túlvett)"/"(túladott)" jelölés jelenik meg
RSI_OVERBOUGHT/RSI_OVERSOLD küszöbök alapján.

v9 VÁLTOZÁS - NCFX KIZÁRVA: az NCSK mellett az NCFX előtagú (szintén nem
kriptó, tokenizált) termékek is ki vannak zárva a jelöltek közül.

v11 VÁLTOZÁS - KILLZONE, FUNDING RATE, EMA SQUEEZE, SPAM-MENTESÍTÉS:
  - OI-küszöb 2.5% -> 1.5% (érzékenyebb jelzés).
  - ÚJ: Killzone (London 07:00-10:00 UTC, New York 13:30-16:00 UTC) infósor
    az üzenetben - CSAK tájékoztat, nem szűr.
  - ÚJ: Funding Rate lekérdezés (BingX premiumIndex végpont), Squeeze Vadász
    figyelmeztetéssel (short/long squeeze), amikor a jelzés iránya a
    finanszírozási rátával ellentétes pozíciók túlsúlyára utal. A funding
    rate-et, mivel ritkán (általában óránál ritkábban) változik érdemben,
    a HTF trendhez hasonlóan CSAK EGYSZER kérdezzük le szimbólumonként egy
    futáson belül, és memóriában cache-eljük (funding_cache) - így nem nő
    feleslegesen az API-terhelés minden 30 mp-es körben.
  - ÚJ: EMA Squeeze (beszorulás) kitörés-riasztás - önálló, a STANDARD/SÁV
    KITÖRÉS jelzésektől TELJESEN FÜGGETLEN logika, saját cooldown-nal és
    lazább OI/volumen-küszöbökkel. FONTOS: mivel teljesen független, egy
    adott élő gyertyára ELVILEG egyszerre mehet ki STANDARD/SÁV KITÖRÉS ÉS
    EMA SQUEEZE riasztás is (két külön Telegram-üzenet) - ez a kért
    függetlenség szándékos velejárója.
  - TÖRÖLVE: automatikus visszaigazolás (lásd v4 fenti megjegyzését) - a bot
    többé NEM küld "Megerősítve/Visszafordult/Semleges" üzeneteket.
  - SZELLŐS DIZÁJN: minden kiküldött üzenet elejére/végére egy-egy extra
    sortörés kerül, hogy Telegramon ne folyjanak össze az egymást követő
    riasztások.
  - Az 5m klines lekérés limitje (KLINES_LIMIT) 65 -> 120-ra nőtt, hogy az
    EMA Squeeze EMA(50) számítása stabilabb (jobban "bemelegedett") legyen.

v12 VÁLTOZÁS - EMA 20 REJECTION (MOZGÓÁTLAG-VISSZAUTASÍTÁS): ÚJ, önálló,
kizárólag SHORT (DUMP) irányú riasztás-logika, a STANDARD/SÁV KITÖRÉS/EMA
SQUEEZE jelzésektől TELJESEN FÜGGETLEN, saját cooldown-nal. Setup: korábbi
UP trend (EMA20 az EMA50 felett) után az ár betört az EMA20 alá, majd
alulról szorosan visszatesztelte azt, de nem tudott fölé zárni - a szint
"visszautasította", és az élő gyertya határozottan piros, folytatódó
beszakadást jelezve. A HTF trend/RSI/MACD/killzone/funding infósorok ennél a
jelzéstípusnál is megjelennek, csakúgy, mint a többinél. Lazított (60% OI /
70% volumen) küszöbökkel fut, ugyanúgy, mint az EMA Squeeze.

v13 VÁLTOZÁS - EMA 20/50 REJECTION KÉTFÁZISÚVÁ BŐVÍTVE + LAZÍTOTT KÜSZÖBÖK:
az EMA Rejection korábban csak addig működött, amíg EMA20 > EMA50 volt (csak
az EMA20-ról való visszapattanást ismerte fel). Mostantól, ha a letörés már
annyira megerősödött, hogy az EMA20 lekeresztezte az EMA50-et, a logika
automatikusan az EMA50-re, mint "alsó" szintre vált - ugyanazzal a letörés->
visszateszt->elutasítás mintával. Emellett a STANDARD/EMA SQUEEZE/EMA
REJECTION küszöbök (OI%, volumen-szorzó, visszateszt-tolerancia stb.) kicsit
lazábbak lettek, hogy több valós jelzés menjen ki.

v14 VÁLTOZÁS - NAPI WINRATE-ÖSSZESÍTŐ: minden kiküldött jelzést (típustól
függetlenül) a bot mostantól "megjegyez" (state-ben, "pending_outcomes" néven)
a belépő árral együtt, és OUTCOME_EVAL_MINUTES (30) perc múlva - a soron
következő körben, a már úgyis lekért ticker-árak alapján, TEHÁT NINCS PLUSZ
API-HÍVÁS - kiértékeli: a jelzés iránya szerint mozdult-e az ár legalább
OUTCOME_WIN_THRESHOLD_PCT (0.3%) %-ot (WIN), az ellenkező irányba (LOSS), vagy
egyik sem (NEUTRAL). Ezt egy önálló, append-only naplófájlba (alert_log.jsonl)
írja. Minden nap, kicsivel éjfél UTC után (hogy az előző nap utolsó jelzései
is stabilan kiértékelődjenek), egyetlen összesítő Telegram-üzenetben elküldi
az előző nap jelzéstípusonkénti darabszámát, W/L/N bontását és winrate-jét.
Ismert korlát: ha egy adott naphoz tartozó jelzés csak a nap váltása UTÁN,
a DAILY_SUMMARY_MIN_DELAY_MINUTES ablakon túl értékelődik ki (ritka, csak
akkor fordulhat elő, ha a szimbólum közben kikerül a jelöltlistából és
OUTCOME_MAX_STALE_MINUTES-ig nem sikerül árat találni hozzá), az az adott nap
összesítőjéből technikailag kimaradhat - ez egy tudatosan vállalt, apró
pontatlanság egy személyes használatra szánt eszköznél.

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
from zoneinfo import ZoneInfo
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
MIN_OI_INCREASE = 1.2       # v13: 1.5 -> 1.2, minimum OI-ugrás %-ban (~5 perces referenciaablak) -
                             # kicsit lazítva, hogy több jelzés menjen ki
MIN_CANDLE_VOL_USDT = 15_000  # az élő gyertya eddigi USDT-forgalmának minimuma

VOLUME_MA_PERIOD = 10       # ennyi megelőző LEZÁRT gyertya átlagához viszonyítunk
MIN_VOL_MULTIPLIER = 1.8    # v13: 2.0 -> 1.8, az élő gyertya eddigi volumene legalább ennyiszerese
                             # legyen az átlagnak - kicsit lazítva

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
EMA_SQUEEZE_MAX_EMA_GAP_PCT = 1.8      # v13: 1.5 -> 1.8, az EMA20 és EMA50 távolsága ennél kisebb legyen
EMA_SQUEEZE_MIN_OI_INCREASE = 0.7      # v13: 0.8 -> 0.7, a standardnál lazább OI-küszöb (a setup önmagában erős)
EMA_SQUEEZE_MIN_VOL_MULTIPLIER = 1.15  # v13: 1.3 -> 1.15, a standardnál lazább volumen-küszöb
EMA_SQUEEZE_MAX_PRICE_CHANGE = 5.0     # felső korlát az élő gyertya ármozgására - egy
                                        # adathiba/kiugrás miatti extrém "kitörést" szűr ki

# --- ÚJ (v12): EMA 20 Rejection (mozgóátlag-visszautasítás) - önálló, a
# STANDARD/SÁV KITÖRÉS/EMA SQUEEZE jelzésektől TELJESEN FÜGGETLEN, kizárólag
# SHORT (DUMP) irányú riasztás-logika. Setup: korábbi UP trend (EMA20>EMA50)
# után az ár betört az EMA20 alá, majd alulról visszatesztelte azt, de nem
# tudott fölé zárni - "visszautasítás" (rejection), és az élő gyertya
# határozottan piros, jelezve a beszakadás folytatódását. ---
EMA_REJECTION_FAST_PERIOD = 20
EMA_REJECTION_SLOW_PERIOD = 50
EMA_REJECTION_LOOKBACK_CANDLES = 8         # v13: 6 -> 8, hogy több lehetőség legyen a "letörés"
                                            # gyertyát megtalálni (lazítás, több jelzésért)
EMA_REJECTION_BREAK_BELOW_PCT = 0.08       # v13: 0.1 -> 0.08, ennyivel legyen a close "határozottan"
                                            # a szint alatt (%) - kicsit engedékenyebb
EMA_REJECTION_RETEST_TOLERANCE_PCT = 0.7   # v13: 0.5 -> 0.7, a visszateszt High-ja ennyi %-on belül
                                            # érintse a szintet - szélesebb "érintési" zóna
EMA_REJECTION_MIN_RED_BODY_PCT = 0.10      # v13: 0.15 -> 0.10, a trigger-gyertya piros teste legalább
                                            # ennyi % legyen (nyitó->jelenlegi ár), hogy ne zajszintű
                                            # mozgás triggereljen, de több valódi setup átjöjjön
EMA_REJECTION_MAX_PRICE_CHANGE = 5.0       # felső korlát az élő gyertya ármozgására (adathiba-védelem)
# Lazított OI/volumen-küszöbök, ugyanúgy, mint az EMA Squeeze-nél: a setup
# önmagában (price action) elég erős, ezért a standardnál engedékenyebb
# küszöbök is elfogadhatók. v13: tovább lazítva (a korábbi 60%/70% arányról),
# hogy a bővített (EMA20 + EMA50 kétfázisú) logika több jelzést hozzon:
EMA_REJECTION_MIN_OI_INCREASE = 0.7        # korábban 0.9
EMA_REJECTION_MIN_VOL_MULTIPLIER = 1.2     # korábban 1.4

# --- ÚJ (v3): belső ciklus időzítése egy GitHub Actions futáson belül ---
TOTAL_RUN_BUDGET_SECONDS = 280   # v15d: 240 -> 280 (~4m40s). MOST MÁR 6 PERCES
                                  # külső cron-intervallumhoz igazítva (a
                                  # cron-job.org beállítást is 5 -> 6 percre kell
                                  # állítani, lásd a válaszban)! 360s (6 perc) -
                                  # ~31s körítés - ~49s biztonsági tartalék = ~280s.
                                  # Ezzel a "vak rés" a ciklusok között gyakorlatilag
                                  # eltűnik, miközben a torlódás elleni tartalék is
                                  # bőven megmarad (retry-kkel járó lassabb git push
                                  # esetére is).
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
                                # emelése (15M -> 150M) várhatóan jelentősen
                                # megnöveli a jelöltlista méretét - érdemes az
                                # első pár futást figyelni (lásd a válaszban)
REQUEST_TIMEOUT = 10
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5
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

OUTCOME_EVAL_MINUTES = 30       # ennyi idő elteltével értékeljük ki a jelzést
OUTCOME_WIN_THRESHOLD_PCT = 0.3 # ennyi %-os, a jelzés iránya szerinti elmozdulás
                                 # kell WIN-hez (illetve ennek ellentettje LOSS-hoz);
                                 # a kettő közötti sáv NEUTRAL
OUTCOME_MAX_STALE_MINUTES = 90  # ha ennyi idő után sem sikerül árat találni a
                                 # szimbólumhoz (pl. kikerült a jelöltlistából),
                                 # UNKNOWN-ként lezárjuk, hogy ne ragadjon be

DAILY_SUMMARY_MIN_DELAY_MINUTES = OUTCOME_EVAL_MINUTES + 5  # ennyivel éjfél UTC
                                                              # után küldjük az
                                                              # előző napi összesítőt


def _log_signal_outcome(record: dict) -> None:
    """Egyetlen sort ír a napló (JSONL) fájlba - append-only, soha nem
    módosítunk/törlünk belőle korábbi sort."""
    try:
        with SIGNAL_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"HIBA: nem sikerült írni a jelzés-naplóba: {e}")


def register_pending_signal(state: dict, symbol: str, signal_type: str,
                             direction: str, entry_price: float, now: datetime) -> None:
    """Egy most kiküldött jelzést berak a 'pending_outcomes' listába - ezt
    fogja a resolve_pending_signals() a megfelelő időben kiértékelni."""
    pending = state.setdefault("pending_outcomes", [])
    pending.append({
        "id": f"{symbol}_{signal_type}_{now.strftime('%Y%m%dT%H%M%S')}",
        "symbol": symbol,
        "signal_type": signal_type,
        "direction": direction,
        "entry_price": entry_price,
        "entry_ts": now.isoformat(),
        "entry_date": now.astimezone(SUMMARY_TIMEZONE).strftime("%Y-%m-%d"),  # v15: helyi (nem UTC)
                                                    # dátum - ehhez a (helyi) naphoz számít a napi
                                                    # összesítőben, függetlenül attól, mikor zárul
                                                    # le ténylegesen a jelzés kiértékelése
        "evaluate_at_ts": (now + timedelta(minutes=OUTCOME_EVAL_MINUTES)).isoformat(),
    })


def resolve_pending_signals(state: dict, price_map: dict, now: datetime) -> None:
    """Végigmegy a függőben lévő jelzéseken, és amelyiknél letelt a
    kiértékelési ablak (vagy már túl régi ahhoz, hogy értelmes legyen tovább
    várni), lezárja: megnézi az irány szerinti %-os elmozdulást, WIN/LOSS/
    NEUTRAL-t rendel hozzá, és a végleges rekordot a naplófájlba írja."""
    pending = state.get("pending_outcomes", [])
    if not pending:
        return

    still_pending = []
    for item in pending:
        try:
            evaluate_at = datetime.fromisoformat(item["evaluate_at_ts"])
            entry_dt = datetime.fromisoformat(item["entry_ts"])
        except (KeyError, ValueError):
            continue  # sérült bejegyzés - eldobjuk, nem akadunk el rajta

        age_minutes = (now - entry_dt).total_seconds() / 60
        if now < evaluate_at:
            still_pending.append(item)
            continue

        current_price = price_map.get(item["symbol"])
        if current_price is None or current_price <= 0:
            if age_minutes >= OUTCOME_MAX_STALE_MINUTES:
                # Túl régóta nem sikerül árat találni ehhez a szimbólumhoz
                # (pl. kikerült a 24h volumen-sávból) - inkább UNKNOWN-ként
                # lezárjuk, mintsem örökre a listában ragadjon.
                _log_signal_outcome({**item, "exit_price": None,
                                      "exit_ts": now.isoformat(),
                                      "pnl_pct": None, "outcome": "UNKNOWN"})
            else:
                still_pending.append(item)  # próbáljuk újra a következő körben
            continue

        entry_price = item["entry_price"]
        if item["direction"] == "LONG":
            directional_pct = (current_price - entry_price) / entry_price * 100
        else:  # SHORT
            directional_pct = (entry_price - current_price) / entry_price * 100

        if directional_pct >= OUTCOME_WIN_THRESHOLD_PCT:
            outcome = "WIN"
        elif directional_pct <= -OUTCOME_WIN_THRESHOLD_PCT:
            outcome = "LOSS"
        else:
            outcome = "NEUTRAL"

        _log_signal_outcome({
            **item,
            "exit_price": current_price,
            "exit_ts": now.isoformat(),
            "pnl_pct": round(float(directional_pct), 3),
            "outcome": outcome,
        })

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
        print(f"HIBA: nem sikerült beolvasni a jelzés-naplót: {e}")
    return entries


def _format_daily_summary(date_str: str, entries: list) -> str:
    by_type = {}
    for rec in entries:
        by_type.setdefault(rec.get("signal_type", "ISMERETLEN"), []).append(rec)

    type_labels = {
        "STANDARD": "⚡ STANDARD",
        "RANGE_BREAKOUT": "🎯 SÁV KITÖRÉS",
        "EMA_SQUEEZE": "🗜️ EMA SQUEEZE",
        "EMA_REJECTION": "📉 EMA REJECTION",
    }

    lines = [f"📊 <b>Napi összesítő</b> ({date_str})", "━━━━━━━━━━━━━"]
    total_wins = total_losses = total_neutral = total_unknown = 0

    for sig_type in sorted(by_type.keys()):
        recs = by_type[sig_type]
        wins = sum(1 for r in recs if r.get("outcome") == "WIN")
        losses = sum(1 for r in recs if r.get("outcome") == "LOSS")
        neutral = sum(1 for r in recs if r.get("outcome") == "NEUTRAL")
        unknown = sum(1 for r in recs if r.get("outcome") == "UNKNOWN")
        total_wins += wins
        total_losses += losses
        total_neutral += neutral
        total_unknown += unknown

        decided = wins + losses
        winrate_str = f"{(wins / decided * 100):.1f}%" if decided > 0 else "n/a"
        label = type_labels.get(sig_type, sig_type)
        lines.append(
            f"{label}: {len(recs)} jelzés | {wins}W-{losses}L-{neutral}N"
            f"{f'-{unknown}?' if unknown else ''} | Winrate: {winrate_str}"
        )

    total = len(entries)
    total_decided = total_wins + total_losses
    total_winrate_str = f"{(total_wins / total_decided * 100):.1f}%" if total_decided > 0 else "n/a"
    lines.append("━━━━━━━━━━━━━")
    lines.append(
        f"Összesen: {total} jelzés | {total_wins}W-{total_losses}L-{total_neutral}N"
        f"{f'-{total_unknown}?' if total_unknown else ''} | Winrate: {total_winrate_str}"
    )
    return f"\n{chr(10).join(lines)}\n"


def maybe_send_daily_summary(state: dict, now: datetime) -> None:
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
        send_telegram_message(summary_msg)
        print(f"Napi winrate-összesítő elküldve ({yesterday_str}, {len(entries)} jelzés).")
    else:
        print(f"Nem volt jelzés {yesterday_str}-n - napi összesítő kihagyva.")

    state["_last_summary_date"] = today_str

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


def format_scalp_message(symbol, direction, price, price_change_pct,
                          candle_vol_usdt, vol_multiplier, oi_value, oi_change_pct,
                          htf_trend=None, bounce_confluence=False, near_level_risk=False,
                          rsi=None, macd_status=None, signal_type="STANDARD",
                          funding_rate=None, now=None, ema_rejection_level=None):
    action = DIRECTION_LABELS.get(direction, direction)
    if signal_type == "RANGE_BREAKOUT":
        header = f"🎯 SÁV KITÖRÉS ({action}): <b>{symbol}</b>"
    elif signal_type == "EMA_SQUEEZE":
        header = f"🗜️ EMA SQUEEZE KITÖRÉS ({action}): <b>{symbol}</b>"
    elif signal_type == "EMA_REJECTION":
        # v13: a jelzés most már megmutatja, melyik szintről (EMA20 vagy
        # EMA50) történt a visszautasítás - lásd detect_ema_rejection().
        level_note = f" [{ema_rejection_level}]" if ema_rejection_level else ""
        header = f"📉 EMA REJECTION{level_note} ({action}): <b>{symbol}</b>"
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


# ----------------------------------------------------------------------------
# ÚJ (v12), KIBŐVÍTVE v13-BAN: EMA 20/50 REJECTION (MOZGÓÁTLAG-VISSZAUTASÍTÁS)
# DETEKTÁLÁS - önálló, a STANDARD/SÁV KITÖRÉS/EMA SQUEEZE jelzésektől TELJESEN
# FÜGGETLEN, kizárólag SHORT (DUMP) irányú logika. Ugyanazokból a klines
# adatokból számol, nincs plusz API-hívás.
#
# v13 VÁLTOZÁS: a felhasználó egy TradingView chart-képernyőképet küldött,
# amin jól látszik, hogy egy erős dump során az ár EGYMÁS UTÁN, KÉT
# KÜLÖNBÖZŐ mozgóátlagról (fentről lefelé: előbb a gyorsabb EMA20-ról, majd -
# ahogy a letörés tovább erősödik és az EMA20 is lekeresztezi az EMA50-et -
# az EMA50-ről is) pattan vissza lefelé, folytatva a esést. A korábbi (v12)
# logika ezt csak az EMA20-ra nézte, és a "last_ema20 > last_ema50" feltétel
# miatt onnantól, hogy a trend teljesen megfordult (EMA20 az EMA50 ALÁ
# keresztezett), a második, EMA50-ös visszautasítást SOHA nem tudta jelezni -
# ez volt a hiányzó rész. A kép kérésének megfelelően EGY (a legfelső)
# HARMADIK, ennél is lejjebb lévő mozgóátlagot (pl. EMA100/200) TUDATOSAN NEM
# veszünk figyelembe - kizárólag a felső kettő (EMA20/EMA50) számít.
#
# Két, egymást kizáró FÁZIS van (mindig csak az egyik releváns az adott
# pillanatban, "fentről lefelé" haladva):
#   FÁZIS 1 (EMA20 a szint): amíg EMA20 > EMA50 (a trend csak most kezd
#     megtörni) - a letörés+visszateszt+elutasítás mintát az EMA20-ra nézzük.
#   FÁZIS 2 (EMA50 a szint): ha EMA20 <= EMA50 (az EMA20 már lekeresztezte az
#     EMA50-et, a letörés megerősödött) - innentől ugyanazt a mintát az
#     EMA50-re nézzük, hiszen a képen látható módon ez lesz a következő
#     "alsó" szint, amiről az ár visszapattanhat.
#
# Az egyes fázisokon belüli lépések (1 szintre, "level_ema"-ra vetítve):
# 1) Letörés: az utolsó EMA_REJECTION_LOOKBACK_CANDLES db LEZÁRT gyertya közül
#    legalább egynek a záróára határozottan (EMA_REJECTION_BREAK_BELOW_PCT
#    %-kal) az akkori level_ema alatt legyen - tehát a trend megtört.
# 2) Visszateszt: az utolsó LEZÁRT vagy az ÉLŐ gyertya High-ja alulról szorosan
#    (EMA_REJECTION_RETEST_TOLERANCE_PCT %-on belül) megközelítse a jelenlegi
#    level_ema-t.
# 3) Trigger: az élő gyertya határozottan piros (jelenlegi ár a nyitóár alatt,
#    minimum EMA_REJECTION_MIN_RED_BODY_PCT %-kal), ÉS a jelenlegi ár már a
#    level_ema alatt van - ez igazolja, hogy a szint tényleg visszautasította
#    az árat.
# ----------------------------------------------------------------------------

def _ema_rejection_level_triggered(closed: pd.DataFrame, live: pd.Series,
                                    level_ema_series: pd.Series, level_ema_now: float) -> bool:
    """Egy KONKRÉT EMA-szintre (lehet EMA20 vagy EMA50 is - a hívó dönti el)
    nézi meg a letörés -> visszateszt -> elutasítás hármas mintát. Ugyanazt a
    lépéssort futtatja le, amit korábban csak az EMA20-ra használtunk, immár
    újrafelhasználhatóan bármelyik EMA-ra."""
    if pd.isna(level_ema_now) or level_ema_now <= 0:
        return False

    # 1) Letörés: legalább egy lezárt gyertya close-ja határozottan az akkori
    # level_ema alatt zárt (a saját időpontjának megfelelő EMA-hoz
    # viszonyítva, nem a jelenlegihez - így a "megtört a trend" pillanatát
    # nézzük).
    lookback_closed = closed.iloc[-EMA_REJECTION_LOOKBACK_CANDLES:]
    lookback_ema = level_ema_series.iloc[-EMA_REJECTION_LOOKBACK_CANDLES:]
    break_threshold = lookback_ema * (1 - EMA_REJECTION_BREAK_BELOW_PCT / 100)
    broke_below = bool((lookback_closed["close"].values < break_threshold.values).any())
    if not broke_below:
        return False

    # 2) Visszateszt: az utolsó LEZÁRT gyertya VAGY az ÉLŐ gyertya High-ja
    # alulról szorosan megközelítette a (jelenlegi) level_ema-t.
    tolerance = level_ema_now * (EMA_REJECTION_RETEST_TOLERANCE_PCT / 100)
    channel_low = level_ema_now - tolerance
    channel_high = level_ema_now + tolerance

    last_closed_high = float(closed["high"].iloc[-1])
    live_high = float(live["high"])
    retested = (channel_low <= last_closed_high <= channel_high) or (channel_low <= live_high <= channel_high)
    if not retested:
        return False

    # 3) Trigger: az élő gyertya határozottan piros, ÉS a jelenlegi ár már a
    # level_ema alatt van (igazolva, hogy a szint tényleg visszautasított).
    live_open = float(live["open"])
    live_close = float(live["close"])
    if live_open <= 0:
        return False

    red_body_pct = (live_open - live_close) / live_open * 100
    if red_body_pct < EMA_REJECTION_MIN_RED_BODY_PCT:
        return False
    if live_close >= level_ema_now:
        return False

    return True


def detect_ema_rejection(closed: pd.DataFrame, live: pd.Series):
    """Visszaad egy (irány, szint) párost - ("SHORT", "EMA20") vagy
    ("SHORT", "EMA50") -, ha teljesül az EMA Rejection setup a megfelelő
    fázisban, egyébként (None, None)-t. Lásd a fenti blokk-kommentet a két
    fázis (EMA20, majd EMA50) logikájáról."""
    min_len = EMA_REJECTION_SLOW_PERIOD + EMA_REJECTION_LOOKBACK_CANDLES
    if len(closed) < min_len:
        return None, None

    ema20 = closed["close"].ewm(span=EMA_REJECTION_FAST_PERIOD, adjust=False).mean()
    ema50 = closed["close"].ewm(span=EMA_REJECTION_SLOW_PERIOD, adjust=False).mean()

    last_ema20 = ema20.iloc[-1]
    last_ema50 = ema50.iloc[-1]
    if pd.isna(last_ema20) or pd.isna(last_ema50):
        return None, None

    if last_ema20 > last_ema50:
        # FÁZIS 1: a trend csak most kezd megtörni - az EMA20 (a "felső",
        # gyorsabb átlag) a releváns visszautasítási szint.
        if _ema_rejection_level_triggered(closed, live, ema20, last_ema20):
            return "SHORT", "EMA20"
        return None, None

    # FÁZIS 2: az EMA20 már lekeresztezte lefelé az EMA50-et (a letörés
    # megerősödött) - innentől az EMA50 a releváns, "alsó" szint. Egy
    # esetleges HARMADIK, még lejjebb lévő mozgóátlagot (a kérésnek
    # megfelelően) itt sem veszünk figyelembe.
    if _ema_rejection_level_triggered(closed, live, ema50, last_ema50):
        return "SHORT", "EMA50"

    return None, None


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

    # ÚJ (v12, kibővítve v13-ban): EMA 20/50 Rejection (mozgóátlag-
    # visszautasítás) - önálló, kizárólag SHORT irányú kiegészítő jelzés.
    # v13-tól kétfázisú: EMA20-ról VAGY (ha a trend már lejjebb tartott)
    # EMA50-ről való visszautasítást is felismeri - lásd detect_ema_rejection.
    ema_rejection_signal, ema_rejection_level = detect_ema_rejection(closed, live)

    return {
        "price": current_price,
        "price_change_pct": round(float(price_change_pct), 2),
        "vol_multiplier": round(float(vol_multiplier), 2),
        "candle_vol_usdt": candle_vol_usdt,
        "direction": direction,
        "rsi": rsi_val,
        "macd_status": macd_status,
        "signal_type": signal_type,
        "ema_squeeze_signal": ema_squeeze_signal,
        "ema_gap_pct": ema_gap_pct,
        "ema_rejection_signal": ema_rejection_signal,
        "ema_rejection_level": ema_rejection_level,
    }

# ----------------------------------------------------------------------------
# EGY KIÉRTÉKELÉSI KÖR (a belső 30 mp-es ciklus egy "üteme")
# ----------------------------------------------------------------------------

async def run_single_pass(state: dict, valid_contracts, htf_cache: dict, funding_cache: dict, now: datetime):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tickers = await fetch_all_tickers(session)
        if not tickers:
            print("Nem sikerült ticker adatot lekérni a BingX API-ból, kör kihagyva.")
            return 0, 0, valid_contracts, htf_cache, funding_cache

        if valid_contracts is None:
            valid_contracts = await fetch_valid_contract_symbols(session)

        # ÚJ (v14): a korábban kiküldött, még függőben lévő jelzések
        # kiértékelése - ugyanabból a ticker-lekérésből, amit fentebb úgyis
        # elvégeztünk minden szimbólumra, tehát NINCS plusz API-hívás.
        price_map = {s: info.get("last_price") for s, info in tickers.items()}
        resolve_pending_signals(state, price_map, now)

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
        kline_tasks = [fetch_klines(session, semaphore, s, ALERT_TIMEFRAME) for s in candidates]
        htf_tasks = [fetch_htf_trend(session, semaphore, s) for s in missing_htf]
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
            # ÚJ (v14): a jelzést a napi winrate-összesítőhöz is regisztráljuk.
            register_pending_signal(state, symbol, candle.get("signal_type", "STANDARD"),
                                     candle["direction"], candle["price"], now)
            trend_note = " ⚠️ TRENDDEL SZEMBEN" if against_trend else ""
            bounce_note = " 🎯 SZINT-VISSZAPATTANÁS" if bounce_confluence else ""
            type_note = f" [{candle.get('signal_type', 'STANDARD')}]"
            print(f"JELZÉS küldve: {symbol} [{candle['direction']}]{type_note} (Ár {candle['price_change_pct']:+.2f}%, "
                  f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%, "
                  f"1h trend: {htf_trend or 'ismeretlen'}){trend_note}{bounce_note}")

        # --- ÚJ: EMA SQUEEZE (beszorulás) - önálló, a fenti STANDARD/SÁV
        # KITÖRÉS jelzéstől TELJESEN FÜGGETLEN riasztás, saját cooldown-nal,
        # lazább volumen/OI küszöbökkel (a szoros EMA-csatorna kitörése
        # önmagában is erős setup), de saját felső ár-mozgás korláttal is
        # (nehogy egy adathiba/kiugrás extrém "kitörésként" jelentkezzen).
        ema_signal = candle.get("ema_squeeze_signal")
        if ema_signal is not None:
            ema_is_setup = (
                abs(candle["price_change_pct"]) <= EMA_SQUEEZE_MAX_PRICE_CHANGE
                and oi_change_pct >= EMA_SQUEEZE_MIN_OI_INCREASE
                and candle["vol_multiplier"] >= EMA_SQUEEZE_MIN_VOL_MULTIPLIER
                and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
            )
            ema_cooldown_ok = True
            if entry.get("last_ema_squeeze_alert_ts"):
                last_ema_dt = datetime.fromisoformat(entry["last_ema_squeeze_alert_ts"])
                if (now - last_ema_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    ema_cooldown_ok = False

            if ema_is_setup and ema_cooldown_ok:
                # Az EMA Squeeze iránya (ema_signal) eltérhet a fenti STANDARD
                # jelzés irányától, ezért a szint-közelséget külön, az EMA
                # Squeeze SAJÁT irányára nézve számoljuk újra.
                ema_near_level_risk = (
                    (ema_signal == "LONG" and near_resistance)
                    or (ema_signal == "SHORT" and near_support)
                )
                ema_bounce_confluence = (
                    (ema_signal == "LONG" and near_support)
                    or (ema_signal == "SHORT" and near_resistance)
                )
                ema_msg = format_scalp_message(
                    symbol, ema_signal, candle["price"], candle["price_change_pct"],
                    candle["candle_vol_usdt"], candle["vol_multiplier"],
                    oi_now, oi_change_pct, htf_trend=htf_trend,
                    bounce_confluence=ema_bounce_confluence, near_level_risk=ema_near_level_risk,
                    rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                    signal_type="EMA_SQUEEZE",
                    funding_rate=funding_rate, now=now,
                )
                send_telegram_message(ema_msg)
                entry["last_ema_squeeze_alert_ts"] = now.isoformat()
                alerts_sent += 1
                # ÚJ (v14): a jelzést a napi winrate-összesítőhöz is regisztráljuk.
                register_pending_signal(state, symbol, "EMA_SQUEEZE", ema_signal, candle["price"], now)
                print(f"JELZÉS küldve: {symbol} [{ema_signal}] [EMA_SQUEEZE] (EMA gap {candle.get('ema_gap_pct')}%, "
                      f"Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%)")

        # --- ÚJ (v12): EMA 20 REJECTION (mozgóátlag-visszautasítás) - önálló,
        # a STANDARD/SÁV KITÖRÉS/EMA SQUEEZE jelzésektől TELJESEN FÜGGETLEN,
        # kizárólag SHORT (DUMP) irányú riasztás, saját cooldown-nal, lazított
        # (60% OI / 70% volumen) küszöbökkel - ugyanúgy, mint az EMA Squeeze-nél,
        # mivel ez is egy erős, önmagában is jól definiált price action setup. ---
        ema_rejection_signal = candle.get("ema_rejection_signal")
        if ema_rejection_signal is not None:
            rejection_is_setup = (
                abs(candle["price_change_pct"]) <= EMA_REJECTION_MAX_PRICE_CHANGE
                and oi_change_pct >= EMA_REJECTION_MIN_OI_INCREASE
                and candle["vol_multiplier"] >= EMA_REJECTION_MIN_VOL_MULTIPLIER
                and candle["candle_vol_usdt"] >= MIN_CANDLE_VOL_USDT
            )
            rejection_cooldown_ok = True
            if entry.get("last_ema_rejection_alert_ts"):
                last_rejection_dt = datetime.fromisoformat(entry["last_ema_rejection_alert_ts"])
                if (now - last_rejection_dt) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    rejection_cooldown_ok = False

            if rejection_is_setup and rejection_cooldown_ok:
                # Az EMA Rejection mindig SHORT irányú, de a szint-közelséget
                # (mint az EMA Squeeze-nél) erre az irányra nézve számoljuk.
                rejection_near_level_risk = (ema_rejection_signal == "SHORT" and near_support)
                rejection_bounce_confluence = (ema_rejection_signal == "SHORT" and near_resistance)
                rejection_msg = format_scalp_message(
                    symbol, ema_rejection_signal, candle["price"], candle["price_change_pct"],
                    candle["candle_vol_usdt"], candle["vol_multiplier"],
                    oi_now, oi_change_pct, htf_trend=htf_trend,
                    bounce_confluence=rejection_bounce_confluence, near_level_risk=rejection_near_level_risk,
                    rsi=candle.get("rsi"), macd_status=candle.get("macd_status"),
                    signal_type="EMA_REJECTION",
                    funding_rate=funding_rate, now=now,
                    ema_rejection_level=candle.get("ema_rejection_level"),
                )
                send_telegram_message(rejection_msg)
                entry["last_ema_rejection_alert_ts"] = now.isoformat()
                alerts_sent += 1
                # ÚJ (v14): a jelzést a napi winrate-összesítőhöz is regisztráljuk.
                register_pending_signal(state, symbol, "EMA_REJECTION", ema_rejection_signal, candle["price"], now)
                print(f"JELZÉS küldve: {symbol} [{ema_rejection_signal}] [EMA_REJECTION/{candle.get('ema_rejection_level')}] "
                      f"(Vol {candle['vol_multiplier']:.1f}x átlag, OI {oi_change_pct:+.2f}%)")

    if htf_warned:
        print(f"  (ebben a körben {htf_warned} kiküldött jelzés ment trenddel szemben - figyelmeztetéssel)")
    if sr_warned:
        print(f"  (ebben a körben {sr_warned} kiküldött jelzés ment támasz/ellenállás ellen - figyelmeztetéssel)")

    # ÚJ (v14): ha új UTC nap kezdődött (és eltelt egy kis idő éjfél óta),
    # elküldi az előző nap winrate-összesítőjét. A state-alapú gate miatt
    # (lásd maybe_send_daily_summary) naponta csak egyszer megy ki, akárhány
    # 30 mp-es körben is fut le ez a függvény.
    maybe_send_daily_summary(state, now)

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
