# Changelog — BingX Skalp Felhalmozás-figyelő

Ez a fájl tartalmazza a szkript teljes verziótörténetét. A `alert_checker.py`
tetején csak egy rövid, aktuális állapotot összegző docstring maradt -
a részletes, kronologikus indoklások (mit, miért, hogyan változtattunk)
ide kerültek, hogy a kódfájl áttekinthető maradjon.

---

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

