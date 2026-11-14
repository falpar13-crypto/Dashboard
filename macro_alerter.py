"""
Makró Figyelmeztető Bot (macro_alerter.py)
====================================================================
Ez a script a ForexFactory ingyenes JSON végpontját használja.
Két üzemmódja van (az aktuális futtatási időpont alapján dönti el):
1. Reggeli mód (UTC 06:00 körül): Kilistázza a mai napi High Impact USD eseményeket.
2. Live mód: Megnézi, hogy a közelmúltban volt-e fontos adatközlés, és elküldi az eredményeket.
Automatikusan kezeli a téli/nyári időszámítást!

ÚJ (javítás): állapot-alapú DUPLIKÁCIÓ-VÉDELEM. A korábbi verzió minden
futásnál újra elküldte az üzenetet, ha a feltételek (időablak) teljesültek -
ez azt jelentette, hogy egy kézi (workflow_dispatch) újraindítás, vagy egy
sűrűbb, biztonsági-háló jellegű ütemezés (lásd a yml-t) UGYANAZT az
eseményt/reggeli összefoglalót TÖBBSZÖR is kiküldte volna. Most egy kis
state fájl (macro_state.json) nyilvántartja, mely eseményeket és mely
napi összefoglalót küldtük már ki - ezek nem mennek ki újra.
"""

import html
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

# --- BEÁLLÍTÁSOK ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FF_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Csak ezek a kulcsszavak érdekelnek minket a High Impact események közül (angolul)
TARGET_EVENTS = ["CPI", "PPI", "FOMC", "Non-Farm", "Fed", "Interest Rate", "GDP"]

STATE_FILE = Path(__file__).parent / "macro_state.json"

# ----------------------------------------------------------------------------
# ÚJ: RÖVID/HOSSZÚ TÁVÚ VÁRHATÓ HATÁS BECSLÉSE
# ----------------------------------------------------------------------------
# TISZTÁN TÁJÉKOZTATÓ - nem előrejelzés, csak bevált, dokumentált piaci
# ökölszabályok (2026 szeptemberi kutatás alapján). A valós reakció mindig
# kontextusfüggő (pozicionáltság, egyidejű hírek, a Fed hangneme stb.) - a
# tényleges piaci mozgás ELTÉRHET ettől. FONTOS ÁRNYALAT: a munkaerőpiaci
# adatoknál (NFP, munkanélküliség) a hatás KÉTÉLŰ - egy erős adat egyszerre
# jó jel (egészséges gazdaság) ÉS rossz jel (inflációs/kamatemelési
# kockázat) is lehet, a piac aktuális "narratívájától" függően. 2026
# szeptemberében a Fed inkább KAMATEMELÉST fontolgat a tartós infláció
# miatt (nem csökkentést) - ez a jelenlegi értelmezés alapja, ami idővel
# változhat, ha a Fed-politika iránya megfordul.
def _parse_numeric(value_str) -> Optional[float]:
    """Megpróbál kinyerni egy numerikus értéket egy makró-adat mezőből
    (pl. "3.4%", "180K", "-0.2%") - None-t ad vissza, ha nem sikerül
    (pl. FOMC-nál gyakran egy kamatsáv vagy szöveges döntés jön, nem
    egyetlen szám)."""
    if not value_str or value_str in ("N/A", ""):
        return None
    s = str(value_str).strip().replace("%", "").replace(",", "")
    multiplier = 1.0
    if s[-1:].upper() == "K":
        multiplier = 1_000.0
        s = s[:-1]
    elif s[-1:].upper() == "M":
        multiplier = 1_000_000.0
        s = s[:-1]
    elif s[-1:].upper() == "B":
        multiplier = 1_000_000_000.0
        s = s[:-1]
    try:
        return float(s) * multiplier
    except ValueError:
        return None


def estimate_market_impact(title: str, actual, forecast) -> Optional[str]:
    """Kategória (infláció / kamatdöntés / munkaerőpiac / GDP) + a
    tény-vs-várt meglepetés iránya alapján ad egy rövid, tájékoztató
    jellegű becslést a várható rövid/hosszú távú hatásról."""
    title_lower = (title or "").lower()
    actual_val = _parse_numeric(actual)
    forecast_val = _parse_numeric(forecast)

    surprise = None
    if actual_val is not None and forecast_val is not None:
        diff = actual_val - forecast_val
        if abs(diff) < 1e-9:
            surprise = "inline"
        elif diff > 0:
            surprise = "higher"
        else:
            surprise = "lower"

    # --- Inflációs adatok (CPI, PPI) ---
    if "cpi" in title_lower or "ppi" in title_lower:
        if surprise == "higher":
            return ("📉 <i>Várható hatás:</i> a vártnál MAGASABB infláció jellemzően "
                    "\"hawkish\" jel (kamatemelés esélye nő / csökkentés elhalasztódik) "
                    "→ rövid távon RISK-OFF nyomás valószínű a kriptón. Ha a trend "
                    "folytatódik, hosszabb távon is tartósabb nyomást jelenthet.")
        elif surprise == "lower":
            return ("📈 <i>Várható hatás:</i> a vártnál ALACSONYABB infláció jellemzően "
                    "\"dovish\" jel (kamatemelési kockázat csökken) → rövid távon "
                    "RISK-ON hangulat valószínű. Tartós dezinfláció esetén hosszabb "
                    "távon is erősítheti a kockázatvállalási kedvet.")
        elif surprise == "inline":
            return ("➡️ <i>Várható hatás:</i> a várakozásnak megfelelő adat - "
                    "jellemzően enyhe/vegyes reakció, a piac inkább a következő "
                    "adatokra és a Fed-kommentárokra fókuszál.")
        # JAVÍTVA: ha nincs szám (surprise=None, pl. "N/A"), ne állítsuk, hogy "inline" volt
        return None

    # --- Kamatdöntés (FOMC, Fed, Interest Rate) ---
    if "fomc" in title_lower or "interest rate" in title_lower or "fed" in title_lower:
        return ("⚖️ <i>Várható hatás:</i> kamatdöntésnél NEM elsősorban a szám "
                "számít, hanem a Fed HANGNEME és a jövőbeli pálya (dot plot, "
                "sajtótájékoztató). Kamatemelés/hawkish hangnem → RISK-OFF "
                "(nyomás a kriptón). Kamatcsökkentés/dovish hangnem → RISK-ON. "
                "2026 őszén a Fed inkább kamatemelést fontolgat a tartós "
                "infláció miatt - ez emeli a \"hawkish meglepetés\" esélyét.")

    # --- Munkaerőpiaci adatok (Non-Farm Payrolls, munkanélküliség) ---
    if "non-farm" in title_lower or "payroll" in title_lower or "unemployment" in title_lower:
        return ("🔀 <i>Várható hatás:</i> a munkaerőpiaci adatok hatása KÉTÉLŰ - "
                "erős adat egyszerre jó jel (egészséges gazdaság) ÉS rossz jel "
                "(inflációs/kamatemelési kockázat), a piac aktuális "
                "narratívájától függően. A jelenlegi (inflációval küzdő) "
                "környezetben az ERŐS adat inkább hawkish/RISK-OFF, a GYENGE "
                "adat inkább dovish/RISK-ON irányba hathat - de ez gyorsan "
                "válthat, ha a hangsúly növekedési félelmekre kerül.")

    # --- GDP ---
    if "gdp" in title_lower:
        if surprise == "higher":
            return ("📊 <i>Várható hatás:</i> a vártnál ERŐSEBB GDP jellemzően "
                    "pozitív a kockázati eszközöknek, DE magas inflációs "
                    "aggodalom mellett hawkish jelként is értelmezhető - vegyes "
                    "reakció is elképzelhető.")
        elif surprise == "lower":
            return ("📊 <i>Várható hatás:</i> a vártnál GYENGÉBB GDP növekedési "
                    "félelmeket válthat ki (kockázatkerülés), bár enyhítheti a "
                    "kamatemelési várakozásokat is - a reakció iránya "
                    "bizonytalanabb, mint a tiszta inflációs adatoknál.")
        return None

    return None

# ÚJ: a "live" ablakot 15 percről 60 percre bővítettük. Ennek oka: a yml
# mostantól egy sűrűbb, biztonsági-háló jellegű ütemezéssel is fut (lásd
# macro-bot.yml), hogy a NEM hardcode-olt időpontban közölt eseményeket
# (pl. váratlan Fed-beszéd, elhalasztott adatközlés) is elkapja - de mivel
# ehhez a GitHub saját cron-ja nem garantáltan pontos percre, egy szűkebb
# (15 perces) ablak könnyen "átlógna" két futás között. A duplikáció-
# védelem (lásd lent) miatt a szélesebb ablak nem okoz többszörös küldést.
LIVE_WINDOW_MINUTES = 60

# Ennél régebbi state-bejegyzéseket eldobjuk íráskor, ne nőjön korlátlanul
STATE_RETENTION_DAYS = 3


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            # JAVÍTVA: érvényes JSON, de nem dict (pl. lista) -> a setdefault() elszállt volna
            if isinstance(loaded, dict):
                return loaded
            print("A state fájl formátuma hibás, üres állapotból indulunk.")
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            print("A state fájl olvasása sikertelen, üres állapotból indulunk.")
    return {"sent_event_ids": [], "sent_summary_dates": []}


def save_state(state: dict) -> None:
    tmp_path = STATE_FILE.with_suffix(".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp_path, STATE_FILE)
    except OSError as e:
        print(f"State mentési hiba: {e}")


def _event_id(event: dict) -> str:
    """Egy esemény egyedi azonosítója - cím + időpont kombinációja. Ez
    azonosítja ugyanazt az eseményt akkor is, ha több futás is látja."""
    return f"{event.get('title', '')}|{event.get('date', '')}"


def send_telegram_message(text: str) -> bool:
    """JAVÍTVA: bool-t ad vissza (sikeres volt-e a küldés), 4096 karakteres
    limit miatt sorok mentén darabol, 429 esetén egyszer újrapróbál."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("HIBA: Hiányzó Telegram hitelesítő adatok!")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    chunks, cur = [], ""
    for line in text.split("\n"):
        line = line[:4000]
        if cur and len(cur) + len(line) + 1 > 4000:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)

    for chunk in chunks:
        ok = False
        for attempt in range(2):
            try:
                resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"}, timeout=10)
                if resp.status_code == 429 and attempt == 0:
                    try:
                        wait = float(resp.json().get("parameters", {}).get("retry_after", 3))
                    except Exception:
                        wait = 3.0
                    time.sleep(min(max(wait, 1.0), 30.0))
                    continue
                if resp.status_code == 200:
                    ok = True
                else:
                    print(f"Telegram hiba ({resp.status_code}): {resp.text}")
                break
            except Exception as e:
                print(f"Telegram küldési hiba: {e}")
                break
        if not ok:
            return False
    return True


def get_macro_data():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(FF_JSON_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        # JAVÍTVA: nem lista alakú válasz (pl. hibaobjektum) esetén is None, ne iteráljunk kulcsokon
        if not isinstance(data, list):
            print("Váratlan alakú válasz a ForexFactory végpontról.")
            return None
        return data
    except Exception as e:
        print(f"Hiba az adatok lekérésekor: {e}")
        # JAVÍTVA: None (nem üres lista), hogy a hívó meg tudja különböztetni a hibát a "nincs esemény"-től
        return None


def main():
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    state = load_state()
    state.setdefault("sent_event_ids", [])
    state.setdefault("sent_summary_dates", [])

    events = get_macro_data()
    if events is None:
        # JAVÍTVA: lekérési hiba esetén NE jelöljük késznek a mai összefoglalót és ne mentsünk state-et
        print("Nincs adat (lekérési hiba) - a futás kihagyva, state érintetlen.")
        return

    today_events = []
    recent_events = []

    for event in events:
        # Csak a magas prioritású USD események érdekelnek minket
        if not isinstance(event, dict):  # JAVÍTVA: nem dict elem nem dönti el a futást
            continue
        if event.get("country") != "USD" or event.get("impact") != "High":
            continue

        # Címke szűrés (Hogy ne kapjunk értesítést minden apróságról)
        title = event.get("title") or ""  # JAVÍTVA: None cím esetén nem szállunk el a .lower()-en
        if not any(keyword.lower() in title.lower() for keyword in TARGET_EVENTS):
            continue

        try:
            # A ForexFactory időbélyeg formátuma: 2024-02-13T08:30:00-05:00
            event_time = datetime.fromisoformat(event.get("date")).astimezone(timezone.utc)
        except Exception:
            continue

        # Ha ma van az esemény
        if event_time.date() == now_utc.date():
            today_events.append((event_time, event))

        # ÚJ: ha a live-ablakon (LIVE_WINDOW_MINUTES) belül történt az
        # adatközlés, ÉS még nem küldtük ki (dedup az event_id alapján)
        time_diff = (now_utc - event_time).total_seconds() / 60
        if 0 <= time_diff <= LIVE_WINDOW_MINUTES:
            # JAVÍTVA: ha a feed még nem frissítette a "tény" mezőt (friss közlés), ne küldjünk
            # "N/A" üzenetet és ne jelöljük kiküldöttnek - egy későbbi futás pótolhatja
            if not event.get("actual") and time_diff < 10:
                continue
            event_id = _event_id(event)
            if event_id not in state["sent_event_ids"]:
                recent_events.append((event_id, event))

    # --- 1. MÓD: REGGELI ÖSSZEFOGLALÓ (Ha UTC 05:00 és 07:00 között futunk) ---
    if 5 <= now_utc.hour <= 7:
        # ÚJ: csak akkor küldjük el, ha MA MÉG NEM küldtük ki - enélkül egy
        # sűrűbb (pl. 15 percenkénti) ütemezés a 2 órás ablakon belül
        # TÖBBSZÖR is elküldené ugyanazt a reggeli összefoglalót.
        if today_str in state["sent_summary_dates"]:
            print("A mai reggeli összefoglalót már kiküldtük - kihagyva.")
            return
        if today_events:
            msg = "📅 <b>NAPI MAKRÓ FIGYELMEZTETÉS</b>\n<i>Extrém volatilitás várható ma!</i>\n\n"
            for etime, ev in sorted(today_events, key=lambda x: x[0]):
                time_str = etime.strftime("%H:%M (UTC)")
                msg += f"⏰ {time_str} - <b>{html.escape(str(ev.get('title', '')))}</b>\n"  # JAVÍTVA: HTML escape
            # JAVÍTVA: csak sikeres küldés esetén jelöljük késznek a napot
            if not send_telegram_message(msg):
                print("Reggeli összefoglaló küldése sikertelen - később újrapróbáljuk.")
                return
            print("Reggeli összefoglaló elküldve.")
        else:
            print("Ma nincs High Impact USD esemény.")
        state["sent_summary_dates"].append(today_str)
        _prune_and_save_state(state, now_utc)
        return  # Reggel nem csinálunk mást

    # --- 2. MÓD: LIVE ADATKÖZLÉS (Napközbeni futások) ---
    if recent_events:
        msg = "🚨 <b>MAKRÓ ADAT MEGJELENT!</b> 🚨\n\n"
        sent_ids = []
        for event_id, ev in recent_events:
            title = ev.get("title") or "Ismeretlen"
            actual = ev.get("actual", "") or "N/A"
            forecast = ev.get("forecast", "") or "N/A"
            previous = ev.get("previous", "") or "N/A"

            # JAVÍTVA: HTML escape (pl. "<0.1%" vagy "&" a mezőkben elrontaná a Telegram HTML parse-t)
            msg += f"📌 <b>{html.escape(str(title))}</b>\n"
            msg += f"Tény: <b>{html.escape(str(actual))}</b>\n"
            msg += f"Várt: {html.escape(str(forecast))} | Előző: {html.escape(str(previous))}\n"

            # ÚJ: rövid/hosszú távú várható hatás - lásd
            # estimate_market_impact() blokk-kommentjét a módszertanért.
            impact_note = estimate_market_impact(title, actual, forecast)
            if impact_note:
                msg += f"{impact_note}\n"
            msg += "\n"
            sent_ids.append(event_id)

        msg += "<i>⚠️ A piac vadul rángathat, óvatosan a nyitott pozíciókkal! A fenti hatás-becslés TÁJÉKOZTATÓ jellegű, nem garantált előrejelzés.</i>"
        # JAVÍTVA: csak sikeres küldés után kerül az event a state-be (különben végleg elveszne)
        if send_telegram_message(msg):
            state["sent_event_ids"].extend(sent_ids)
            print(f"Live makró adat elküldve ({len(recent_events)} esemény).")
        else:
            print("Live makró üzenet küldése sikertelen - a következő futás újrapróbálja.")
    else:
        print("Nem volt új (még ki nem küldött) adatközlés a live-ablakban.")

    _prune_and_save_state(state, now_utc)


def _prune_and_save_state(state: dict, now_utc: datetime) -> None:
    """A STATE_RETENTION_DAYS-nél régebbi event_id-ket és dátumokat
    eldobja, hogy a state fájl ne nőjön korlátlanul. Az event_id-k maguk
    nem tárolnak explicit dátumot, ezért a hozzávetőleges tisztításhoz a
    listát egyszerűen egy ésszerű maximális hosszra vágjuk (egy hétre
    elegendő High Impact esemény jóval e alatt van)."""
    MAX_EVENT_IDS = 200
    if len(state["sent_event_ids"]) > MAX_EVENT_IDS:
        state["sent_event_ids"] = state["sent_event_ids"][-MAX_EVENT_IDS:]

    cutoff_str = (now_utc - timedelta(days=STATE_RETENTION_DAYS)).strftime("%Y-%m-%d")
    state["sent_summary_dates"] = [d for d in state["sent_summary_dates"] if d >= cutoff_str]

    save_state(state)


if __name__ == "__main__":
    main()
