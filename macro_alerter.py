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

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# --- BEÁLLÍTÁSOK ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FF_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Csak ezek a kulcsszavak érdekelnek minket a High Impact események közül (angolul)
TARGET_EVENTS = ["CPI", "PPI", "FOMC", "Non-Farm", "Fed", "Interest Rate", "GDP"]

STATE_FILE = Path(__file__).parent / "macro_state.json"

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
                return json.load(f)
        except (json.JSONDecodeError, OSError):
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


def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("HIBA: Hiányzó Telegram hitelesítő adatok!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code != 200:
            print(f"Telegram hiba ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"Telegram küldési hiba: {e}")


def get_macro_data():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(FF_JSON_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Hiba az adatok lekérésekor: {e}")
        return []


def main():
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    state = load_state()
    state.setdefault("sent_event_ids", [])
    state.setdefault("sent_summary_dates", [])

    events = get_macro_data()

    today_events = []
    recent_events = []

    for event in events:
        # Csak a magas prioritású USD események érdekelnek minket
        if event.get("country") != "USD" or event.get("impact") != "High":
            continue

        # Címke szűrés (Hogy ne kapjunk értesítést minden apróságról)
        title = event.get("title", "")
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
                msg += f"⏰ {time_str} - <b>{ev['title']}</b>\n"
            send_telegram_message(msg)
            print("Reggeli összefoglaló elküldve.")
        else:
            print("Ma nincs High Impact USD esemény.")
        state["sent_summary_dates"].append(today_str)
        _prune_and_save_state(state, now_utc)
        return  # Reggel nem csinálunk mást

    # --- 2. MÓD: LIVE ADATKÖZLÉS (Napközbeni futások) ---
    if recent_events:
        msg = "🚨 <b>MAKRÓ ADAT MEGJELENT!</b> 🚨\n\n"
        for event_id, ev in recent_events:
            title = ev.get("title", "Ismeretlen")
            actual = ev.get("actual", "") or "N/A"
            forecast = ev.get("forecast", "") or "N/A"
            previous = ev.get("previous", "") or "N/A"

            msg += f"📌 <b>{title}</b>\n"
            msg += f"Tény: <b>{actual}</b>\n"
            msg += f"Várt: {forecast} | Előző: {previous}\n\n"
            state["sent_event_ids"].append(event_id)

        msg += "<i>⚠️ A piac vadul rángathat, óvatosan a nyitott pozíciókkal!</i>"
        send_telegram_message(msg)
        print(f"Live makró adat elküldve ({len(recent_events)} esemény).")
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
