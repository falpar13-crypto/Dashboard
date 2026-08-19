"""
Makró Figyelmeztető Bot (macro_alerter.py)
====================================================================
Ez a script a ForexFactory ingyenes JSON végpontját használja.
Két üzemmódja van (az aktuális futtatási időpont alapján dönti el):
1. Reggeli mód (UTC 06:00 körül): Kilistázza a mai napi High Impact USD eseményeket.
2. Live mód: Megnézi, hogy az elmúlt 15 percben volt-e fontos adatközlés, és elküldi az eredményeket.
Automatikusan kezeli a téli/nyári időszámítást!
"""

import os
import requests
from datetime import datetime, timedelta, timezone

# --- BEÁLLÍTÁSOK ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FF_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Csak ezek a kulcsszavak érdekelnek minket a High Impact események közül (angolul)
TARGET_EVENTS = ["CPI", "PPI", "FOMC", "Non-Farm", "Fed", "Interest Rate", "GDP"]

def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("HIBA: Hiányzó Telegram hitelesítő adatok!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
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
            
        # Ha az elmúlt 15 percben történt az adatközlés (LIVE mód)
        time_diff = (now_utc - event_time).total_seconds() / 60
        if 0 <= time_diff <= 15:
            recent_events.append(event)

    # --- 1. MÓD: REGGELI ÖSSZEFOGLALÓ (Ha UTC 05:00 és 07:00 között futunk) ---
    if 5 <= now_utc.hour <= 7:
        if today_events:
            msg = "📅 <b>NAPI MAKRÓ FIGYELMEZTETÉS</b>\n<i>Extrém volatilitás várható ma!</i>\n\n"
            for etime, ev in sorted(today_events, key=lambda x: x[0]):
                # Időpont formázása a chathez (UTC időpontot kiírva)
                time_str = etime.strftime("%H:%M (UTC)")
                msg += f"⏰ {time_str} - <b>{ev['title']}</b>\n"
            send_telegram_message(msg)
            print("Reggeli összefoglaló elküldve.")
        else:
            print("Ma nincs High Impact USD esemény.")
        return # Reggel nem csinálunk mást

    # --- 2. MÓD: LIVE ADATKÖZLÉS (Napközbeni futások) ---
    if recent_events:
        msg = "🚨 <b>MAKRÓ ADAT MEGJELENT!</b> 🚨\n\n"
        for ev in recent_events:
            title = ev.get("title", "Ismeretlen")
            actual = ev.get("actual", "") or "N/A"
            forecast = ev.get("forecast", "") or "N/A"
            previous = ev.get("previous", "") or "N/A"
            
            msg += f"📌 <b>{title}</b>\n"
            msg += f"Tény: <b>{actual}</b>\n"
            msg += f"Várt: {forecast} | Előző: {previous}\n\n"
            
        msg += "<i>⚠️ A piac vadul rángathat, óvatosan a nyitott pozíciókkal!</i>"
        send_telegram_message(msg)
        print("Live makró adat elküldve.")
    else:
        print("Nem volt adatközlés az elmúlt 15 percben.")

if __name__ == "__main__":
    main()
