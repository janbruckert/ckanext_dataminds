from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import os
import zipfile
import requests
import time
import threading
import json

class DataFetcher:
    """
    Holt Daten von TED (POST) und BeschA (ZIP) und passt sich adaptiv an
    Änderungen der API-Spezifikation an.
    """
    def __init__(self,
                 ted_api_url="https://api.ted.europa.eu/v3/notices/search",
                 bescha_api_url="https://www.oeffentlichevergabe.de/api/notice-exports?format=ocds.zip"):
        self.ted_api_url = ted_api_url
        self.bescha_api_url = bescha_api_url
        self.api_version = None
        self.current_payload = {
            "query": "(title-proc='technology')",
            "fields": ["title-proc", "buyer-name", "publication-date", "publication-number"],
            "limit": 100
        }
        self.monitor_thread = threading.Thread(target=self.monitor_api_spec, daemon=True)
        self.monitor_thread.start()

    def fetch_ted_data(self):
        all_notices = []
        next_token = None
        max_retries = 3
        print(f"[DEBUG] Starting fetch_ted_data with initial payload: {self.current_payload}")
        attempt_counter = 0
        while True:
            payload = dict(self.current_payload)
            if next_token:
                payload['nextToken'] = next_token
            print(f"[DEBUG] Sending request with payload: {payload}")

            for attempt in range(1, max_retries + 1):
                attempt_counter += 1
                try:
                    print(f"[DEBUG] Attempt {attempt} of {max_retries} (overall try #{attempt_counter})")
                    r = requests.post(
                        self.ted_api_url,
                        headers={"Content-Type": "application/json"},
                        json=payload,
                        timeout=10
                    )
                    print(f"[DEBUG] Received response: status_code={r.status_code}")
                    r.raise_for_status()
                    data = r.json()
                    print(f"[DEBUG] Response JSON keys: {list(data.keys())}")
                    break
                except requests.RequestException as e:
                    print(f"[ERROR] TED-Request failed (Try {attempt}/{max_retries}): {e}")
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        print(f"[DEBUG] Waiting {wait}s before retry")
                        time.sleep(wait)
                    else:
                        print("[ERROR] Max retries reached, aborting fetch_ted_data.")
                        return None

            page_notices = data.get('notices', [])
            all_notices.extend(page_notices)

            next_token = data.get('iterationNextToken')
            if not next_token:
                total = len(all_notices)
                print(f"[DEBUG] No more pages. Total notices collected: {total}")
                return {'notices': all_notices, 'totalNoticeCount': total}

    def fetch_bescha_data(self, ):
        """
        Holt die tägliche BESCHA-OCIDS-ZIP, entpackt sie und sammelt alle 'releases'
        aus den JSON-Dateien. Liefert ein Dict im TED-ähnlichen Format:
        {'notices': [...], 'totalNoticeCount': n}
        """
        max_retries = 3
        all_releases = []
        if pub_day is None:
            dt = datetime.now() - timedelta(days=1)
        elif isinstance(pub_day, str):
            dt = datetime.strptime(pub_day, "%Y-%m-%d")
        else:
            dt = pub_day
        pub_day_str = dt.strftime("%Y-%m-%d")

        # URL mit pubDay-Parameter bauen
        parsed = urlparse(self.bescha_api_url)
        qs = parse_qs(parsed.query)
        qs['pubDay'] = [pub_day]
        qs['format'] = ['ocds.zip']
        new_query = urlencode(qs, doseq=True)
        fetch_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path,
                                parsed.params, new_query, parsed.fragment))
        print(f"[DEBUG] Starting fetch_bescha_data for pubDay={pub_day}")
        print(f"[DEBUG] Fetch URL: {fetch_url}")

        # ZIP-Download mit Retries
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[DEBUG] Attempt {attempt}/{max_retries} to download BESCHA-ZIP")
                r = requests.get(fetch_url, timeout=10)
                print(f"[DEBUG] Received status_code={r.status_code}")
                r.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"[ERROR] BESCHA-Request failed (Try {attempt}): {e}")
                if attempt < max_retries:
                    wait = 2 ** attempt
                    print(f"[DEBUG] Waiting {wait}s before retry")
                    time.sleep(wait)
                else:
                    print("[ERROR] Max retries reached, aborting fetch_bescha_data.")
                    return None

        # In temporäres Verzeichnis speichern und entpacken
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = "/srv/app/ckanext_dataminds/BESCHA"
        os.makedirs(base, exist_ok=True)
        zip_path = os.path.join(base, f"bescha_{pub_day}_{timestamp}.zip")
        with open(zip_path, "wb") as f:
            f.write(r.content)
        print(f"[OK] BeschA-ZIP gespeichert: {zip_path}")

        tmp_dir = os.path.join(base, f"unzipped_{pub_day}_{timestamp}")
        os.makedirs(tmp_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(tmp_dir)
        print(f"[OK] BeschA-ZIP entpackt nach: {tmp_dir}")

        # JSON-Dateien einlesen und 'releases' sammeln
        for root, _, files in os.walk(tmp_dir):
            for fn in files:
                if not fn.lower().endswith(".json"):
                    continue
                fp = os.path.join(root, fn)
                try:
                    with open(fp, 'r', encoding='utf-8') as jf:
                        data = json.load(jf)
                    releases = data.get('releases', [])
                    if isinstance(releases, list):
                        all_releases.extend(releases)
                    else:
                        print(f"[WARN] {fn}: 'releases' ist kein Array")
                except Exception as e:
                    print(f"[WARN] Fehler beim Parsen von {fn}: {e}")

        # Aufräumen
        os.remove(zip_path)
        # optional: shutil.rmtree(tmp_dir)

        total = len(all_releases)
        print(f"[DEBUG] Total BESCHA releases collected: {total}")
        return {'notices': all_releases, 'totalNoticeCount': total}

    def monitor_api_spec(self):
        while True:
            try:
                response = requests.get(self.ted_api_url, timeout=5)
                if response.status_code == 405:
                    print("[WARN] GET-Methode nicht erlaubt für den TED-API-Endpunkt. Überspringe API-Spezifikationscheck.")
                elif response.ok:
                    data = response.json()
                    new_version = data.get("api_version")
                    if new_version and new_version != self.api_version:
                        print(f"[INFO] API-Version hat sich geändert: {self.api_version} -> {new_version}")
                        self.api_version = new_version
                        self.adapt_api()
                else:
                    print(f"[WARN] API-Spezifikation nicht erreichbar (Status {response.status_code})")
            except Exception as e:
                print(f"[WARN] Fehler beim Überwachen der API-Spezifikation: {e}")
            time.sleep(60)

    def adapt_api(self):
        print("[INFO] Adaptive Maßnahme wird durchgeführt. Prüfe aktuelle API-Version und passe Parameter an.")
        if self.api_version == "2.0":
            self.ted_api_url = "https://api.ted.europa.eu/v3/notices/search/v2"
            self.current_payload = {
                "query": "(title='technology')",
                "fields": ["title", "purchaser", "pub_date", "publication-number"],
                "limit": 5
            }
            print("[INFO] API-Version 2.0 erkannt: URL und Payload angepasst.")
        else:
            self.ted_api_url = "https://api.ted.europa.eu/v3/notices/search"
            self.current_payload = {
                "query": "(title-proc='technology')",
                "fields": ["title-proc", "buyer-name", "publication-date", "publication-number"],
                "limit": 5
            }
            print("[INFO] API-Version unbekannt oder Standard: URL und Payload zurückgesetzt.")
