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
            "limit": 80
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
            print(f"[DEBUG] Fetched {len(page_notices)} notices in this page")
            all_notices.extend(page_notices)

            next_token = data.get('iterationNextToken')
            print(f"[DEBUG] Next token: {next_token}")
            if not next_token:
                total = len(all_notices)
                print(f"[DEBUG] No more pages. Total notices collected: {total}")
                return {'notices': all_notices, 'totalNoticeCount': total}

    def fetch_bescha_data(self):
        print("Fetching BeschA data... (dataFetch)")
        try:
            yesterday = datetime.now() - timedelta(days=1)
            pub_day = yesterday.strftime("%Y-%m-%d")
            parsed_url = urlparse(self.bescha_api_url)
            qs = parse_qs(parsed_url.query)
            qs.pop("pubMonth", None)
            qs["pubDay"] = [pub_day]
            qs["format"] = ["ocds.zip"]
            new_query = urlencode(qs, doseq=True)
            new_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                                   parsed_url.params, new_query, parsed_url.fragment))
            print(f"[INFO] Abgerufen wird URL: {new_url}")

            r = requests.get(new_url, timeout=10)
            r.raise_for_status()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            bescha_folder = "/srv/app/ckanext_dataminds/BESCHA"
            os.makedirs(bescha_folder, exist_ok=True)

            tmp_zip = os.path.join(bescha_folder, f"bescha_data_{timestamp}.zip")
            with open(tmp_zip, "wb") as f:
                f.write(r.content)
            print(f"[OK] BeschA-ZIP gespeichert: {tmp_zip}")

            tmp_folder = os.path.join(bescha_folder, f"bescha_unzip_{timestamp}")
            os.makedirs(tmp_folder, exist_ok=True)

            with zipfile.ZipFile(tmp_zip, "r") as zip_ref:
                zip_ref.extractall(tmp_folder)
            print(f"[OK] BeschA-ZIP entpackt in '{tmp_folder}'")

            json_files = []
            for root, dirs, files in os.walk(tmp_folder):
                for file in files:
                    if file.lower().endswith(".json"):
                        json_files.append(os.path.join(root, file))
            os.remove(tmp_zip)
            return json_files, tmp_folder

        except requests.RequestException as e:
            print(f"[FEHLER] BeschA-Request fehlgeschlagen: {e}")
            return [], None
        except zipfile.BadZipFile as e:
            print(f"[FEHLER] Ungültige ZIP-Datei: {e}")
            return [], None

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