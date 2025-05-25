import logging
import os
import shutil
import json
import time
import concurrent.futures
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from contextlib import contextmanager

from . import dataFetch
from . import mongoWriter
from . import CKANPublisher

log = logging.getLogger(__name__)
BASE_DIR = "/srv/app/ckanext_dataminds"

def run_ted_cron_job():
    job_start   = time.time()
    ted_dir     = os.path.join(BASE_DIR, "TED")
    os.makedirs(ted_dir, exist_ok=True)
    counter_file= os.path.join(ted_dir, "ted_job_counter.txt")
    task_num    = _next_counter(counter_file)
    lock_file   = os.path.join(ted_dir, "ted_cron_job.lock")

    def _job():
        """Der komplette Job, den wir im Worker-Thread ausführen."""
        if os.path.exists(lock_file):
            print(f"[Task {task_num}] TED Job already running – warte …")
            while os.path.exists(lock_file):
                time.sleep(1)
        print("------------------------------------------------")
        print(f"[Task {task_num}] Starting job at {datetime.now().isoformat()}")
        with open(lock_file, "w") as f:
            f.write(str(datetime.now()))

        # 1) Fetch Data
        t0 = time.time()
        fetcher = dataFetch.DataFetcher(
            ted_api_url="https://api.ted.europa.eu/v3/notices/search",
            bescha_api_url="https://www.oeffentlichevergabe.de/api/notice-exports"
        )
        ted_data = fetcher.fetch_ted_data()
        print(f"[TIME] fetch_ted_data: {time.time() - t0:.2f}s")
        if not ted_data:
            log.error(f"[Task {task_num}] TED-Data could not be fetched.")
            return

        # 2) Store to JSON
        t1 = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ted_data_{timestamp}.json"
        file_path= os.path.join(ted_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(ted_data, f, indent=2, ensure_ascii=False)
        print(f"[TIME] store_ted_data: {time.time() - t1:.2f}s")

        # 3) Save to Mongo
        t2 = time.time()
        mongo = mongoWriter.MongoWriter(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo"
        )
        mongo.store_ted_data(file_path)
        print(f"[TIME] save_to_mongo: {time.time() - t2:.2f}s")

        # 4) Publish to CKAN
        t3 = time.time()
        publisher = CKANPublisher.CkanPublisher(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo",
            owner_org="publicai")
        publisher.publish_ted_notices(file_path)
        print(f"[TIME] publish_to_ckan: {time.time() - t3:.2f}s")

    # Starte den Job in einem Worker-Thread mit Timeout
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_job)
            # Timeout in Sekunden, z.B. 600 = 10 Minuten
            future.result(timeout=20)
    except concurrent.futures.TimeoutError:
        log.error(f"[Task {task_num}] TED Cron Job timed out after 600s")
        print(f"[Task {task_num}] Aborted: timeout")
    except Exception:
        log.exception(f"[Task {task_num}] TED job failed")
        print(f"[Task {task_num}] Failed")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
        total = time.time() - job_start
        print(f"[Task {task_num}] Done – total time: {total:.2f}s")
        print("------------------------------------------------")

def run_ted_cron_job_for(start_date=None, end_date=None):
    job_start   = time.time()
    ted_dir     = os.path.join(BASE_DIR, "TED")

    if not start_date and not end_date:

        yesterday = datetime.now() - timedelta(days=1)
        start = end = yesterday.strftime("%Y%m%d")
    else:
      # aus Admin-Panel übergeben
        start = start_date.replace('-', '')
        end = end_date.replace('-', '')
    print(f"[INFO] Fetching TED notices from {start} to {end}  (format YYYYMMDD)")

    date_query = f"(publication-date>={start} AND publication-date<={end})"

    os.makedirs(ted_dir, exist_ok=True)
    counter_file= os.path.join(ted_dir, "ted_job_counter.txt")
    task_num    = _next_counter(counter_file)
    lock_file   = os.path.join(ted_dir, "ted_cron_job.lock")

    def _job():
        """Der komplette Job, den wir im Worker-Thread ausführen."""
        if os.path.exists(lock_file):
            print(f"[Task {task_num}] TED Job already running – warte …")
            while os.path.exists(lock_file):
                time.sleep(1)
        print("------------------------------------------------")
        print(f"[Task {task_num}] Starting job at {datetime.now().isoformat()}")
        with open(lock_file, "w") as f:
            f.write(str(datetime.now()))

        # 1) Fetch Data
        t0 = time.time()
        fetcher = dataFetch.DataFetcher()
        fetcher.current_payload['query'] = date_query
        ted_data = fetcher.fetch_ted_data()
        print(f"[TIME] fetch_ted_data: {time.time() - t0:.2f}s")
        if not ted_data:
            log.error(f"[Task {task_num}] TED-Data could not be fetched.")
            return

        # 2) Store to JSON
        t1 = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ted_data_{timestamp}.json"
        file_path= os.path.join(ted_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(ted_data, f, indent=2, ensure_ascii=False)
        print(f"[TIME] store_ted_data: {time.time() - t1:.2f}s")

        # 3) Save to MongoDB
        t2 = time.time()
        mongo = mongoWriter.MongoWriter(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo"
        )
        mongo.store_ted_data(file_path)
        print(f"[TIME] save_to_mongo: {time.time() - t2:.2f}s")

        # 4) Publish to CKAN
        t3 = time.time()
        publisher = CKANPublisher.CkanPublisher(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo",
            owner_org="publicai")
        publisher.publish_ted_notices(file_path)
        print(f"[TIME] publish_to_ckan: {time.time() - t3:.2f}s")

    # Starte den Job in einem Worker-Thread mit Timeout
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_job)
            # Timeout in Sekunden, z.B. 600 = 10 Minuten
            future.result(timeout=20)
    except concurrent.futures.TimeoutError:
        log.error(f"[Task {task_num}] TED Cron Job timed out after 600s")
        print(f"[Task {task_num}] Aborted: timeout")
    except Exception:
        log.exception(f"[Task {task_num}] TED job failed")
        print(f"[Task {task_num}] Failed")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
        total = time.time() - job_start
        print(f"[Task {task_num}] Done – total time: {total:.2f}s")
        print("------------------------------------------------")

def run_bescha_cron_job():
    bescha_dir = os.path.join(BASE_DIR, "BESCHA")
    os.makedirs(bescha_dir, exist_ok=True)
    lock_file = os.path.join(bescha_dir, "bescha_cron_job.lock")
    #if os.path.exists(lock_file):
        #print("BeschA Cron Job is already running.")
        #return
    try:
        with open(lock_file, "w") as f:
            f.write(str(datetime.now()))
        print("STARTE BeschA CRON JOB -----------------------------------------------------------------------------------------------")
        log.info("Starte BeschA Cron-Job...")

        yesterday = datetime.now() - timedelta(days=1)
        pub_day = yesterday.strftime("%Y-%m-%d")
        parsed_url = urlparse("https://www.oeffentlichevergabe.de/api/notice-exports?format=ocds.zip")
        qs = parse_qs(parsed_url.query)
        qs["pubDay"] = [pub_day]
        new_query = urlencode(qs, doseq=True)
        bescha_api_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                                     parsed_url.params, new_query, parsed_url.fragment))
        print(f"[INFO] Abgerufen wird URL: {bescha_api_url}")

        already_processed = False
        for folder in os.listdir(bescha_dir):
            if pub_day in folder:
                print(f"Zeitraum {pub_day} wurde bereits verarbeitet (Ordner: {folder}).")
                log.info(f"Zeitraum {pub_day} wurde bereits verarbeitet (Ordner: {folder}).")
                already_processed = True
                break
        if already_processed:
            print("Überspringe Abruf, da Zeitraum bereits vorhanden ist.")
            log.info("Überspringe Abruf, da Zeitraum bereits vorhanden ist.")
            return

        fetcher = dataFetch.DataFetcher(
            ted_api_url="https://api.ted.europa.eu/v3/notices/search",
            bescha_api_url=bescha_api_url
        )
        json_files, subfolder_path = fetcher.fetch_bescha_data()
        if json_files:
            print(f"BeschA-Daten wurden in {subfolder_path} entpackt.")
            log.info(f"BeschA-Daten wurden in {subfolder_path} entpackt.")

            mongo = mongoWriter.MongoWriter(mongo_uri="mongodb://mongodb:27017/", db_name="ckan_mongo")
            mongo.store_bescha_data(json_files)

            ckan = CKANPublisher.CkanPublisher(
                mongo_uri="mongodb://localhost:27017/",
                db_name="ckan_mongo",
                ckan_url="http://localhost:5000",
                ckan_api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJiOFBXUl9ibG1waVVZU0YwNnVCeXFsZFJoZFN2bExKUUwwN2RjQTVnYnBNIiwiaWF0IjoxNzQyMDkyODQ4fQ.8WrBY_A5pHRaqChdgTRYTwPHKgo3e9jpYSeYUO6DCD8"
            )
            ckan.publish_bescha_dataset(dataset_name="bescha-dataset", dataset_title="BeschA Dataset")

            if subfolder_path and os.path.exists(subfolder_path):
                shutil.rmtree(subfolder_path, ignore_errors=True)
        else:
            log.error("BeschA-Daten konnten nicht abgerufen werden.")
    except Exception as e:
        log.error(f"Fehler im BeschA Cron Job: {e}")
    finally:
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
                print("Lock-Datei für BeschA Cron Job gelöscht.")
                log.info("Lock-Datei für BeschA Cron Job gelöscht.")
        except Exception as e:
            log.error(f"Fehler beim Löschen der Lock-Datei {lock_file}: {e}")

def _next_counter(path):
    """Liefert die nächste Zahl und schreibt sie zurück in path."""
    if os.path.exists(path):
        with open(path, "r") as f:
            try:
                n = int(f.read().strip())
            except ValueError:
                n = 0
    else:
        n = 0
    n += 1
    with open(path, "w") as f:
        f.write(str(n))
    return n

class TimeoutException(Exception):
    pass
