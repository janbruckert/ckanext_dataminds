import logging
import os
import shutil
import json
import time
import csv
import concurrent.futures
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from contextlib import contextmanager

from . import dataFetch, DataFetcher
from . import mongoWriter
from . import CKANPublisher

log = logging.getLogger(__name__)
BASE_DIR = "/srv/app/ckanext_dataminds"
TIMINGS_CSV = os.path.join(BASE_DIR, "timings.csv")

def record_timing(task_num, phase, duration_s):
    """Schreibt eine Zeile (task_num, phase, duration_s, timestamp) in TIMINGS_CSV."""
    is_new = not os.path.exists(TIMINGS_CSV)
    with open(TIMINGS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(["task_num", "phase", "duration_s"])
        writer.writerow([task_num, phase, f"{duration_s:.2f}"])

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
            print(f"[Task {task_num}] TED Job already running – waiting...")
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
            future.result(timeout=600)
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
    print(f"[DEBUG] start_date from frontend: {start_date}, end_date from frontend: {end_date}")
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

        duration = time.time() - t0
        print(f"[TIME] fetch_ted_data: {duration:.2f}s")
        record_timing(task_num, "fetch_ted", duration)

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
        duration = time.time() - t1
        print(f"[TIME] store_ted_data: {duration:.2f}s")
        record_timing(task_num, "store_ted_data", duration)

        # 3) Save to MongoDB
        t2 = time.time()
        mongo = mongoWriter.MongoWriter(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo"
        )
        mongo.store_ted_data(file_path)
        duration = time.time() - t2
        print(f"[TIME] save_to_mongo: {duration:.2f}s")
        record_timing(task_num, "save_to_mongo", duration)

        # 4) Publish to CKAN
        t3 = time.time()
        publisher = CKANPublisher.CkanPublisher(
            mongo_uri="mongodb://mongodb:27017/",
            db_name="ckan_mongo",
            owner_org="publicai")
        publisher.publish_ted_notices(file_path)
        duration = time.time() - t3
        print(f"[TIME] publish_to_ckan: {duration:.2f}s")
        record_timing(task_num, "publish_to_ckan", duration)

    # Starte den Job in einem Worker-Thread mit Timeout
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_job)
            future.result(timeout=600)
    except concurrent.futures.TimeoutError:
        log.error(f"[Task {task_num}] TED Cron Job timed out after 600s")
        print(f"[Task {task_num}] Aborted: timeout")
    except Exception:
        log.exception(f"[Task {task_num}] TED job failed")
        print(f"[Task {task_num}] Failed")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
        total_duration = time.time() - job_start
        record_timing(task_num, "total_job_time", total_duration)
        print(f"[Task {task_num}] Done – total time: {total_duration:.2f}s")
        print("------------------------------------------------")



def run_bescha_cron_job_for(start_date=None, end_date=None):
    """
    Holt für den angegebenen Datumsbereich (YYYY-MM-DD) die BESCHA-OCIDS-ZIPs,
    entpackt, speichert sie und publisht sie in CKAN.
    """
    job_start = time.time()
    bescha_dir = os.path.join(BASE_DIR, "BESCHA")
    os.makedirs(bescha_dir, exist_ok=True)
    counter_file = os.path.join(bescha_dir, "bescha_job_counter.txt")
    task_num = _next_counter(counter_file)
    lock_file = os.path.join(bescha_dir, "bescha_cron_job.lock")

    # 1. Datum bestimmen
    if not start_date or not end_date:
        yesterday = datetime.now() - timedelta(days=1)
        dates = [yesterday]
    else:
        # strings "YYYY-MM-DD" in datetime-Objekte und dann Liste aller Tage
        sd = datetime.strptime(start_date, "%Y-%m-%d")
        ed = datetime.strptime(end_date,   "%Y-%m-%d")
        num_days = (ed - sd).days
        dates = [sd + timedelta(days=i) for i in range(num_days + 1)]

    print(f"[DEBUG] BESCHA run_for dates: {[d.strftime('%Y-%m-%d') for d in dates]}")

    def _job():
        if os.path.exists(lock_file):
            print(f"[Task {task_num}] BESCHA Job already running – waiting…")
            while os.path.exists(lock_file):
                time.sleep(1)
        with open(lock_file, "w") as f:
            f.write(datetime.now().isoformat())
        print("------------------------------------------------")
        print(f"[Task {task_num}] Starting BESCHA job at {datetime.now().isoformat()}")

        for d in dates:
            pub_day = d.strftime("%Y-%m-%d")
            print(f"[INFO] Fetching BESCHA for pubDay={pub_day}")

            # Download & Unzip
            t0 = time.time()
            notices_dict = dataFetch.DataFetcher().fetch_bescha_data()
            duration = time.time() - t0
            print(f"[TIME] fetch_bescha_data ({pub_day}): {duration:.2f}s")
            record_timing(task_num, f"fetch_bescha_{pub_day}", duration)


            # Mongo speichern
            t1 = time.time()
            mongoWriter.MongoWriter().store_bescha_data(notices_dict)
            duration = time.time() - t1
            print(f"[TIME] save_bescha_to_mongo ({pub_day}): {duration:.2f}s")
            record_timing(task_num, f"save_bescha_to_mongo_{pub_day}", duration)

            # CKAN publizieren
            t2 = time.time()
            publisher = CKANPublisher.CkanPublisher(
                mongo_uri="mongodb://mongodb:27017/",
                db_name="ckan_mongo",
                owner_org="publicai")
            publisher.publish_bescha_notices(notices_dict)
            duration = time.time() - t2
            print(f"[TIME] publish_bescha ({pub_day}): {duration:.2f}s")
            record_timing(task_num, f"publish_bescha_{pub_day}", duration)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_job)
            future.result(timeout=600)
    except concurrent.futures.TimeoutError:
        log.error(f"[Task {task_num}] BESCHA Cron Job timed out after 600s")
        print(f"[Task {task_num}] Aborted: timeout")
    except Exception:
        log.exception(f"[Task {task_num}] BESCHA job failed")
        print(f"[Task {task_num}] Failed")
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
        total_duration = time.time() - job_start
        record_timing(task_num, "total_job_time", total_duration)
        print(f"[Task {task_num}] Done – total time: {total_duration:.2f}s")
        print("------------------------------------------------")


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
