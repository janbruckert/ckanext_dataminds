import json
import logging
import os
import shutil
import zipfile
from pymongo import MongoClient

log = logging.getLogger(__name__)

class MongoWriter:
    """
    Schreibt Datensätze in MongoDB.
    Nutzt standardmäßig die DB 'ckan_mongo' und Collections 'ted_data' bzw. 'bescha_data'.
    """
    def __init__(self, mongo_uri="mongodb://mongodb:27017/", db_name="ckan_mongo"):
        self.mongo_uri = mongo_uri
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            db_names = self.client.list_database_names()
        except Exception as e:
            print("Error connecting to MongoDB:", e)
        self.db = self.client[db_name]

    def store_ted_data(self, ted_json_path):
        """
        Speichert TED-Daten aus einer JSON-Datei in die MongoDB-Collection 'ted_data'.
        Vor dem Einfügen wird anhand des Dateinamens geprüft, ob diese Datei bereits hochgeladen wurde.
        """
        try:
            filename = os.path.basename(ted_json_path)
            existing = self.db["ted_data"].find_one({"source_file": filename})
            if existing:
                print(f"[INFO] File {filename} has already been uploaded. Skipping...")
                return

            with open(ted_json_path, 'r', encoding='utf-8') as f:
                ted_data = json.load(f)
            if "notices" not in ted_data or not ted_data["notices"]:
                print("[WARN] No 'notices' found.")
                return
            for notice in ted_data["notices"]:
                notice["source_file"] = filename
            result = self.db["ted_data"].insert_many(ted_data["notices"])
            print(f"[OK] {len(result.inserted_ids)} TED-Documents from {filename} successfully uploaded.")
        except FileNotFoundError:
            print(f"[FEHLER] File not Found: {ted_json_path}")
        except json.JSONDecodeError as e:
            print(f"[FEHLER] JSON-Decoding Error: {e}")


    def store_bescha_data(self, zip_file_paths):
        """
        Speichert JSON-Daten aus entpackten BeschA-ZIP-Dateien in die MongoDB-Collection 'bescha_data'.
        Für jeden ZIP-Dateipfad wird anhand des Dateinamens geprüft, ob er bereits verarbeitet wurde.
        """
        if not zip_file_paths or not isinstance(zip_file_paths, list):
            print("[WARN] No BeschA-ZIP-Data Paths available.")
            return

        docs_to_insert = []
        for zip_path in zip_file_paths:
            try:
                zip_filename = os.path.basename(zip_path)
                existing = self.db["bescha_data"].find_one({"source_file": zip_filename})
                if existing:
                    print(f"[INFO] Datei {zip_filename} wurde bereits hochgeladen. Überspringe Verarbeitung.")
                    continue

                tmp_folder = zip_path.replace(".zip", "_unzipped")
                os.makedirs(tmp_folder, exist_ok=True)

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmp_folder)

                print(f"[OK] ZIP-Datei entpackt: {tmp_folder}")

                for root, _, files in os.walk(tmp_folder):
                    for file in files:
                        if file.lower().endswith(".json"):
                            json_path = os.path.join(root, file)
                            try:
                                with open(json_path, "r", encoding="utf-8") as f:
                                    data = json.load(f)
                                    if isinstance(data, list):
                                        for doc in data:
                                            doc["source_file"] = zip_filename
                                        docs_to_insert.extend(data)
                                    else:
                                        data["source_file"] = zip_filename
                                        docs_to_insert.append(data)
                            except json.JSONDecodeError as e:
                                print(f"[WARN] JSON-Dekodierungsfehler in Datei {file}: {e}")
                            except Exception as e:
                                print(f"[WARN] Fehler beim Lesen der Datei {file}: {e}")

                shutil.rmtree(tmp_folder, ignore_errors=True)
            except zipfile.BadZipFile as e:
                print(f"[FEHLER] Ungültige ZIP-Datei: {zip_path}, Fehler: {e}")
            except Exception as e:
                print(f"[FEHLER] Fehler beim Verarbeiten der ZIP-Datei {zip_path}: {e}")

        if docs_to_insert:
            try:
                result = self.db["bescha_data"].insert_many(docs_to_insert)
                print(f"[OK] {len(result.inserted_ids)} BeschA-Dokumente erfolgreich eingefügt.")
            except Exception as e:
                print(f"[FEHLER] Fehler beim Einfügen in MongoDB: {e}")
        else:
            print("[WARN] Keine gültigen Dokumente gefunden.")
