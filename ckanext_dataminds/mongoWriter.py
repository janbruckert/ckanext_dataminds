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


    def store_bescha_data(self, zip_paths):
        """
        Speichert JSON-Daten aus entpackten BeschA-ZIP-Dateien in die MongoDB-Collection 'bescha_data'.
        Für jeden ZIP-Dateipfad wird anhand des Dateinamens geprüft, ob er bereits verarbeitet wurde.
        """
        if isinstance(zip_paths, str):
            zip_paths = [zip_paths]

        docs_to_insert = []
        for zip_path in zip_paths:
            source_file = os.path.basename(zip_path)

            # Überspringen, falls schon importiert
            if self.db["bescha_data"].find_one({"source_file": source_file}):
                print(f"[INFO] {source_file} bereits verarbeitet, skip.")
                continue

            print(f"[INFO] Verarbeite ZIP: {source_file}…")

            # ZIP entpacken und JSON-Dateien parsen
            try:
                with zipfile.ZipFile(zip_path, "r") as archive:
                    for member in archive.namelist():
                        if not member.lower().endswith(".json"):
                            continue
                        with archive.open(member) as f:
                            try:
                                data = json.load(f)
                            except json.JSONDecodeError as e:
                                print(f"[WARN] JSON-Fehler in {member}: {e}")
                                continue

                        # Unter 'releases' oder 'notices' abholen
                        releases = data.get("releases") or data.get("notices") or []
                        if isinstance(releases, dict):
                            releases = [releases]

                        for rel in releases:
                            rel["source_file"] = source_file
                            docs_to_insert.append(rel)

            except zipfile.BadZipFile as e:
                print(f"[FEHLER] Ungültige ZIP-Datei {zip_path}: {e}")
            except Exception as e:
                print(f"[FEHLER] Fehler beim Verarbeiten von {zip_path}: {e}")

        # Bulk-Insert in MongoDB
        if not docs_to_insert:
            print("[WARN] Keine BESCHA-Dokumente gefunden zum Einfügen.")
            return

        try:
            result = self.db["bescha_data"].insert_many(docs_to_insert)
            print(f"[OK] {len(result.inserted_ids)} BESCHA-Dokumente eingefügt (source_file zuletzt: {source_file}).")
        except Exception as e:
            print(f"[FEHLER] Beim Einfügen in MongoDB ist ein Fehler aufgetreten: {e}")
