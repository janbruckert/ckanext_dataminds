import json
import io
import logging
from datetime import datetime

from pymongo import MongoClient
import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)

class CkanPublisher:
    """
    Veröffentlichung einzelner TED-Notices als separate Datasets in CKAN.
    """

    # Context für Toolkit-Aktionen ohne Authentifizierung
    context = {'ignore_auth': True}

    def __init__(self, mongo_uri, db_name, owner_org):
        # MongoDB-Verbindung
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        # Org, unter der die Datasets angelegt werden
        self.owner_org = owner_org
        print(f"CKAN Publisher ready (DB {db_name}, owner_org={owner_org})")

    def _get_or_create_package(self, name, title, description, tags=None, extras=None):
        """
        Legt ein neues CKAN-Paket an oder lädt es, wenn es bereits existiert.
        Jetzt mit owner_org und aussagekräftigen Logs.
        """
        existing = tk.get_action('package_list')(self.context, {})
        data = {
            'name': name,
            'title': title,
            'notes': description,
            'owner_org': self.owner_org,
            'tags': [{'name': t} for t in (tags or [])],
            'private': False
        }
        if extras:
            data['extras'] = [{'key': k, 'value': str(v)} for k, v in extras.items()]

        if name in existing:
            pkg = tk.get_action('package_show')(self.context, {'id': name})
            print(f"Package exists: {name}")
        else:
            pkg = tk.get_action('package_create')(self.context, data)
            print(f"Created package: {name}")
        return pkg

    def _publish_ted_notice(self, notice):
        """
        Baut aus einer Notice das Dataset plus Resource.
        """
        pubnum = notice.get('publication-number', 'unknown')
        dataset_name = f"ted-{pubnum}"

        title_map = notice.get('title-proc', {})
        print(title_map)
        if 'eng' not in title_map:
            print(f"WARN: Notice {pubnum} has no title. Skipping...")
            return
        title_eng = None
        for code in ('eng', 'en'):
            if code in title_map:
                title_eng = title_map[code].strip()
                break
        if not title_eng:
            print(f"WARN: Notice {pubnum} hat keinen englischen Titel unter 'eng' oder 'en'. Überspringe…")
            return
        raw_date = notice.get('publication-date', '').rstrip('Z')
        buyer_map = notice.get('buyer-name') or {}
        buyer_list = None
        for code in ('eng', 'en'):
            if code in buyer_map:
                buyer_list = buyer_map[code]
                break
            if not buyer_list:
                print(f"WARN: Notice {pubnum} hat keinen englischen Käufernamen unter 'eng' oder 'en'. Überspringe…")
                return
        buyer = ", ".join(buyer_list)
        # Links aufbereiten
        links_md = ""
        for ltype, langs in notice.get('links', {}).items():
            for lang, url in langs.items():
                links_md += f"- **{ltype}/{lang}**: {url}\n"

        # Beschreibung mit Markdown-Absätzen
        description = (
            f"**Notice Number:** {pubnum}\n\n"
            f"**Buyer Name (eng):** {buyer}\n\n"
            f"**Publication Date:** {raw_date}\n\n"
             f"**Links:**\n\n{links_md}".strip()
        )

        date_only = raw_date.split('T', 1)[0].split('+', 1)[0]
        tags = ['TED', pubnum, date_only]
        extras = {
            'publication_number': pubnum,
            'buyer_name': buyer,
            'publication_date': raw_date
        }

        # Paket anlegen oder holen
        pkg = self._get_or_create_package(
            name=dataset_name,
            title=title_eng,
            description=description,
            tags=tags,
            extras=extras
        )
        pkg_id = pkg['id']

        # JSON-Resource erstellen/aktualisieren
        notice_json = json.dumps(notice, ensure_ascii=False, indent=2)
        fp = io.BytesIO(notice_json.encode('utf-8'))
        fp.name = f"ted_{pubnum}.json"
        res_args = {
            'package_id': pkg_id,
            'name': fp.name,
            'upload': fp,
            'format': 'json',
            'title': title_eng
        }
        # prüfen, ob Resource schon da ist
        existing = next((r for r in pkg.get('resources', []) if r['name'] == fp.name), None)
        if existing:
            print(f"Resource {fp.name} already uploaded ({dataset_name}), skipping...")
            return

        tk.get_action('resource_create')(self.context, res_args)
        print(f"Created resource for {dataset_name}/{title_eng}: {fp.name}")    

    def publish_ted_notices(self, file_path):
        """
        Liest eine TED-JSON-Datei ein, zerlegt sie in Notices und legt
        je ein CKAN-Dataset pro Notice an.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        notices = data.get('notices', [])
        print(f"Found {len(notices)} notices in {file_path}")

        for notice in notices:
            pubnum = notice.get('publication-number', 'unknown')
            try:
                self._publish_ted_notice(notice)
            except Exception as e:
                print(f"Fehler bei Notice {pubnum}: {e}")

