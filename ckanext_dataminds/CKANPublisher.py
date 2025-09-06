import json
import io
import re
import logging
from datetime import datetime

from pymongo import MongoClient
import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)


def clean_tag(input_tag):
    tag = re.sub(r'[^a-zA-Z0-9 \-_.]', '', input_tag)
    return tag[:63].strip()


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
        else:
            pkg = tk.get_action('package_create')(self.context, data)
        return pkg

    def _publish_ted_notice(self, notice):
        """
        Baut aus einer Notice das Dataset plus Resource.
        """
        pubnum = notice.get('publication-number', 'unknown')
        dataset_name = f"ted-{pubnum}"

        title_map = notice.get('title-proc', {})
        title_text = None
        lang_code = None
        title_eng = None

        for code in ('eng', 'deu'):
            if code in title_map:
                title_text = title_map[code].strip()
                lang_code = code
                break
        if not title_text:
            return False
        raw_date = notice.get('publication-date', '').rstrip('Z')
        buyer_map = notice.get('buyer-name') or {}
        buyer_list = None
        for code in ('eng', 'deu'):
            if code in buyer_map:
                buyer_list = buyer_map[code]
                break
        if not buyer_list:
            return False
        buyer = ", ".join(buyer_list)
        # Links aufbereiten
        links_md = ""
        for ltype, langs in notice.get('links', {}).items():
            for lang, url in langs.items():
                links_md += f"- **{ltype}/{lang}**: {url}\n"

        # Beschreibung mit Markdown-Absätzen
        description = (
            f"**Notice Number:** {pubnum}\n\n"
            f"**Buyer Name:** {buyer}\n\n"
            f"**Publication Date:** {raw_date}\n\n"
             f"**Links:**\n\n{links_md}".strip()
        )

        date_only = raw_date.split('T', 1)[0].split('+', 1)[0]
        tags = ['TED', pubnum, date_only, buyer]
        tags = [clean_tag(t) for t in tags if clean_tag(t)]
        extras = {
            'publication_number': pubnum,
            'buyer_name': buyer,
            'publication_date': date_only
        }
        # Paket anlegen oder holen
        pkg = self._get_or_create_package(
            name=dataset_name,
            title=title_text,
            description=description,
            tags=tags,
            extras=extras
        )
        pkg_id = pkg['id']

        # JSON-Resource erstellen/aktualisieren
        notice_json = json.dumps(notice, ensure_ascii=False, indent=2)
        fp = io.BytesIO(notice_json.encode('utf-8'))
        dateiname = "ted_example"
        try:
            with open(dateiname, 'wb') as f:  # 'wb' steht für "write binary"
                f.write(fp.getvalue())
            print(f"Die Datei '{dateiname}' wurde erfolgreich gespeichert.")
        except IOError as e:
            print(f"Fehler beim Speichern der Datei: {e}")

        fp.name = f"ted_{pubnum}.json"
        res_args = {
            'package_id': pkg_id,
            'name': fp.name,
            'upload': fp,
            'format': 'json',
            'title': title_text
        }
        # prüfen, ob Resource schon da ist
        existing = next((r for r in pkg.get('resources', []) if r['name'] == fp.name), None)
        if existing:
            return False

        tk.get_action('resource_create')(self.context, res_args)
        return True

    def publish_ted_notices(self, file_path):
        """
        Liest eine TED-JSON-Datei ein, zerlegt sie in Notices und legt
        je ein CKAN-Dataset pro Notice an.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        notices = data.get('notices', [])
        print(f"Found {len(notices)} notices in {file_path}")
        accepted = 0
        for notice in notices:
            pubnum = notice.get('publication-number', 'unknown')
            try:
                if self._publish_ted_notice(notice):
                    accepted += 1
            except Exception as e:
                print(f"Error at Notice {pubnum}: {e}")
        print(f"[INFO] {accepted} / {len(notices)} notices published.")

    def _publish_bescha_notice(self, release):
        """
        Publiziert ein einzelnes OCDS-Release aus BeschA als eigenes Dataset.
        """
        # Eindeutige ID und Name
        rel_id = release.get('id') or release.get('ocid', 'unknown')
        dataset_name = f"bescha-{rel_id}"

        # Titel – hier z.B. der Tender-Titel oder die OCID
        title_text = release.get('tender', {}).get('title') or rel_id

        # Datum
        raw_date = release.get('date', '')  # e.g. "2024-11-10T23:00:00Z"
        date_only = raw_date.split('T',1)[0]

        # Beschaffungseinheit (Buyer)
        buyer = release.get('buyer', {}).get('name', 'unknown')

        # Beschreibung
        description = (
            f"**OCID:** {release.get('ocid','')}\n\n"
            f"**Release ID:** {rel_id}\n\n"
            f"**Date:** {raw_date}\n\n"
            f"**Buyer:** {buyer}"
        )

        # Tags
        tags = ['BESCHA', rel_id, date_only, buyer]
        tags = [clean_tag(t) for t in tags if clean_tag(t)]

        # Extras
        extras = {
            'ocid': release.get('ocid',''),
            'release_id': rel_id,
            'date': date_only,
            'buyer': buyer
        }
        # Package anlegen oder holen
        pkg = self._get_or_create_package(
            name=dataset_name,
            title=title_text,
            description=description,
            tags=tags,
            extras=extras
        )
        pkg_id = pkg['id']

        # Resource (JSON) erzeugen
        notice_json = json.dumps(release, ensure_ascii=False, indent=2)
        fp = io.BytesIO(notice_json.encode('utf-8'))
        fp.name = f"bescha_{rel_id}.json"
        res_args = {
            'package_id': pkg_id,
            'name': fp.name,
            'upload': fp,
            'format': 'json',
            'title': title_text
        }

        # Prüfen, ob Resource schon existiert
        existing = next((r for r in pkg.get('resources', []) if r['name'] == fp.name), None)
        if existing:
            return False

        # Anlegen
        tk.get_action('resource_create')(self.context, res_args)
        return True


    def publish_bescha_notices(self, data):
        """
        Liest eine BeschA-JSON-Datei ein (mit OCDS 'releases') und legt
        je ein CKAN-Dataset pro Release an.
        """

        if isinstance(data, dict):
            releases = data.get('notices', []) or data.get('releases') or []
        else:
            # altes Verhalten: file_path einlesen
            with open(data, 'r', encoding='utf-8') as f:
                obj = json.load(f)
            releases = data.get('notices', []) or obj.get('notices') or []

        count = 0
        for rel in releases:
            success = self._publish_bescha_notice(rel)
            if success:
                count += 1
        print(f"[INFO] {count} / {len(releases)} BESCHA releases published.")
