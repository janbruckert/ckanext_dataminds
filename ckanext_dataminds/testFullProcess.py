import io
import json
import unittest
import zipfile
from datetime import datetime
from unittest.mock import MagicMock, patch

from ckanext_dataminds import DataFetcher, MongoWriter, CkanPublisher, run_ted_cron_job, run_bescha_cron_job

class TestFullProcess(unittest.TestCase):

    @patch('ckanext_dataminds.CkanPublisher.RemoteCKAN')
    @patch('ckanext_dataminds.dataFetch.requests.post')
    @patch('ckanext_dataminds.dataFetch.requests.get')
    @patch('ckanext_dataminds.mongoWriter.MongoClient')
    def test_full_process(self, mock_mongo_client, mock_requests_get, mock_requests_post, mock_remote_ckan):
        # --- Setup für TED (POST) ---
        dummy_ted_data = {
            "notices": [
                {
                    "publication-date": "2023-11-01",
                    "title-proc": {"eng": "Example TED Notice"},
                    "buyer-name": {"eng": ["Example Buyer"]}
                }
            ]
        }
        dummy_post_response = MagicMock()
        dummy_post_response.ok = True
        dummy_post_response.json.return_value = dummy_ted_data
        mock_requests_post.return_value = dummy_post_response

        # --- Setup für BeschA (GET) ---
        dummy_bescha_data = {"dummy": "value"}
        dummy_json = json.dumps(dummy_bescha_data).encode("utf-8")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w") as zf:
            zf.writestr("dummy.json", dummy_json)
        zip_buffer.seek(0)
        dummy_get_response = MagicMock()
        dummy_get_response.ok = True
        dummy_get_response.content = zip_buffer.read()
        mock_requests_get.return_value = dummy_get_response

        # --- Setup für MongoDB ---
        dummy_ted_collection = MagicMock()
        dummy_ted_collection.find.return_value = dummy_ted_data["notices"]
        dummy_bescha_collection = MagicMock()
        dummy_bescha_collection.find.return_value = [dummy_bescha_data]
        dummy_db = MagicMock()
        dummy_db.__getitem__.side_effect = lambda name: dummy_ted_collection if name == "ted_data" else dummy_bescha_collection
        mock_mongo_client.return_value = dummy_db

        # --- Setup für CKAN-API ---
        dummy_ckan = MagicMock()
        # package_show löst NotFound aus, um die Erstellung zu erzwingen
        dummy_ckan.action.package_show.side_effect = Exception("NotFound")
        dummy_package = {"id": "dummy-package-id", "extras": []}
        dummy_ckan.action.package_create.return_value = dummy_package
        dummy_resource = {"id": "dummy-resource-id"}
        dummy_ckan.action.resource_create.return_value = dummy_resource
        mock_remote_ckan.return_value = dummy_ckan

        # --- Ausführung der Cron Jobs ---
        run_ted_cron_job()
        run_bescha_cron_job()

        # --- Assertions ---
        self.assertTrue(dummy_ckan.action.package_create.called, "package_create wurde nicht aufgerufen.")
        self.assertTrue(dummy_ckan.action.resource_create.called, "resource_create wurde nicht aufgerufen.")

if __name__ == '__main__':
    unittest.main()

