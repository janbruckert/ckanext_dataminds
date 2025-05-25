"""CKAN DataMinds Extension

Version: 0.1.0
Author: Jan Bruckert
"""
# ckanext_dataminds/__init__.py

from .ckanext_dataminds import (
    DataFetcher,
    MongoWriter,
    CkanPublisher,
    run_ted_cron_job,
    run_bescha_cron_job,
    DatamindsPlugin
)


__version__ = "0.1.0"
__author__ = "Jan Bruckert"


