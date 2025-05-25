import json

from flask import Blueprint, render_template, request, redirect, url_for, flash
import os
import ckan.plugins.toolkit as tk
from .cron_jobs import run_ted_cron_job, run_ted_cron_job_for, run_bescha_cron_job

# Define the blueprint with the template folder relative to this module
dataminds_blueprint = Blueprint('dataminds', __name__, template_folder='templates/dataminds')

# Path to the log file (adjust as necessary)
LOG_FILE_PATH = '/var/log/ckan/ckanext_dataminds.log'
BASE_DIR         = "/srv/app/ckanext_dataminds"
SETTINGS_FILE    = os.path.join(BASE_DIR, "settings.json")

@dataminds_blueprint.route('/admin/dataminds', methods=['GET'])
def settings():
    """
    Render the settings page for the Dataminds plugin.
    Displays current cron schedules, additional settings, and the last 50 log lines.
    """

    # Read the log file and capture the last 50 lines
    log_lines = []
    if os.path.exists(LOG_FILE_PATH):
        print("---------------------------------------------------LOG FILE EXISTS")
        try:
            with open(LOG_FILE_PATH, 'r') as log_file:
                lines = log_file.readlines()
                log_lines = [line.strip() for line in lines[-50:]]
        except Exception as e:
            flash(f"Error reading log file: {e}", "error")
    settings_data = load_settings()
    # Render the settings template; note that the blueprint's template_folder is used, so use the template name directly.
    return render_template('settings.html', settings=settings_data,
                           log_lines=log_lines)

@dataminds_blueprint.route('/admin/dataminds/update', methods=['POST'], endpoint='update_settings')
def update_settings():
    source = request.form.get('source')
    freq   = request.form.get('data_frequency')
    start  = request.form.get('start_date')
    end    = request.form.get('end_date')

    settings = load_settings()
    if source in settings:
        settings[source]['frequency']   = freq
        settings[source]['start_date']  = start
        settings[source]['end_date']    = end
        save_settings(settings)
        flash("Settings gespeichert.", "success")
    else:
        flash("Unbekannte Datenquelle.", "error")

    return redirect(url_for('dataminds.settings'))


@dataminds_blueprint.route('/admin/dataminds/trigger/<source>')
def trigger(source):
    settings = load_settings()
    # zuerst Query-Parameter, sonst zuletzt gespeicherte Werte
    start = request.args.get('start_date') or settings.get(source, {}).get('start_date')
    end   = request.args.get('end_date')   or settings.get(source, {}).get('end_date')

    if source == 'ted':
        run_ted_cron_job_for(start_date=start, end_date=end)
        flash(f"Ted-Cron gestartet für {start or 'Vortag'} … {end or ''}", "success")
    elif source == 'bescha':
        run_bescha_cron_job()
        flash("BeschA-Cron gestartet.", "success")
    else:
        flash("Unbekannte Datenquelle.", "error")

    return redirect(url_for('dataminds.settings'))

def load_settings():
    defaults = {
        "ted":   {"frequency": "daily", "start_date": "", "end_date": ""},
        "bescha":{"frequency": "daily", "start_date": "", "end_date": ""}
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                data = json.load(f)
                defaults.update(data)
        except Exception:
            pass
    return defaults

def save_settings(settings):
    os.makedirs(BASE_DIR, exist_ok=True)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)