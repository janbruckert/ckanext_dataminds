import logging
from ckan.plugins import SingletonPlugin, implements, IConfigurer, IBlueprint, ITemplateHelpers
from ckan.plugins.toolkit import add_template_directory, add_public_directory

from . import cron_jobs

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class DatamindsPlugin(SingletonPlugin):
    implements(IConfigurer)
    implements(IBlueprint)
    implements(ITemplateHelpers)

    def update_config(self, config):
        # Add templates and public directories from the extension
        add_template_directory(config, 'templates')
        add_public_directory(config, 'public')
        # Set default cron job schedules (can be overridden in the CKAN config file)
        config.setdefault('dataminds.ted_schedule', '0 0 * * *')      # e.g., daily at midnight
        config.setdefault('dataminds.bescha_schedule', '0 1 * * *')    # e.g., daily at 1 AM
        return config

    def get_blueprint(self):
        # Return the Flask blueprint for the admin interface
        from .controller import dataminds_blueprint
        return dataminds_blueprint

    def get_helpers(self):
        # Make the cron job functions available in templates
        return {
        }

