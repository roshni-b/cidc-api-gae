import logging
import traceback
from os.path import dirname, abspath, join

from eve import Eve
from eve.auth import TokenAuth
from eve_sqlalchemy import SQL
from eve_sqlalchemy.validation import ValidatorSQL
from flask import jsonify
from flask_migrate import Migrate, upgrade
from flask_cors import CORS

import cidc_schemas

from models import BaseModel
from auth import BearerAuth
from services import register_services

ABSPATH = dirname(abspath(__file__))
SETTINGS = join(ABSPATH, "config", "settings.py")
MIGRATIONS = join(ABSPATH, "..", "migrations")

# Instantiate the Eve app
app = Eve(
    auth=BearerAuth,
    data=SQL,
    validator=ValidatorSQL,
    settings=SETTINGS,
    static_folder=None,
)

# Inherit logging config from gunicorn if running behind gunicorn
app.logger.setLevel(logging.DEBUG)
if __name__ != "__main__":
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

# Log tracebacks on server errors
@app.errorhandler(500)
def print_server_error(exception):
    """Print out the traceback and error message for all server errors."""
    try:
        orig_exc = exception.original_exception
    except AttributeError:
        orig_exc = exception
    traceback.print_exception(type(orig_exc), orig_exc, orig_exc.__traceback__)


# Enable CORS
# TODO: be more selective about which domains can make requests
CORS(app, resources={r"*": {"origins": "*"}})

# Register custom services
register_services(app)

# Bind the data model to the app's database engine
db = app.data.driver
BaseModel.metadata.bind = db.engine
db.Model = BaseModel

# Configure flask-migrate and upgrade the database
# Note: while upgrades are performed automatically,
# generating the migrations should be performed by hand
# using the flask-migrate CLI, and the resulting files
# should be checked into source control.
Migrate(app, db, MIGRATIONS)
with app.app_context():
    upgrade(MIGRATIONS)

# Generate empty manifest/assay templates on startup
print(
    f"Writing empty templates to {app.config['TEMPLATES_DIR']} (cidc_schemas=={cidc_schemas.__version__})"
)
cidc_schemas.template.generate_all_templates(app.config["TEMPLATES_DIR"])

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
