from os.path import dirname, abspath, join

from eve import Eve
from eve.auth import TokenAuth
from eve_sqlalchemy import SQL
from eve_sqlalchemy.validation import ValidatorSQL
from eve_swagger import swagger
from flask import jsonify
from flask_migrate import Migrate, upgrade

from errors import register_error_handlers
from models import BaseModel
from auth import BearerAuth

ABSPATH = dirname(abspath(__file__))
SETTINGS = join(ABSPATH, "settings.py")
MIGRATIONS = join(ABSPATH, "migrations")

# Instantiate the Eve app
app = Eve(auth=BearerAuth, data=SQL, validator=ValidatorSQL, settings=SETTINGS)

# Bind the data model to the app's database engine
db = app.data.driver
BaseModel.metadata.bind = db.engine
db.Model = BaseModel

# Configure flask-migrate and upgrade the database
# Note: while upgrades are performed automatically,
# generating the migrations should be performed by hand
# using the flask-migrate CLI, and the resulting files
# should be checked into source control.
Migrate(app, db)
with app.app_context():
    app.logger.info("Upgrading the database...")
    upgrade(MIGRATIONS)
    app.logger.info("Done upgrading the database.")

# Configure the swagger site
# TODO: flesh this out
app.register_blueprint(swagger)
app.config["SWAGGER_INFO"] = {
    "title": "CIDC API",
    "version": "0.1",
    "termsOfService": "[TODO]",
    "contact": {
        "name": "support",
        "url": "https://github.com/cimac-cidc/cidc-api-gae/blob/master/README.md",
    },
    "license": {
        "name": "MIT",
        "url": "https://github.com/dfci/cidc-api-gae/blob/master/LICENSE",
    },
    "schemes": ["http", "https"],
}

# Register custom error handlers with the API server
register_error_handlers(app)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
