import os

from eve import Eve
from eve.auth import TokenAuth
from eve_sqlalchemy import SQL
from eve_sqlalchemy.validation import ValidatorSQL
from eve_swagger import swagger
from flask import jsonify
from flask_migrate import Migrate, upgrade

from models import BaseModel
from auth import BearerAuth, AuthError

# Instantiate the Eve app
app = Eve(auth=BearerAuth, data=SQL, validator=ValidatorSQL, settings="settings.py")

# Bind the data model to the app's database engine
db = app.data.driver
BaseModel.metadata.bind = db.engine
db.Model = BaseModel

# Configure flask-migrate
Migrate(app, db)

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

# Error handling
@app.errorhandler(AuthError)
def handle_auth_error(e: AuthError):
    response = jsonify(e.json())
    response.status_code = e.status_code
    return response


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
