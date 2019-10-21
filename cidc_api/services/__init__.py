"""A collection of Eve event hooks and custom endpoints."""
from eve import Eve

from .files import files_api, register_files_hooks
from .info import info_api
from .ingestion import ingestion_api, register_ingestion_hooks
from .users import users_api, register_users_hooks
from .permissions import register_permissions_hooks


def register_services(app: Eve):
    """Register service blueprints with the provided app"""
    # Blueprints
    app.register_blueprint(files_api)
    app.register_blueprint(ingestion_api)
    app.register_blueprint(info_api)
    app.register_blueprint(users_api)

    # Hooks
    register_ingestion_hooks(app)
    register_users_hooks(app)
    register_files_hooks(app)
    register_permissions_hooks(app)
