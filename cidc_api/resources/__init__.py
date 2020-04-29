from flask import Flask

from .info import info_bp
from .users import users_bp
from .permissions import permissions_bp
from .trial_metadata import trial_metadata_bp
from .downloadable_files import downloadable_files_bp
from .upload_jobs import upload_jobs_bp, ingestion_bp


def register_resources(app: Flask):
    """Wire up the CIDC resource blueprints to `app`."""
    app.url_map.strict_slashes = False

    app.register_blueprint(info_bp, url_prefix="/info")
    app.register_blueprint(users_bp, url_prefix="/users")
    app.register_blueprint(permissions_bp, url_prefix="/permissions")
    app.register_blueprint(trial_metadata_bp, url_prefix="/trial_metadata")
    app.register_blueprint(downloadable_files_bp, url_prefix="/downloadable_files")
    app.register_blueprint(upload_jobs_bp, url_prefix="/upload_jobs")
    app.register_blueprint(ingestion_bp, url_prefix="/ingestion")
