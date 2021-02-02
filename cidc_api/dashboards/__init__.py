from flask import Flask

from .upload_jobs_table import upload_jobs_table


def register_dashboards(app: Flask):
    """Add dashboard endpoints to the provided Flask app instance."""
    upload_jobs_table.init_app(app)
