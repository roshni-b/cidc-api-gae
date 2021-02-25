from flask import Flask

from .shipments import shipments_dashboard


def register_dashboards(app: Flask):
    """Add dashboard endpoints to the provided Flask app instance."""
    shipments_dashboard.init_app(app)
