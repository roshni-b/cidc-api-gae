"""Package up cidc_api/models.py for use in other services."""

from setuptools import setup

setup(
    name="cidc_api_modules",
    description="SQLAlchemy data models and configuration tools used in the CIDC API",
    python_requires=">=3.6",
    install_requires=[
        "flask==1.1.1",
        "flask-sqlalchemy==2.4.0",
        "google-cloud-storage==1.16.1",
    ],
    license="MIT license",
    packages=["cidc_api.config"],
    py_modules=["cidc_api.models"],
    url="https://github.com/CIMAC-CIDC/cidc_api-gae",
    version="0.1.0",
    zip_safe=False,
)
