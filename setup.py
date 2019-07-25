"""Package up cidc_api/models.py for use in other services."""

from setuptools import setup

setup(
    name="cidc_api_models",
    description="SQLAlchemy data models used in the CIDC API",
    python_requires=">=3.6",
    install_requires=["flask-sqlalchemy==2.4.0", "flask==1.1.1"],
    license="MIT license",
    py_modules=["cidc_api.models"],
    url="https://github.com/CIMAC-CIDC/cidc_api-gae",
    version="0.1.0",
    zip_safe=False,
)
