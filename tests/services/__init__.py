import os

from models import Users

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(CUR_DIR, "data")


def open_data_file(filename: str):
    """Return an open file pointer to filename"""
    path = os.path.join(DATA_DIR, filename)
    return open(path, "rb")
