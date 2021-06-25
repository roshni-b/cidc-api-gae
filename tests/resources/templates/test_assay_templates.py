import os.path
from cidc_api.models import HandeAssay, insert_record_batch

from .examples import EXAMPLE_DIR


def test_hande_assay(cidc_api):
    with cidc_api.app_context():
        records = HandeAssay.read(os.path.join(EXAMPLE_DIR, "hande_assay.xlsx"))
        errors = insert_record_batch(records)
        assert len(errors) == 0
