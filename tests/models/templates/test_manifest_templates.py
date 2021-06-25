import os.path
import pytest

from cidc_api.models import ClinicalDataTemplate, insert_record_batch, PBMCTemplate
from .examples import EXAMPLE_DIR
from .utils import set_up_example_trial


def test_clinical_data_template(cidc_api):
    with cidc_api.app_context():
        ClinicalDataTemplate.write("clinical_data_manifest.xlsx")


def test_pbmc_template(cidc_api, tmp_path):
    # test successful read
    with cidc_api.app_context():
        set_up_example_trial(cidc_api)

        records = PBMCTemplate.read(os.path.join(EXAMPLE_DIR, "pbmc_manifest.xlsx"))
        assert len(records) > 0
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

    # test write and empty read
    f = tmp_path / "pbmc_template.xlsx"
    with cidc_api.app_context():
        PBMCTemplate.write(f)

        # empty read test shows that format is correct
        # and confirms that empty templates fail
        with pytest.raises(Exception, match="required value protocol identifier"):
            PBMCTemplate.read(f)
