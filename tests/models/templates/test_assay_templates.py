from datetime import datetime
import os.path
import pytest
from unittest.mock import MagicMock

from cidc_api.models import (
    HandeAssay,
    HandeUpload,
    insert_record_batch,
    Users,
    WesBamAssay,
    WesFastqAssay,
    WESUpload,
)
from cidc_api.shared import auth

from .examples import EXAMPLE_DIR
from .utils import setup_example


def mock_get_current_user(monkeypatch):
    get_current_user = MagicMock()
    get_current_user.return_value = Users(email="user@email.com")
    monkeypatch.setattr(auth, "get_current_user", get_current_user)


def test_hande_assay(clean_db, cidc_api, monkeypatch, tmp_path):
    # test write and empty read
    f = tmp_path / "pbmc_template.xlsx"
    with cidc_api.app_context():
        HandeAssay.write(f)

        # empty read test shows that format is correct
        # and confirms that empty templates fail
        with pytest.raises(Exception, match="required value protocol identifier"):
            HandeAssay.read(f)

    mock_get_current_user(monkeypatch)
    setup_example(clean_db, cidc_api)

    with cidc_api.app_context():
        records = HandeAssay.read(os.path.join(EXAMPLE_DIR, "hande_assay.xlsx"))
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join([str(e) for e in errors])

        entry = clean_db.query(HandeUpload).first()

        assert entry is not None
        assert entry.trial_id == "test_trial"

        records = [r for r in entry.records]
        images = [i for i in entry.images]
        assert len(records) == 2, str(records)
        assert len(images) == 2, str(images)
        for i in (1, 2):
            record, image = records[i - 1], images[i - 1]
            assert record.cimac_id == f"CTTTPP1{i}1.00"
            assert record.upload_id == entry.id
            assert record.trial_id == "test_trial"
            for k in [
                "tumor_tissue_percentage",
                "viable_tumor_percentage",
                "viable_stroma_percentage",
                "necrosis_percentage",
                "fibrosis_percentage",
            ]:
                assert getattr(record, k) == i
            assert record.comment == f"a{'nother' if i-1 else ''} comment"

            assert image.object_url == f"test_trial/hande/CTTTPP1{i}1.00/image_file.svs"
            assert image.upload_id == entry.id
            assert image.trial_id == "test_trial"
            assert image.local_path == f"path/to/image{i}.svs"
            assert image.data_format == "hande_image.svs"

            assert record.image.unique_field_values() == image.unique_field_values()


def test_wes_fastq_assay(clean_db, cidc_api, monkeypatch):
    mock_get_current_user(monkeypatch)
    setup_example(clean_db, cidc_api)

    with cidc_api.app_context():
        records = WesFastqAssay.read(os.path.join(EXAMPLE_DIR, "wes_fastq_assay.xlsx"))
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join([str(e) for e in errors])

    assert_wes_fastq_worked(cidc_api, clean_db)


def assert_wes_fastq_worked(cidc_api, clean_db):
    with cidc_api.app_context():
        entry = clean_db.query(WESUpload).first()

        assert entry is not None
        assert entry.trial_id == "test_trial"
        assert (
            entry.sequencing_protocol
            == "Express Somatic Human WES (Deep Coverage) v1.1"
        )
        assert entry.library_kit == "Hyper Prep ICE Exome Express: 1.0"
        assert entry.sequencer_platform == "Illumina - NextSeq 550"
        assert entry.paired_end_reads == "Paired"
        assert entry.read_length == 100
        assert entry.bait_set == "whole_exome_illumina_coding_v1"

        records = [r for r in entry.records]
        assert len(records) == 2
        for i in (1, 2):
            record = records[i - 1]
            assert record.cimac_id == f"CTTTPP1{i}1.00"
            assert (
                record.sequencing_date == datetime.strptime("20100101", "%Y%m%d").date()
            )
            assert record.quality_flag in (1, 1.0)

            files = [f for f in record.files]
            for n, file in enumerate(files):
                assert file.lane == 1 + n
                assert (
                    file.r1_object_url
                    == f"test_trial/wes/CTTTPP1{i}1.00/r1_L{1+n}.fastq.gz"
                )
                assert (
                    file.r2_object_url
                    == f"test_trial/wes/CTTTPP1{i}1.00/r2_L{1+n}.fastq.gz"
                )

                assert (
                    file.r1.local_path
                    == f"/local/path/to/fwd.1.{1 + i%2}.1"
                    + ("_2" if n == 1 else "")
                    + ".fastq.gz"
                )
                assert (
                    file.r2.local_path
                    == f"/local/path/to/rev.1.{1 + i%2}.1"
                    + ("_2" if n == 1 else "")
                    + ".fastq.gz"
                )


def test_wes_bam_assay(clean_db, cidc_api, monkeypatch):
    mock_get_current_user(monkeypatch)
    setup_example(clean_db, cidc_api)

    with cidc_api.app_context():
        records = WesBamAssay.read(os.path.join(EXAMPLE_DIR, "wes_bam_assay.xlsx"))
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join([str(e) for e in errors])

        entry = clean_db.query(WESUpload).first()

        assert entry is not None
        assert entry.trial_id == "test_trial"
        assert (
            entry.sequencing_protocol
            == "Express Somatic Human WES (Deep Coverage) v1.1"
        )
        assert entry.library_kit == "Hyper Prep ICE Exome Express: 1.0"
        assert entry.sequencer_platform == "Illumina - NextSeq 550"
        assert entry.paired_end_reads == "Paired"
        assert entry.read_length == 100
        assert entry.bait_set == "whole_exome_illumina_coding_v1"

        records = [r for r in entry.records]
        assert len(records) == 2
        for i in (1, 2):
            record = records[i - 1]
            assert record.cimac_id == f"CTTTPP1{i}1.00"
            assert (
                record.sequencing_date == datetime.strptime("20100101", "%Y%m%d").date()
            )
            assert record.quality_flag in (1, 1.0)

            files = [f for f in record.files]
            for n, file in enumerate(files):
                assert file.number == 1 + n
                assert (
                    file.bam_object_url
                    == f"test_trial/wes/CTTTPP1{i}1.00/reads_{1+n}.bam"
                )

                assert (
                    file.bam.local_path
                    == f"gs://local/path/to/fwd.1.{1 + i%2}.1"
                    + ("_2" if n == 1 else "")
                    + ".bam"
                )
