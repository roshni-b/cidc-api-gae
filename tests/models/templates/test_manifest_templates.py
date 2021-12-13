import os

os.environ["TZ"] = "UTC"
from datetime import datetime
import os.path
import pytest

from cidc_api.models import (
    # ClinicalDataTemplate,
    insert_record_batch,
    Participant,
    PbmcManifest,
    Sample,
    Shipment,
    TissueSlideManifest,
)
from .examples import EXAMPLE_DIR
from .utils import set_up_example_trial


# def test_clinical_data_template(cidc_api):
#     with cidc_api.app_context():
#         ClinicalDataTemplate.write("clinical_data_manifest.xlsx")


def test_pbmc_template(clean_db, cidc_api, tmp_path):
    # test write and empty read
    f = tmp_path / "pbmc_template.xlsx"
    with cidc_api.app_context():
        PbmcManifest.write(f)

        # confirm that empty templates fail
        with pytest.raises(Exception, match="Error in processing preamble"):
            PbmcManifest.read(f)

    # test successful read
    with cidc_api.app_context():
        set_up_example_trial(clean_db, cidc_api)

        records = PbmcManifest.read(os.path.join(EXAMPLE_DIR, "pbmc_manifest.xlsx"))
        assert len(records) > 0
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

    assert_pbmc_worked(cidc_api, clean_db)


def assert_pbmc_worked(cidc_api, clean_db):
    with cidc_api.app_context():
        shipments = clean_db.query(Shipment).all()
        participants = (
            clean_db.query(Participant).order_by(Participant.cimac_participant_id).all()
        )
        samples = clean_db.query(Sample).all()

        assert len(shipments) == 1, ",".join([s.manifest_id for s in shipments])
        shipment = shipments[0]
        assert shipment.trial_id == "test_trial"
        assert shipment.manifest_id == "test_prism_trial_id_PBMC"
        assert shipment.assay_priority == "4"
        assert shipment.assay_type == "Olink"
        assert shipment.receiving_party == "MSSM_Rahman"
        assert shipment.courier == "USPS"
        assert shipment.tracking_number == "TrackN"
        assert shipment.account_number == "AccN"
        assert shipment.shipping_condition == "Frozen_Dry_Ice"
        assert shipment.date_shipped == datetime(2001, 10, 10).date()
        assert shipment.date_received == datetime(2002, 10, 10).date()
        assert (
            shipment.quality_of_shipment
            == "Specimen shipment received in good condition"
        )
        assert shipment.ship_from == "ship from"
        assert shipment.ship_to == "ship to"

        assert shipment.json_data["assay_priority"] == "4"
        assert shipment.json_data["assay_type"] == "Olink"
        assert shipment.json_data["receiving_party"] == "MSSM_Rahman"
        assert shipment.json_data["courier"] == "USPS"
        assert shipment.json_data["tracking_number"] == "TrackN"
        assert shipment.json_data["account_number"] == "AccN"
        assert shipment.json_data["shipping_condition"] == "Frozen_Dry_Ice"
        assert shipment.json_data["date_shipped"] == "20011010"
        assert shipment.json_data["date_received"] == "20021010"
        assert (
            shipment.json_data["quality_of_shipment"]
            == "Specimen shipment received in good condition"
        )
        assert shipment.json_data["ship_from"] == "ship from"
        assert shipment.json_data["ship_to"] == "ship to"

        assert len(participants) == 2
        for i, partic in enumerate(
            sorted(participants, key=lambda p: p.cimac_participant_id[-1])
        ):
            assert partic.trial_id == "test_trial"
            assert partic.cimac_participant_id == f"CTTTP0{i+1}"
            assert partic.trial_participant_id == f"TTTP0{i+1}"
            assert partic.json_data["trial_participant_id"] == f"TTTP0{i+1}"
            if i == 0:
                assert partic.cohort_name == "Arm_Z"
                assert partic.gender == "Female"
                assert partic.race == "Asian"
                assert partic.ethnicity == "Hispanic or Latino"
                assert partic.json_data["gender"] == "Female"
                assert partic.json_data["race"] == "Asian"
                assert partic.json_data["ethnicity"] == "Hispanic or Latino"
            else:  # i == 1
                assert partic.cohort_name == "Arm_A"
                assert partic.gender == "Male"
                assert partic.race == "Native Hawaiian/Pacific Islander"
                assert partic.ethnicity == "Unknown"
                assert partic.json_data["gender"] == "Male"
                assert partic.json_data["race"] == "Native Hawaiian/Pacific Islander"
                assert partic.json_data["ethnicity"] == "Unknown"

        assert len(samples) == 6
        for i, sample in enumerate(sorted(samples, key=lambda s: s.cimac_id[-6:-3])):
            partic = participants[i // 3]

            assert sample.trial_id == "test_trial"
            assert sample.cimac_id == f"{partic.cimac_participant_id}A{i%3 +1}.00"
            assert sample.cimac_participant_id == partic.cimac_participant_id
            assert (
                sample.collection_event_name == "Baseline"
                if i % 3 + 1 != 2
                else "Pre_Day_1_Cycle_2"
            )
            assert sample.manifest_id == shipment.manifest_id

            assert sample.shipping_entry_number == i + 1
            assert sample.box_number == "1"
            assert (
                sample.surgical_pathology_report_id
                == f"Surgical pathology report {i+1}"
            )
            assert sample.clinical_report_id == f"clinical report {i+1}"
            assert sample.parent_sample_id == f"TRIALGROUP {i+1}"
            assert sample.processed_sample_id == "BIOBANK 1"
            assert sample.site_description == "ANAL CANAL & ANUS"
            assert sample.topography_code == "C00.1"
            assert sample.topography_description == "LIP"
            assert sample.histology_behavior == "8004/3"
            assert sample.histology_behavior_description == "Neoplasm, malignant"
            assert sample.sample_location == "123"
            assert sample.type_of_sample == "Blood"
            assert sample.sample_collection_procedure == "Core Biopsy"
            assert (
                sample.type_of_primary_container
                == "Stool collection container with DNA stabilizer"
            )
            assert sample.processed_sample_type == "Plasma"
            assert sample.processed_sample_volume in (i + 1, float(i + 1))
            assert sample.processed_sample_volume_units == "Other"
            assert sample.processed_sample_concentration == 0.3 if i in (1, 5) else 0.2
            assert sample.processed_sample_concentration_units == "Not Reported"
            assert sample.processed_sample_derivative == "Tumor DNA"
            assert sample.sample_derivative_volume in (0, 0.0)
            assert sample.sample_derivative_volume_units == "Microliters"
            assert sample.sample_derivative_concentration in (1, 1.0)
            assert (
                sample.sample_derivative_concentration_units
                == "Nanogram per Microliter"
            )
            assert sample.pbmc_viability in (1, 1.0)
            assert sample.pbmc_recovery in (1, 1.0)
            assert sample.material_used in (1, 1.0)
            assert sample.material_remaining in (0, 0.0)
            assert (
                sample.material_storage_condition == "Other"
                if i < 3
                else "Not Reported"
            )
            assert sample.quality_of_sample == "Fail" if i % 2 else "Pass"
            assert sample.sample_replacement == "Replacement Not Requested"
            assert sample.residual_sample_use == "Sample Returned"
            assert (
                sample.diagnosis_verification
                == "Local review consistent with diagnostic pathology report"
            )
            assert sample.intended_assay == shipment.assay_type

            # check deprecated fields
            assert sample.json_data["shipping_entry_number"] == str(i + 1)
            assert sample.json_data["box_number"] == "1"
            assert (
                sample.json_data["surgical_pathology_report_id"]
                == f"Surgical pathology report {i+1}"
            )
            assert sample.json_data["clinical_report_id"] == f"clinical report {i+1}"
            assert sample.json_data["parent_sample_id"] == f"TRIALGROUP {i+1}"
            assert sample.json_data["processed_sample_id"] == "BIOBANK 1"
            assert sample.json_data["site_description"] == "ANAL CANAL & ANUS"
            assert sample.json_data["topography_code"] == "C00.1"
            assert sample.json_data["topography_description"] == "LIP"
            assert sample.json_data["histology_behavior"] == "8004/3"
            assert (
                sample.json_data["histology_behavior_description"]
                == "Neoplasm, malignant"
            )
            assert sample.json_data["sample_location"] == "123"
            assert sample.json_data["type_of_sample"] == "Blood"
            assert sample.json_data["sample_collection_procedure"] == "Core Biopsy"
            assert (
                sample.json_data["type_of_primary_container"]
                == "Stool collection container with DNA stabilizer"
            )
            assert sample.json_data["processed_sample_type"] == "Plasma"
            assert sample.json_data["processed_sample_volume"] in (
                str(i + 1),
                str(float(i + 1)),
            )
            assert sample.json_data["processed_sample_volume_units"] == "Other"
            assert (
                sample.json_data["processed_sample_concentration"] == str(0.3)
                if i in (1, 5)
                else str(0.2)
            )
            assert (
                sample.json_data["processed_sample_concentration_units"]
                == "Not Reported"
            )
            assert sample.json_data["processed_sample_derivative"] == "Tumor DNA"
            assert sample.json_data["sample_derivative_volume"] in (str(0), str(0.0))
            assert sample.json_data["sample_derivative_volume_units"] == "Microliters"
            assert sample.json_data["sample_derivative_concentration"] in (
                str(1),
                str(1.0),
            )
            assert (
                sample.json_data["sample_derivative_concentration_units"]
                == "Nanogram per Microliter"
            )
            assert sample.json_data["pbmc_viability"] in (str(1), str(1.0))
            assert sample.json_data["pbmc_recovery"] in (str(1), str(1.0))
            assert sample.json_data["material_used"] in (str(1), str(1.0))
            assert sample.json_data["material_remaining"] in (str(0), str(0.0))
            assert (
                sample.json_data["material_storage_condition"] == "Other"
                if i < 3
                else "Not Reported"
            )
            assert sample.json_data["quality_of_sample"] == "Fail" if i % 2 else "Pass"
            assert sample.json_data["sample_replacement"] == "Replacement Not Requested"
            assert sample.json_data["residual_sample_use"] == "Sample Returned"
            assert (
                sample.json_data["diagnosis_verification"]
                == "Local review consistent with diagnostic pathology report"
            )

            # Non-optimal, but stored on Shipment so not really an issue
            assert "intended_assay" not in sample.json_data

            if i < 3:
                if i == 0:
                    assert sample.pbmc_resting_period_used == "Yes"
                    assert sample.comments == "Comment"
                    assert sample.json_data["pbmc_resting_period_used"] == "Yes"
                    assert sample.json_data["comments"] == "Comment"
                elif i == 1:
                    assert sample.pbmc_resting_period_used == "No"
                    assert sample.comments is None
                    assert sample.json_data["pbmc_resting_period_used"] == "No"
                    assert "comments" not in sample.json_data
                else:  # i == 2
                    assert sample.pbmc_resting_period_used == "Not Reported"
                    assert sample.comments is None
                    assert (
                        sample.json_data["pbmc_resting_period_used"] == "Not Reported"
                    )
                    assert "comments" not in sample.json_data
            else:  # i >= 3
                assert sample.pbmc_resting_period_used == "Other"
                assert sample.comments is None
                assert sample.json_data["pbmc_resting_period_used"] == "Other"
                assert "comments" not in sample.json_data


def test_tissue_slide_template(clean_db, cidc_api, tmp_path):
    # test write and empty read
    f = tmp_path / "tissue_slide_template.xlsx"
    with cidc_api.app_context():
        TissueSlideManifest.write(f)

        # confirm that empty templates fail
        with pytest.raises(Exception, match="Error in processing preamble"):
            TissueSlideManifest.read(f)

    # test successful read
    with cidc_api.app_context():
        set_up_example_trial(clean_db, cidc_api)

        records = TissueSlideManifest.read(
            os.path.join(EXAMPLE_DIR, "tissue_slide_manifest.xlsx")
        )
        assert len(records) > 0
        errors = insert_record_batch(records)
        assert len(errors) == 0, "\n".join(str(e) for e in errors)

        shipments = clean_db.query(Shipment).all()
        participants = (
            clean_db.query(Participant).order_by(Participant.cimac_participant_id).all()
        )
        samples = clean_db.query(Sample).all()

        assert len(shipments) == 1
        shipment = shipments[0]
        assert shipment.trial_id == "test_trial"
        assert shipment.manifest_id == "test_prism_trial_id_slide"
        assert shipment.assay_priority == "3"
        assert shipment.assay_type == "IHC"
        assert shipment.receiving_party == "DFCI_Severgnini"
        assert shipment.courier == "USPS"
        assert shipment.tracking_number == "TrackN"
        assert shipment.account_number == "AccN"
        assert shipment.shipping_condition == "Not Reported"
        assert shipment.date_shipped == datetime(2001, 10, 10).date()
        assert shipment.date_received == datetime(2002, 10, 10).date()
        assert (
            shipment.quality_of_shipment
            == "Specimen shipment received in poor condition"
        )
        assert shipment.ship_from == "ship from"
        assert shipment.ship_to == "ship to"

        assert shipment.json_data["assay_priority"] == "3"
        assert shipment.json_data["assay_type"] == "IHC"
        assert shipment.json_data["receiving_party"] == "DFCI_Severgnini"
        assert shipment.json_data["courier"] == "USPS"
        assert shipment.json_data["tracking_number"] == "TrackN"
        assert shipment.json_data["account_number"] == "AccN"
        assert shipment.json_data["shipping_condition"] == "Not Reported"
        assert shipment.json_data["date_shipped"] == "20011010"
        assert shipment.json_data["date_received"] == "20021010"
        assert (
            shipment.json_data["quality_of_shipment"]
            == "Specimen shipment received in poor condition"
        )
        assert shipment.json_data["ship_from"] == "ship from"
        assert shipment.json_data["ship_to"] == "ship to"

        assert len(participants) == 2
        for i, partic in enumerate(participants):
            assert partic.trial_id == "test_trial"
            assert partic.cimac_participant_id == f"CTTTP0{i+1}"
            assert partic.trial_participant_id == f"TTTP0{i+1}"
            assert partic.json_data["trial_participant_id"] == f"TTTP0{i+1}"
            if i == 0:
                assert partic.cohort_name == "Arm_Z"
            else:  # i == 1
                assert partic.cohort_name == "Arm_A"

        assert len(samples) == 4
        for i, sample in enumerate(sorted(samples, key=lambda s: s.cimac_id)):
            partic = participants[i // 3]

            assert sample.trial_id == "test_trial"
            assert sample.cimac_id == f"{partic.cimac_participant_id}A{i%3 +1}.00"
            assert sample.cimac_participant_id == partic.cimac_participant_id
            assert (
                sample.collection_event_name == "Baseline"
                if i % 3 + 1 != 2
                else "Pre_Day_1_Cycle_2"
            )
            assert sample.manifest_id == shipment.manifest_id
            assert sample.shipping_entry_number == i + 1
            assert sample.box_number == "2"
            assert sample.parent_sample_id == f"TRIALGROUP {i+1}"
            assert sample.processed_sample_id == "BIOBANK 1"
            assert sample.sample_location == f"A{i+1}"
            assert sample.type_of_sample == "Tumor Tissue"
            assert (
                sample.type_of_tumor_sample == "Metastatic Tumor"
                if i < 2
                else "Primary Tumor"
            )
            assert sample.sample_collection_procedure == "Core Biopsy"
            assert sample.core_number == 1
            assert (
                sample.processed_sample_type == "Fixed Slide"
                if i < 2
                else "H&E-Stained Fixed Tissue Slide Specimen"
            )
            assert sample.processed_sample_quantity in ((4, 4.0) if i < 2 else (1, 1.0))
            assert sample.material_used in ((3, 3.0) if i < 2 else (1, 1.0))
            assert sample.material_remaining in ((1, 1.0) if i < 2 else (0, 0.0))
            assert sample.material_storage_condition == "RT"
            assert sample.quality_of_sample == "Pass"
            assert sample.sample_replacement == "Replacement Not Requested"
            assert sample.residual_sample_use == "Not Reported"
            assert sample.intended_assay == shipment.assay_type

            # deprecated
            assert sample.json_data["shipping_entry_number"] == str(i + 1)
            assert sample.json_data["box_number"] == "2"
            assert sample.json_data["parent_sample_id"] == f"TRIALGROUP {i+1}"
            assert sample.json_data["processed_sample_id"] == "BIOBANK 1"
            assert sample.json_data["sample_location"] == f"A{i+1}"
            assert sample.json_data["type_of_sample"] == "Tumor Tissue"
            assert (
                sample.json_data["type_of_tumor_sample"] == "Metastatic Tumor"
                if i < 2
                else "Primary Tumor"
            )
            assert sample.json_data["sample_collection_procedure"] == "Core Biopsy"
            assert sample.json_data["core_number"] == str(1)
            assert (
                sample.json_data["processed_sample_type"] == "Fixed Slide"
                if i < 2
                else "H&E-Stained Fixed Tissue Slide Specimen"
            )
            assert sample.json_data["processed_sample_quantity"] in (
                (str(4), str(4.0)) if i < 2 else (str(1), str(1.0))
            )
            assert sample.json_data["material_used"] in (
                (str(3), str(3.0)) if i < 2 else (str(1), str(1.0))
            )
            assert sample.json_data["material_remaining"] in (
                (str(1), str(1.0)) if i < 2 else (str(0), str(0.0))
            )
            assert sample.json_data["material_storage_condition"] == "RT"
            assert sample.json_data["quality_of_sample"] == "Pass"
            assert sample.json_data["sample_replacement"] == "Replacement Not Requested"
            assert sample.json_data["residual_sample_use"] == "Not Reported"

            if i == 0:
                assert sample.fixation_stabilization_type == "Archival FFPE"
                assert (
                    sample.json_data["fixation_stabilization_type"] == "Archival FFPE"
                )
            elif i == 1:
                assert (
                    sample.fixation_stabilization_type
                    == "Formalin-Fixed Paraffin-Embedded"
                )
                assert (
                    sample.json_data["fixation_stabilization_type"]
                    == "Formalin-Fixed Paraffin-Embedded"
                )
            elif i == 2:
                assert (
                    sample.fixation_stabilization_type
                    == "Formalin-Fixed Paraffin-Embedded"
                )
                assert (
                    sample.json_data["fixation_stabilization_type"]
                    == "Formalin-Fixed Paraffin-Embedded"
                )
            else:  # i == 3
                assert sample.fixation_stabilization_type == "Not Reported"
                assert sample.json_data["fixation_stabilization_type"] == "Not Reported"
