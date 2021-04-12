from unittest.mock import MagicMock, call

import pytest
from cidc_schemas.migrations import MigrationResult

import cidc_api.models.migrations as migrations
from cidc_api.models.migrations import (
    RollbackableQueue,
    PieceOfWork,
    Session,
    run_metadata_migration,
)
from cidc_api.models import UploadJobs, TrialMetadata, DownloadableFiles
from cidc_api.config.settings import GOOGLE_DATA_BUCKET


def test_rollbackable_queue():
    # Queue works on a well-behaved example
    tasks = RollbackableQueue()
    state = {"a": 1, "b": 2}
    orig = state.copy()
    t1 = PieceOfWork(lambda: state.pop("a"), lambda: state.__setitem__("a", 1))
    t2 = PieceOfWork(
        lambda: state.__setitem__("b", 1), lambda: state.__setitem__("b", 2)
    )
    tasks.schedule(t1)
    tasks.schedule(t2)
    tasks.run_all()
    assert state == {"b": 1}

    # Queue rolls back when a task errors
    state = orig.copy()
    t3_keyerror = PieceOfWork(lambda: state["foo"], lambda: state.__setitem__("c", 3))
    tasks.schedule(t3_keyerror)
    with pytest.raises(KeyError):
        tasks.run_all()
    assert state == orig

    # Ensure rollback only calls `undo` once.
    tasks = RollbackableQueue()
    t1 = MagicMock()
    t2 = MagicMock()
    t2.side_effect = KeyError
    tasks.schedule(PieceOfWork(t1, t1))
    tasks.schedule(PieceOfWork(t2, t2))
    with pytest.raises(KeyError):
        tasks.run_all()
    assert len(t1.call_args_list) == 2
    t2.assert_called_once()


@pytest.mark.parametrize("use_upload_jobs_table", [(True,), (False,)])
def test_migrations_failures(use_upload_jobs_table, monkeypatch):
    """Test that changes get rolled back in potential failure scenarios."""
    # Mock alembic
    monkeypatch.setattr(migrations, "op", MagicMock())

    # Mock sqlalchemy
    mock_session_builder = MagicMock()
    mock_session = MagicMock()
    mock_session_builder.return_value = mock_session
    monkeypatch.setattr(migrations, "Session", mock_session_builder)

    # Mock cidc_api and prism functions
    trial_record = MagicMock()
    select_trials = MagicMock()
    select_trials.return_value = [trial_record]
    monkeypatch.setattr(migrations, "_select_trials", select_trials)

    df_record = MagicMock()
    select_df = MagicMock()
    select_df.return_value = df_record
    monkeypatch.setattr(DownloadableFiles, "get_by_object_url", select_df)

    select_assay_uploads = MagicMock()
    select_assay_uploads.return_value = [
        UploadJobs(gcs_file_map={"a_old_url/ts": "foo", "b_old_url/ts": "bar"})
    ]
    monkeypatch.setattr(
        migrations, "_select_successful_assay_uploads", select_assay_uploads
    )

    if not use_upload_jobs_table:
        select_manifest_uploads = MagicMock()
        select_manifest_uploads.return_value = [MagicMock()]
        monkeypatch.setattr(
            migrations, "_select_manifest_uploads", select_manifest_uploads
        )

    new_metadata = {
        "some_assay": {
            "extra": "metadata",
            "files": [{"upload_placeholder": "foo"}, {"upload_placeholder": "bar"}],
        }
    }
    mock_migration = MagicMock()
    mock_migration.return_value = MigrationResult(
        new_metadata,
        {
            "a_old_url": {"object_url": "a_new_url", "upload_placeholder": "foo"},
            "b_old_url": {"object_url": "b_new_url", "upload_placeholder": "bar"},
        },
    )

    rename_gcs_obj = MagicMock()
    monkeypatch.setattr(migrations, "rename_gcs_blob", rename_gcs_obj)

    def reset_mocks():
        rename_gcs_obj.reset_mock()
        mock_session.commit.reset_mock()
        mock_session.rollback.reset_mock()
        mock_session.close.reset_mock()

    # GCS failure config
    rename_gcs_obj.side_effect = [None, Exception("gcs failure"), None]

    with pytest.raises(Exception, match="gcs failure"):
        run_metadata_migration(mock_migration, use_upload_jobs_table)
    # Called 3 times - task 1 succeeds, task 2 fails, task 1 rolls back
    assert len(rename_gcs_obj.call_args_list) == 3
    mock_session.commit.assert_not_called()
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()

    rename_gcs_obj.side_effect = None
    reset_mocks()

    # SQL failure
    select_assay_uploads.side_effect = Exception("sql failure")

    with pytest.raises(Exception, match="sql failure"):
        run_metadata_migration(mock_migration, use_upload_jobs_table)
    mock_session.commit.assert_not_called()
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
    # Ensure no GCS operations were carried out
    rename_gcs_obj.assert_not_called()

    reset_mocks()

    # No failures
    select_assay_uploads.side_effect = None
    run_metadata_migration(mock_migration, use_upload_jobs_table)
    # Ensure we updated trials as expected
    trial_record.safely_set_metadata_json.assert_called_with(new_metadata)
    # Ensure we updated files as expected
    assert df_record.object_url == "b_new_url"
    assert df_record.additional_metadata == {"some_assay.extra": "metadata"}
    # Ensure we renamed the right objects
    assert rename_gcs_obj.call_args_list == [
        call(GOOGLE_DATA_BUCKET, "a_old_url", "a_new_url"),
        call(GOOGLE_DATA_BUCKET, "b_old_url", "b_new_url"),
    ]


def test_upload_job_selection():
    """Check that the upload_job selection helper functions selects from the correct table"""
    session = MagicMock()
    session.query.return_value = MagicMock()

    migrations._select_successful_assay_uploads(True, session)
    session.query.assert_called_with(UploadJobs)

    session.query.clear_mock()

    migrations._select_successful_assay_uploads(False, session)
    session.query.assert_called_with(migrations.AssayUploads)

    session.query.clear_mock()

    migrations._select_manifest_uploads(True, session)
    session.query.assert_called_with(UploadJobs)

    session.query.clear_mock()

    migrations._select_manifest_uploads(False, session)
    session.query.assert_called_with(migrations.ManifestUploads)
