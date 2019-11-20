import os
from contextlib import contextmanager
from functools import partial
from typing import Callable, List, NamedTuple, Any, Tuple

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm.session import Session
from google.cloud import storage

from cidc_api.models import (
    TrialMetadata,
    DownloadableFiles,
    AssayUploads,
    AssayUploadStatus,
    ManifestUploads,
)
from cidc_api.config.settings import GOOGLE_DATA_BUCKET, GOOGLE_UPLOAD_BUCKET
from cidc_schemas.migrations import MigrationResult
from cidc_schemas.prism import _get_uuid_info


class PieceOfWork(NamedTuple):
    do: Callable[[], None]
    undo: Callable[[], None]


class RollbackableQueue:
    """A collection of reversible pieces-of-work."""

    def __init__(self):
        self.tasks = []
        self.done = set()

    def schedule(self, task: PieceOfWork):
        """Add a task to the task queue."""
        self.tasks.append(task)

    def run_all(self):
        """
        Attempt to run all tasks in the queue, rolling back
        successfully completed tasks if a subsequent task fails.
        """
        for i, task in enumerate(self.tasks):
            try:
                task.do()
                self.done.add(i)
            except:
                self.rollback()
                raise

    def rollback(self):
        """
        Undo any work that has been carried out.
        """
        for i, task in enumerate(self.tasks):
            if i in self.done:
                task.undo()
                self.done.discard(i)


@contextmanager
def migration_session():
    session = Session(bind=op.get_bind())
    task_queue = RollbackableQueue()

    try:
        yield session, task_queue
        print("Commiting SQL session...")
        session.commit()
        print("Session commit succeeded.")
    except:
        print("Encountered exception. Running SQL rollback...")
        session.rollback()
        print("SQL rollback succeeded.")
        if task_queue:
            try:
                print("Running GCS rollback...")
                task_queue.rollback()
                print("GCS rollback succeeded.")
            except Exception as e:
                print(f"GCS rollback failed: {e.__class__}: {e}")
        raise
    finally:
        session.close()


def run_metadata_migration(metadata_migration: Callable[[dict], MigrationResult]):
    """Migrate trial metadata, upload job patches, and downloadable files according to `metadata_migration`"""
    with migration_session() as (session, task_queue):
        _run_metadata_migration(metadata_migration, task_queue, session)


def _select_trials(session: Session) -> List[TrialMetadata]:
    return session.query(TrialMetadata).with_for_update().all()


def _select_successful_assay_uploads(session: Session) -> List[AssayUploads]:
    return (
        session.query(AssayUploads)
        .filter_by(status=AssayUploadStatus.MERGE_COMPLETED.value)
        .with_for_update()
        .all()
    )


def _select_manifest_uploads(session: Session) -> List[ManifestUploads]:
    return session.query(ManifestUploads).with_for_update().all()


def _run_metadata_migration(
    metadata_migration: Callable[[dict], MigrationResult],
    gcs_tasks: RollbackableQueue,
    session: Session,
):
    # Migrate all trial records
    trials = _select_trials(session)
    for trial in trials:
        print(f"Running metadata migration for trial: {trial.trial_id}")
        migration = metadata_migration(trial.metadata_json)

        # Update the trial metadata object
        trial.metadata_json = migration.result

        # Update the relevant downloadable files and GCS objects
        for old_gcs_uri, artifact in migration.file_updates.items():
            print(f"Updating GCS and artifact info for {old_gcs_uri}: {artifact}")
            # Update the downloadable file associated with this blob
            df = DownloadableFiles.get_by_object_url(old_gcs_uri, session=session)
            for column, value in artifact.items():
                if hasattr(df, column):
                    setattr(df, column, value)

            # Regenerate additional metadata from the migrated clinical trial
            # metadata object.
            print(
                f"Regenerating additional metadata for artifact with uuid {artifact['upload_placeholder']}"
            )
            df.additional_metadata = _get_uuid_info(
                migration.result, artifact["upload_placeholder"]
            )[1]

            # If the GCS URI has changed, rename the blob
            new_gcs_uri = artifact["object_url"]
            if old_gcs_uri != new_gcs_uri:
                print(
                    f"Encountered GCS data bucket artifact URI to update: {old_gcs_uri}"
                )
                renamer = PieceOfWork(
                    partial(
                        rename_gcs_blob, GOOGLE_DATA_BUCKET, old_gcs_uri, new_gcs_uri
                    ),
                    partial(
                        rename_gcs_blob, GOOGLE_DATA_BUCKET, new_gcs_uri, old_gcs_uri
                    ),
                )
                gcs_tasks.schedule(renamer)

    # Migrate all assay upload successes
    successful_assay_uploads = _select_successful_assay_uploads(session)
    for upload in successful_assay_uploads:
        print(f"Running metadata migration for assay upload: {upload.id}")
        migration = metadata_migration(upload.assay_patch)

        # Update the metadata patch
        upload.assay_patch = migration.result

        # Update the GCS URIs of files that were part of this upload
        old_file_map = upload.gcs_file_map
        new_file_map = {}
        for (
            old_upload_uri,
            old_target_uri,
            artifact_uuid,
        ) in upload.upload_uris_with_data_uris_with_uuids():
            upload_timestamp = old_upload_uri[len(old_target_uri) + 1 :]
            if old_target_uri in migration.file_updates:
                new_target_uri = migration.file_updates[old_target_uri]["object_url"]
                if old_target_uri != new_target_uri:
                    print(
                        f"Encountered GCS upload bucket artifact URI to update: {old_upload_uri}"
                    )
                    new_upload_uri = "/".join([new_target_uri, upload_timestamp])
                    renamer = PieceOfWork(
                        partial(
                            rename_gcs_blob,
                            GOOGLE_UPLOAD_BUCKET,
                            old_upload_uri,
                            new_upload_uri,
                        ),
                        partial(
                            rename_gcs_blob,
                            GOOGLE_UPLOAD_BUCKET,
                            new_upload_uri,
                            old_upload_uri,
                        ),
                    )
                    gcs_tasks.schedule(renamer)
                new_file_map[new_upload_uri] = artifact_uuid

        # Update the upload's file map to use new GCS URIs
        upload.gcs_file_map = new_file_map

    # Migrate all manifest records
    manifest_uploads = _select_manifest_uploads(session)
    for upload in manifest_uploads:
        print(f"Running metadata migration for manifest upload: {upload.id}")
        migration = metadata_migration(upload.metadata_patch)

        # Update the metadata patch
        upload.metadata_patch = migration.result

    # Attempt to make GCS updates
    print(f"Running all GCS tasks...")
    gcs_tasks.run_all()
    print(f"GCS tasks succeeded.")


is_testing = os.environ.get("TESTING")


def rename_gcs_blob(bucket, old_name, new_name):
    full_old_uri = f"gs://{bucket}/{old_name}"
    full_new_uri = f"gs://{bucket}/{new_name}"
    message = f"GCS: moving {full_old_uri} to {full_new_uri}"
    if is_testing:
        print(f"SKIPPING: {message}")
        return

    print(message)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    old_blob = bucket.blob(old_name)
    new_blob = bucket.rename_blob(old_blob, new_name)
    return new_blob