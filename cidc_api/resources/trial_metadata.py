from cidc_api.models.templates.trial_metadata import Cohort, CollectionEvent
from collections import OrderedDict
from flask import Blueprint, jsonify
from webargs import fields
from werkzeug.exceptions import BadRequest, NotFound

from ..shared.auth import get_current_user, requires_auth
from ..models import (
    CIDCRole,
    ClinicalTrial,
    in_single_transaction,
    insert_record_batch,
    IntegrityError,
    remove_record_batch,
    TrialMetadata,
    TrialMetadataSchema,
    TrialMetadataListSchema,
)
from ..models.templates.sync_schemas import _get_all_values
from ..shared.rest_utils import (
    with_lookup,
    marshal_response,
    unmarshal_request,
    use_args_with_pagination,
)

trial_metadata_bp = Blueprint("trials", __name__)

trial_metadata_schema = TrialMetadataSchema()
trial_metadata_list_schema = TrialMetadataListSchema()
partial_trial_metadata_schema = TrialMetadataSchema(partial=True)

trial_modifier_roles = [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]


bundle_argname = "include_file_bundles"
counts_argname = "include_counts"
trial_filter_schema = {
    bundle_argname: fields.Bool(),
    counts_argname: fields.Bool(),
    "trial_ids": fields.DelimitedList(fields.Str),
}


@trial_metadata_bp.route("/", methods=["GET"])
@requires_auth("trial_metadata")
@use_args_with_pagination(trial_filter_schema, trial_metadata_schema)
@marshal_response(trial_metadata_list_schema)
def list_trial_metadata(args, pagination_args):
    """List all trial metadata records."""
    user = get_current_user()
    trials = TrialMetadata.list(
        include_file_bundles=args.pop(bundle_argname, False),
        include_counts=args.pop(counts_argname, False),
        filter_=TrialMetadata.build_trial_filter(user=user, **args),
        **pagination_args,
    )
    count = TrialMetadata.count()

    return {"_items": trials, "_meta": {"total": count}}


@trial_metadata_bp.route("/", methods=["POST"])
@requires_auth("trial_metadata_item", trial_modifier_roles)
@unmarshal_request(trial_metadata_schema, "trial")
@marshal_response(trial_metadata_schema, 201)
def create_trial_metadata(trial):
    """Create a new trial metadata record."""
    try:
        # metadata was already validated by unmarshal_request
        trial.insert(validate_metadata=False)
    except IntegrityError as e:
        raise BadRequest(str(e.orig))

    # relational hook validates by "upsert"
    errs = insert_record_batch(
        {
            ClinicalTrial: [
                ClinicalTrial(
                    # borrowing from sync_schemas since we're already unmarshalling
                    # better handled by a UI change to {key: value}, then old=**request.json
                    **_get_all_values(target=ClinicalTrial, old=trial.metadata_json)
                )
            ]
        }
    )
    if errs:
        raise BadRequest(
            f"Errors in relational add: {len(errs)}\n" + "\n".join(str(e) for e in errs)
        )

    return trial


@trial_metadata_bp.route("/summaries", methods=["GET"])
@requires_auth("trial_metadata_summaries")
def get_trial_metadata_summaries():
    """Get summaries of all trial metadata in the database"""
    return jsonify(TrialMetadata.get_summaries())


@trial_metadata_bp.route("/<string:trial>", methods=["GET"])
@requires_auth("trial_metadata_item", trial_modifier_roles)
@with_lookup(TrialMetadata, "trial", find_func=TrialMetadata.find_by_trial_id)
@marshal_response(trial_metadata_schema)
def get_trial_metadata_by_trial_id(trial):
    """Get one trial metadata record by trial identifier."""
    # this is not user-input due to @with_lookup, so safe to return
    return trial


@trial_metadata_bp.route("/<string:trial>", methods=["PATCH"])
@requires_auth("trial_metadata_item", trial_modifier_roles)
@with_lookup(
    TrialMetadata, "trial", check_etag=True, find_func=TrialMetadata.find_by_trial_id
)
@unmarshal_request(partial_trial_metadata_schema, "trial_updates", load_sqla=False)
@marshal_response(trial_metadata_schema, 200)
def update_trial_metadata_by_trial_id(trial, trial_updates):
    """Update an existing trial metadata record by trial_id."""
    # Block updates to protected metadata JSON fields
    metadata_updates = trial_updates.get("metadata_json")
    if trial.metadata_json or metadata_updates:
        for field in TrialMetadata.PROTECTED_FIELDS:
            if trial.metadata_json.get(field) != metadata_updates.get(field):
                raise BadRequest(
                    f"updating metadata_json['{field}'] via the API is prohibited"
                )

    trial.update(changes=trial_updates)

    # should be moved to @with_lookup which raises NotFound internally
    db_trial = ClinicalTrial.get_by_id(trial.trial_id)
    if db_trial is None:
        raise NotFound(f"Trial {trial.trial_id} not found in relational tables.")

    # relational hook validates by "upsert"
    # also might need to remove things too
    ordered_records_to_add, records_to_remove = OrderedDict(), []
    ordered_records_to_add[ClinicalTrial] = [
        ClinicalTrial(
            # borrowing from sync_schemas since we're already unmarshalling
            # better handled by a UI change to {key: value}, then old=**request.json
            **_get_all_values(target=ClinicalTrial, old=trial.metadata_json)
        )
    ]

    # currently need to compare metadata to look for changes in other tables
    # as UI always returns full list of all values
    for json_key, db_key, model in [
        ("allowed_cohort_names", "cohort_name", Cohort),
        ("allowed_collection_event_names", "event_name", CollectionEvent),
    ]:
        old, new = (
            set(getattr(db_trial, json_key)),
            set(metadata_updates.get(json_key, [])),
        )
        if old != new:
            to_add, to_remove = new.difference(old), old.difference(new)
            if len(to_add):
                ordered_records_to_add[model] = [
                    model(**{"trial_id": trial.trial_id, db_key: value})
                    for value in to_add
                ]
            if len(to_remove):
                for value in to_remove:
                    record = model.get_by_id(trial.trial_id, value)
                    if record:
                        records_to_remove.append(record)

    errs = in_single_transaction(
        {
            insert_record_batch: {"ordered_records": ordered_records_to_add},
            remove_record_batch: {"records": records_to_remove},
        }
    )
    if errs:
        raise BadRequest(
            f"Errors in relational add: {len(errs)}\n" + "\n".join(str(e) for e in errs)
        )

    return trial
