from flask import Blueprint, jsonify, request
from webargs import fields
from werkzeug.exceptions import BadRequest

from ..config.logging import get_logger
from ..shared.auth import get_current_user, requires_auth
from ..models import (
    CIDCRole,
    ClinicalTrial,
    insert_record_batch,
    IntegrityError,
    TrialMetadata,
    TrialMetadataSchema,
    TrialMetadataListSchema,
)
from ..models.templates.csms_api import (
    insert_manifest_from_json,
    insert_manifest_into_blob,
)
from ..models.templates.sync_schemas import (
    update_trial_from_metadata_json,
    _get_all_values,
)
from ..shared.rest_utils import (
    with_lookup,
    marshal_response,
    unmarshal_request,
    use_args_with_pagination,
)

logger = get_logger(__name__)

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

    # relational hook validates by insert
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

    errs = update_trial_from_metadata_json(trial.metadata_json)
    if errs:
        raise BadRequest(
            f"Errors in relational add: {len(errs)}\n" + "\n".join(str(e) for e in errs)
        )

    return trial


@trial_metadata_bp.route("/new_manifest", methods=["POST"])
@requires_auth("new_manifest", [CIDCRole.ADMIN.value])
def add_new_manifest_from_json():
    try:
        # relational hook
        insert_manifest_from_json(request.json)

        # schemas JSON blob hook
        insert_manifest_into_blob(request.json)

    except Exception as e:
        res = jsonify(error=str(e))
        res.status_code = 500
    else:
        res = jsonify(status="success")
        res.status_code = 200
    finally:
        return res
