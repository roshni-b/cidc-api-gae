__all__ = [
    "_all_bases",
    "_all_subclasses",
    "_get_global_insertion_order",
    "in_single_transaction",
    "insert_record_batch",
    "get_full_template_name",
    "remove_record_batch",
]

from collections import defaultdict
from typing import Any, Callable, Dict, List, OrderedDict, Set, Type

from sqlalchemy.orm import Session

from .file_metadata import Upload
from .model_core import MetadataModel, with_default_session


def _all_bases(cls: Type) -> Set[Type]:
    return set(cls.__bases__).union([s for c in cls.__bases__ for s in _all_bases(c)])


def _all_subclasses(cls: Type) -> Set[Type]:
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _all_subclasses(c)]
    )


def _get_global_insertion_order() -> List[MetadataModel]:
    """
    Produce an ordering of all metadata model types based on foreign key dependencies
    between models. For a given model, all models it depends on are guaranteed to
    appear before it in this list.
    """
    models = _all_subclasses(MetadataModel)

    # Build a dictionary mapping table names to model classes -
    # we use this below to look up the model classes associated with
    # a given foreign key.
    table_to_model = defaultdict(list)
    for m in models:
        table_to_model[m.__tablename__].append(m)

    # Build a graph representing foreign-key relationships,
    # mapping models to the set of models they depend on
    fks_on_model = defaultdict(set)
    fks_to_parents = defaultdict(set)
    for model in models:
        fks_on_model[model] = model.__table__.foreign_keys.union(
            {
                fk
                for b in _all_bases(model)
                if hasattr(b, "__table__")
                for fk in b.__table__.foreign_keys
            }
        )
        for fk in fks_on_model[model]:
            fk_models = table_to_model.get(fk.column.table.name, [])
            for fk_model in fk_models:
                if not issubclass(fk_model, model):
                    fks_to_parents[model].add(fk_model)

                # special handling to make sure all Upload subclasses get merged earlier
                # class inheritance will take care of relations within Upload subclasses
                ## created to prevent Upload from being created after relevant Files that need upload_id
                if issubclass(model, Upload) and not issubclass(fk_model, Upload):
                    for c in _all_subclasses(Upload):
                        if c is not fk_model and not issubclass(fk_model, c):
                            fks_to_parents[c].add(fk_model)

                if issubclass(fk_model, Upload) and not issubclass(model, Upload):
                    for c in _all_subclasses(Upload):
                        if c is not model and not issubclass(c, model):
                            fks_to_parents[model].add(c)

    # Topologically sort the dependency graph to produce a valid insertion order
    # using Kahn's algorithm: https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
    ordered_models = []
    depless_models = set(model for model in models if len(fks_to_parents[model]) == 0)
    while len(depless_models) > 0:
        adding = depless_models.pop()
        ordered_models.append(adding)

        for model, fk_models in fks_to_parents.items():
            if adding in fk_models:
                fks_to_parents[model].remove(adding)

                if len(fks_to_parents[model]) == 0:
                    depless_models.add(model)

        fks_to_parents.pop(adding)

    if len(fks_to_parents) > 0:
        raise Exception(f"Cannot figure out how to insert: {fks_to_parents}")

    return ordered_models


@with_default_session
def insert_record_batch(
    ordered_records: OrderedDict[Type, List[MetadataModel]],
    *,
    dry_run: bool = False,
    hold_commit: bool = False,
    session: Session,
) -> List[Exception]:
    """
    Try to insert the given list of models into the database in a single transaction,
    rolling back and returning a list of errors if any are encountered. If `dry_run` is `True`,
    rollback the transaction regardless of whether any errors are encountered.
    If `hold_commit` is passed, all rollback / commit are ignored.
    """
    errors = []
    for model in ordered_records.keys():
        records = ordered_records[model]

        # set any columns that were left to fill in by fk
        # can only set if there's only a single foreign instance
        # also look at all the superclasses for hidden fk's
        fk_to_check = [fk for fk in model.__table__.foreign_keys]
        fk_to_check.extend(
            [
                fk
                for b in _all_bases(model)
                if hasattr(b, "__table__")
                for fk in b.__table__.foreign_keys
            ]
        )
        for fk, target_class in {
            fk: k
            for k, v in ordered_records.items()
            for fk in fk_to_check
            if len(v) == 1
            and fk.column.table.name
            in [k.__tablename__]
            + [b.__tablename__ for b in _all_bases(k) if hasattr(b, "__tablename__")]
        }.items():
            for n in range(len(records)):
                setattr(
                    records[n],
                    fk.parent.name,
                    getattr(ordered_records[target_class][0], fk.column.name),
                )

        # merge all records into session and keep a copy
        for n, record in enumerate(records):
            try:
                record = session.merge(record)
                ordered_records[model][n] = record
            except Exception as e:
                errors.append(e)

        # flush these records to generate db-derived values
        # in case they're needed for later fk's
        session.flush()

    if hold_commit:
        session.flush()
    elif dry_run or len(errors):
        session.rollback()
    else:
        session.commit()

    return errors


@with_default_session
def remove_record_batch(
    records: List[MetadataModel],
    *,
    dry_run: bool = False,
    hold_commit: bool = False,
    session: Session,
) -> List[Exception]:
    """
    Try to safely remove the given list of models from the database in a single transaction,
    rolling back and returning a list of errors if any are encountered. If `dry_run` is `True`,
    rollback the transaction regardless of whether any errors are encountered.
    If `hold_commit` is passed, all rollback / commit are ignored.
    """
    errors = []

    # merge all records into session and keep a copy
    for record in records:
        if record is None:
            continue

        try:
            record = session.delete(record)
        except Exception as e:
            errors.append(e)

    if hold_commit:
        session.flush()
    elif dry_run or len(errors):
        session.rollback()
    else:
        session.commit()

    return errors


@with_default_session
def in_single_transaction(
    calls: OrderedDict[Callable[[Any], List[Exception]], Dict[str, Any]],
    *,
    dry_run: bool = False,
    session: Session,
) -> List[Exception]:
    """Given an arbitrary set of calls, make all of them in a single transaction,
    rolling back and returning a list of errors if any are encountered. If `dry_run` is `True`,
    rollback the transaction regardless of whether any errors are encountered.
    """
    errors = []
    for func, kwargs in calls.items():
        kwargs.update({"session": session, "hold_commit": True})
        errors.extend(func(**kwargs))
        session.flush()

    # no hold_commit here because we need to close the transaction
    if dry_run or len(errors):
        session.rollback()
    else:
        session.commit()

    return errors


def get_full_template_name(temp_name: str):
    """Returns a fully qualified name of a metadata template in the form '<assay_name>'
    where <assay_name> is snake_case
    """
    # as AbbrevCamelCasePurpose
    for purpose in ["Assay", "Analysis", "Manifest"]:
        if temp_name.endswith(purpose):
            temp_name = temp_name[: -len(purpose)]
            break

    return "".join(
        [
            "_" + i if i != j and n else i
            for n, (i, j) in enumerate(zip(temp_name, temp_name.lower()))
        ]
    ).lower()  # as abbrev_camel_case
