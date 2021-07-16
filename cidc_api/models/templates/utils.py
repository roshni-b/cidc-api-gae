from cidc_api.models.templates.file_metadata import UploadStatus
from collections import defaultdict
from typing import List, OrderedDict, Type

from sqlalchemy.orm import Session

from .model_core import MetadataModel, with_default_session


def _get_global_insertion_order() -> List[MetadataModel]:
    """
    Produce an ordering of all metadata model types based on foreign key dependencies
    between models. For a given model, all models it depends on are guaranteed to
    appear before it in this list.
    """

    def all_subclasses(cls: Type):
        return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in all_subclasses(c)]
        )

    from .file_metadata import Upload

    models = all_subclasses(MetadataModel)

    # Build a dictionary mapping table names to model classes -
    # we use this below to look up the model classes associated with
    # a given foreign key.
    table_to_model = {}
    for m in models:
        if m.__tablename__ not in table_to_model:
            table_to_model[m.__tablename__] = [m]
        else:
            table_to_model[m.__tablename__].append(m)

    # Build two graphs representing foreign-key relationships between models:
    # - fks_to_parents, mapping models to the set of models they depend on
    # - parents_to_fks, mapping models to the set of models that depend on them
    fks_to_parents = defaultdict(set)
    parent_to_fks = defaultdict(set)
    for model in models:
        for fk in model.__table__.foreign_keys:
            fk_models = table_to_model.get(fk.column.table.name, [])
            for fk_model in fk_models:
                parent_to_fks[model].add(fk_model)
                fks_to_parents[fk_model].add(model)

    # Topologically sort the dependency graph to produce a valid insertion order
    # using Kahn's algorithm: https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
    ordered_models = []
    depless_models = set(
        model for model in models if len(model.__table__.foreign_keys) == 0
    )
    while len(depless_models) > 0:
        model = depless_models.pop()
        ordered_models.append(model)
        for fk_model in fks_to_parents[model]:
            fks = parent_to_fks[fk_model]
            fks.remove(model)
            if len(fks) == 0:
                depless_models.add(fk_model)

    return ordered_models


@with_default_session
def insert_record_batch(
    ordered_records: OrderedDict[Type, List[MetadataModel]],
    session: Session,
    dry_run: bool = False,
) -> List[Exception]:
    """
    Try to insert the given list of models into the database in a single transaction,
    rolling back and returning a list of errors if any are encountered. If `dry_run` is `True`,
    rollback the transaction regardless of whether any errors are encountered.
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
                for b in model.__bases__
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
            + [b.__tablename__ for b in k.__bases__ if hasattr(b, "__tablename__")]
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
                print(e)
                errors.append(e)

        # flush these records to generate db-derived values
        # in case they're needed for later fk's
        session.flush()

    if dry_run or len(errors):
        session.rollback()
    else:
        session.commit()
    session.close()

    return errors
