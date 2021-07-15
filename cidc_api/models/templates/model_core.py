from collections import defaultdict
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Type
from typing import OrderedDict as OrderedDict_Type

from flask import current_app
from sqlalchemy import Column
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import table

from cidc_api.config.db import BaseModel

identity = lambda v, _: v
cimac_id_to_cimac_participant_id = lambda cimac_id, _: cimac_id[:7]

get_property = lambda prop: lambda _, context: (
    [v for k, v in context.items() if k.name == prop] + [None]
)[0]


def with_default_session(f):
    """
    For some `f` expecting a database session instance as a keyword argument,
    set the default value of the session keyword argument to the current app's
    database driver's session. We need to do this in a decorator rather than
    inline in the function definition because the current app is only available
    once the app is running and an application context has been pushed.
    """

    @wraps(f)
    def wrapped(*args, **kwargs):
        if "session" not in kwargs:
            kwargs["session"] = current_app.extensions["sqlalchemy"].db.session
        return f(*args, **kwargs)

    return wrapped


class MetadataModel(BaseModel):
    __abstract__ = True

    def primary_key_values(self) -> Optional[Tuple[Any]]:
        return tuple(self.primary_key_map().values())

    def primary_key_map(self) -> Optional[Dict[str, Any]]:
        columns_to_check = [c for c in self.__table__.columns]
        for c in type(self).__bases__:
            if hasattr(c, "__table__"):
                columns_to_check.extend(c.__table__.columns)

        primary_key_values = {}
        for column in columns_to_check:
            if column.primary_key:
                value = getattr(self, column.name)
                primary_key_values[column] = value

        if all(v is None for v in primary_key_values):
            return None  # special value

        return primary_key_values

    def unique_field_values(self) -> Optional[Tuple[Any]]:
        columns_to_check = [c for c in self.__table__.columns]
        for c in type(self).__bases__:
            if hasattr(c, "__table__"):
                columns_to_check.extend(c.__table__.columns)

        unique_field_values = []
        for column in columns_to_check:
            # column.primary_key == True for 1+ column guaranteed
            if column.unique or column.primary_key:
                value = getattr(self, column.name)
                unique_field_values.append(value)

        if all(v is None for v in unique_field_values):
            return None  # special value

        return tuple(unique_field_values)

    def merge(self, other):
        """Merge column values from other into self, raising an error on conflicts between non-null fields."""
        if self.__class__ != other.__class__:
            raise Exception(
                f"cannot merge {self.__class__} instance with {other.__class__} instance"
            )

        for column in self.__table__.columns + [
            c
            for b in type(self).__bases__
            if hasattr(b, "__table__")
            for c in b.__table__.columns
        ]:
            current = getattr(self, column.name)
            incoming = getattr(other, column.name)
            if current is None:
                setattr(self, column.name, incoming)
            elif incoming is not None and current != incoming:
                raise Exception(
                    f"found conflicting values for {self.__tablename__}.{column.name}: {current}!={other}"
                )

    @classmethod
    @with_default_session
    def get_by_id(cls, *id, session: Session):
        with current_app.app_context():
            ret = session.query(cls).get(id)
        return ret
