from typing import Any

from beartype import beartype
from luigi import Parameter
from sqlalchemy.engine import Engine

from utilities.sqlalchemy import get_table_name


class EngineParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy engine."""

    @beartype
    def normalize(self, engine: Engine, /) -> Engine:
        return engine

    @beartype
    def serialize(self, engine: Engine, /) -> str:
        return engine.url.render_as_string()


class TableParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy engine."""

    @beartype
    def normalize(self, table_or_model: Any, /) -> Any:
        return table_or_model

    @beartype
    def serialize(self, table_or_model: Any, /) -> str:
        return get_table_name(table_or_model)
