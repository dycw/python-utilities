from __future__ import annotations

from typing import Any

from luigi import Parameter, Target
from sqlalchemy import Engine, Select, create_engine
from typing_extensions import override

from utilities.sqlalchemy import (
    TableDoesNotExistError,
    get_table_name,
    redirect_table_does_not_exist,
)


class DatabaseTarget(Target):
    """A target point to a set of rows in a database."""

    def __init__(self, sel: Select[Any], engine: Engine, /) -> None:
        super().__init__()
        self._sel = sel.limit(1)
        self._engine = engine

    def exists(self) -> bool:  # type: ignore
        engine = self._engine
        try:
            with redirect_table_does_not_exist(engine), engine.begin() as conn:
                res = conn.execute(self._sel).one_or_none()
        except TableDoesNotExistError:
            return False
        else:
            return res is not None


class EngineParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy engine."""

    @override
    def normalize(self, x: Engine) -> Engine:
        """Normalize an `Engine` argument."""
        return x

    @override
    def parse(self, x: str) -> Engine:
        """Parse an `Engine` argument."""
        return create_engine(x)

    @override
    def serialize(self, x: Engine) -> str:
        """Serialize an `Engine` argument."""
        return x.url.render_as_string()


class TableParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy table."""

    @override
    def normalize(self, x: Any) -> Any:
        """Normalize a `Table` or model argument."""
        return x

    @override
    def serialize(self, x: Any) -> str:
        """Serialize a `Table` or model argument."""
        return get_table_name(x)


__all__ = ["DatabaseTarget", "EngineParameter", "TableParameter"]
