from typing import Any

from beartype import beartype
from luigi import Parameter
from luigi import Target
from sqlalchemy import Engine
from sqlalchemy import Select
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.exc import NoSuchTableError

from utilities.sqlalchemy import get_table_name
from utilities.sqlalchemy import redirect_to_no_such_table_error


class DatabaseTarget(Target):
    """A target point to a set of rows in a database."""

    @beartype
    def __init__(self, sel: Select, engine: Engine, /) -> None:
        super().__init__()
        self._sel = sel.limit(1)
        self._engine = engine

    @beartype
    def exists(self) -> bool:  # noqa: D102
        try:
            with self._engine.begin() as conn:
                res = conn.execute(self._sel).one_or_none()
        except DatabaseError as error:
            try:
                redirect_to_no_such_table_error(self._engine, error)
            except NoSuchTableError:
                return False
        else:
            return res is not None


class EngineParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy engine."""

    @beartype
    def normalize(self, engine: Engine, /) -> Engine:
        """Normalize an `Engine` argument."""
        return engine

    @beartype
    def parse(self, engine: str, /) -> Engine:
        """Parse an `Engine` argument."""
        return create_engine(engine)

    @beartype
    def serialize(self, engine: Engine, /) -> str:
        """Serialize an `Engine` argument."""
        return engine.url.render_as_string()


class TableParameter(Parameter):
    """Parameter taking the value of a SQLAlchemy table."""

    @beartype
    def normalize(self, table_or_model: Any, /) -> Any:
        """Normalize a `Table` or model argument."""
        return table_or_model

    @beartype
    def serialize(self, table_or_model: Any, /) -> str:
        """Serialize a `Table` or model argument."""
        return get_table_name(table_or_model)
