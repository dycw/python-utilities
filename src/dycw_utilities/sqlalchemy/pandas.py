from typing import Any

from sqlalchemy.engine import Engine


def insert_dataframe(df: DataFrame, table: Any, engine: Engine, /) -> None:
    """Insert a DataFrame into a database."""
