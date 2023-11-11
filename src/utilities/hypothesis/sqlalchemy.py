from __future__ import annotations

from typing import Any, cast

from hypothesis.strategies import DrawFn, composite
from sqlalchemy import Engine, MetaData

from utilities.sqlalchemy import create_engine

from .hypothesis import temp_paths  # noqa: TID252


@composite
def sqlite_engines(
    _draw: DrawFn, /, *, metadata: MetaData | None = None, base: Any = None
) -> Engine:
    """Strategy for generating SQLite engines."""
    temp_path = _draw(temp_paths())
    path = temp_path.joinpath("db.sqlite")
    engine = create_engine("sqlite", database=str(path))
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_path to the engine, so as to keep it alive
    cast(Any, engine).temp_path = temp_path

    return engine


__all__ = [
    "sqlite_engines",
]
