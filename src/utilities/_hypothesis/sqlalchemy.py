from __future__ import annotations

from typing import Any, cast

from hypothesis.strategies import DrawFn, composite
from sqlalchemy import Engine, MetaData

from utilities._hypothesis.common import temp_paths
from utilities.pathvalidate import valid_path
from utilities.sqlalchemy import create_engine


@composite
def sqlite_engines(
    _draw: DrawFn, /, *, metadata: MetaData | None = None, base: Any = None
) -> Engine:
    """Strategy for generating SQLite engines."""
    temp_path = _draw(temp_paths())
    path = valid_path(temp_path, "db.sqlite")
    engine = create_engine("sqlite", database=str(path))
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_path to the engine, so as to keep it alive
    cast(Any, engine).temp_path = temp_path

    return engine


__all__ = ["sqlite_engines"]
