from pathlib import Path
from typing import Any

from beartype import beartype
from hypothesis.strategies import SearchStrategy
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from utilities.hypothesis import draw_and_map
from utilities.hypothesis.tempfile import temp_paths
from utilities.sqlalchemy import create_engine


@beartype
def sqlite_engines(
    *, metadata: MetaData | None = None, base: Any = None
) -> SearchStrategy[Engine]:
    """Strategy for generating SQLite engines."""

    return draw_and_map(
        _draw_sqlite_engines, temp_paths(), metadata=metadata, base=base
    )


@beartype
def _draw_sqlite_engines(
    temp_path: Path, /, *, metadata: MetaData | None = None, base: Any = None
) -> Engine:
    path = temp_path.joinpath("db.sqlite")
    engine = create_engine("sqlite", database=path.as_posix())
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_path to the engine, so as to keep it alive
    engine.temp_path = temp_path  # type: ignore

    return engine
