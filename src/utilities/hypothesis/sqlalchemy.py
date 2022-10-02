from typing import Any

from hypothesis.strategies import SearchStrategy
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from utilities.hypothesis import draw_and_map
from utilities.hypothesis.tempfile import temp_dirs
from utilities.sqlalchemy import create_engine
from utilities.tempfile import TemporaryDirectory
from beartype import beartype


@beartype
def sqlite_engines(
    *, metadata: MetaData | None = None, base: Any = None
) -> SearchStrategy[Engine]:
    """Strategy for generating SQLite engines."""

    return draw_and_map(
        _draw_sqlite_engines, temp_dirs(), metadata=metadata, base=base
    )


@beartype
def _draw_sqlite_engines(
    temp_dir: TemporaryDirectory,
    /,
    *,
    metadata: MetaData | None = None,
    base: Any = None,
) -> Engine:
    path = temp_dir.name.joinpath("db.sqlite")
    engine = create_engine("sqlite", database=path.as_posix())
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_dir to the engine, so as to keep it alive
    engine.temp_dir = temp_dir  # type: ignore

    return engine
