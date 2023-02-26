from typing import Any
from typing import Optional
from typing import cast

from hypothesis.strategies import DrawFn
from hypothesis.strategies import composite
from sqlalchemy import Engine
from sqlalchemy import MetaData

from utilities.hypothesis import temp_paths
from utilities.sqlalchemy import create_engine


@composite
def sqlite_engines(
    _draw: DrawFn,
    /,
    *,
    metadata: Optional[MetaData] = None,
    base: Any = None,
) -> Engine:
    """Strategy for generating SQLite engines."""
    temp_path = _draw(temp_paths())
    path = temp_path.joinpath("db.sqlite")
    engine = create_engine("sqlite", database=path.as_posix())
    if metadata is not None:
        metadata.create_all(engine)
    if base is not None:
        base.metadata.create_all(engine)

    # attach temp_path to the engine, so as to keep it alive
    cast(Any, engine).temp_path = temp_path

    return engine
