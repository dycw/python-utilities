from pathlib import Path
from typing import Union

from hypothesis.strategies import SearchStrategy
from sqlalchemy.engine import Engine

from dycw_utilities.hypothesis import draw_and_map
from dycw_utilities.hypothesis.tempfile import temp_dirs
from dycw_utilities.hypothesis.typing import MaybeSearchStrategy
from dycw_utilities.sqlalchemy import create_engine
from dycw_utilities.tempfile import TemporaryDirectory


def sqlite_engines(
    *, path: MaybeSearchStrategy[Union[Path, TemporaryDirectory]] = temp_dirs()
) -> SearchStrategy[Engine]:
    """Strategy for generating SQLite engines."""

    return draw_and_map(_draw_sqlite_engines, path)


def _draw_sqlite_engines(path: Union[Path, TemporaryDirectory], /) -> Engine:
    path_use = path if isinstance(path, Path) else path.name
    return create_engine("sqlite", database=path_use.as_posix())
