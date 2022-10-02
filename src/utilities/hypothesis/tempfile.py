from pathlib import Path
from uuid import UUID

from beartype import beartype
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import uuids

from utilities.hypothesis import draw_and_map
from utilities.tempfile import TemporaryDirectory
from utilities.tempfile import gettempdir


@beartype
def temp_dirs() -> SearchStrategy[TemporaryDirectory]:
    """Search strategy for temporary directories."""

    dir = gettempdir().joinpath("hypothesis")
    dir.mkdir(exist_ok=True)
    return draw_and_map(_draw_temp_dirs, uuids(), dir)


@beartype
def _draw_temp_dirs(uuid: UUID, dir: Path, /) -> TemporaryDirectory:
    return TemporaryDirectory(prefix=f"{uuid}__", dir=dir.as_posix())


@beartype
def temp_paths() -> SearchStrategy[Path]:
    """Search strategy for paths to temporary directories."""

    return draw_and_map(_draw_temp_paths, temp_dirs())


@beartype
def _draw_temp_paths(temp_dir: TemporaryDirectory, /) -> Path:
    class SubPath(type(root := temp_dir.name)):
        _temp_dir = temp_dir

    return SubPath(root)
