from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from atomicwrites import move_atomic
from atomicwrites import replace_atomic
from beartype import beartype

from utilities.pathlib import PathLike
from utilities.tempfile import TemporaryDirectory


@contextmanager
@beartype
def writer(path: PathLike, /, *, overwrite: bool = False) -> Iterator[Path]:
    """Yield a path for atomically writing files to disk."""

    path = Path(path)
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    name = path.name
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=parent) as temp_dir:
        yield (temp_path := temp_dir.joinpath(name))
        src, dest = temp_path.as_posix(), path.as_posix()
        if overwrite:
            replace_atomic(src, dest)
        else:
            move_atomic(src, dest)
