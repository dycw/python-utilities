from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from shutil import move, rmtree

from atomicwrites import move_atomic, replace_atomic
from pathvalidate import validate_filepath

from utilities.pathlib import PathLike
from utilities.tempfile import TemporaryDirectory


class DirectoryExistsError(Exception):
    ...


@contextmanager
def writer(path: PathLike, /, *, overwrite: bool = False) -> Iterator[Path]:
    """Yield a path for atomically writing files to disk."""
    path = Path(path)
    validate_filepath(path, platform="auto")
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    name = path.name
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=parent) as temp_dir:
        try:
            yield (temp_path := temp_dir.joinpath(name))
        except KeyboardInterrupt:
            rmtree(temp_dir)
        else:
            if temp_path.is_file():
                src, dest = map(str, [temp_path, path])
                if overwrite:
                    return replace_atomic(src, dest)
                return move_atomic(src, dest)
            if temp_path.is_dir():
                if (not path.exists()) or overwrite:
                    return move(temp_path, path)
                msg = f"{temp_dir=}, {path=}"
                raise DirectoryExistsError(msg)
            msg = f"{temp_path=}"
            raise WriterError(msg)


class WriterError(Exception):
    ...


__all__ = ["DirectoryExistsError", "WriterError", "writer"]
