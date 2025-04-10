from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from shutil import move, rmtree
from typing import TYPE_CHECKING, override

from atomicwrites import move_atomic, replace_atomic

from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


@contextmanager
def writer(path: PathLike, /, *, overwrite: bool = False) -> Iterator[Path]:
    """Yield a path for atomically writing files to disk."""
    path = Path(path)
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    name = path.name
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=parent) as temp_dir:
        temp_path = Path(temp_dir, name)
        try:
            yield temp_path
        except KeyboardInterrupt:
            rmtree(temp_dir)
        else:
            if temp_path.is_file():
                if overwrite:
                    return replace_atomic(temp_path, path)
                return move_atomic(temp_path, path)
            if temp_path.is_dir():
                if path.exists() and not overwrite:
                    raise _WriterDirectoryExistsError(
                        temp_path=temp_path, destination=path
                    )
                return move(temp_path, path)
            raise _WriterTypeError(temp_path=temp_path)


@dataclass(kw_only=True, slots=True)
class WriterError(Exception):
    temp_path: Path


@dataclass(kw_only=True, slots=True)
class _WriterDirectoryExistsError(WriterError):
    destination: Path

    @override
    def __str__(self) -> str:
        return f"Cannot move temporary directory {str(self.temp_path)!r} to {str(self.destination)!r} without `overwrite`"


class _WriterTypeError(WriterError):
    @override
    def __str__(self) -> str:
        return (
            f"Temporary path {str(self.temp_path)!r} is neither a file nor a directory"
        )


__all__ = ["WriterError", "writer"]
