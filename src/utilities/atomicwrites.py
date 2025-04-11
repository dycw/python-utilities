from __future__ import annotations

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from shutil import move, rmtree
from typing import TYPE_CHECKING, override

from atomicwrites import move_atomic, replace_atomic

from utilities.iterables import transpose
from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


def move_atomic_concurrent(*paths: tuple[PathLike, PathLike]) -> None:
    """Move a set of files concurrently."""
    sources, destinations = transpose(paths)
    sources, destinations = map(Path, [sources, destinations])
    len(paths)
    with ExitStack():
        pass


##


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
            is_file, is_dir = temp_path.is_file(), temp_path.is_dir()
            if is_file and overwrite:
                return replace_atomic(temp_path, path)
            if is_file and not overwrite:
                try:
                    return move_atomic(temp_path, path)
                except FileExistsError:
                    raise _WriterFileExistsError(destination=path) from None
            if is_dir and ((not path.exists()) or overwrite):
                return move(temp_path, path)
            if is_dir and path.exists() and not overwrite:
                raise _WriterDirectoryExistsError(destination=path)
            raise _WriterTypeError(temp_path=temp_path)


@dataclass(kw_only=True, slots=True)
class WriterError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _WriterFileExistsError(WriterError):
    destination: Path

    @override
    def __str__(self) -> str:
        return f"Cannot write to {str(self.destination)!r} as file already exists"


@dataclass(kw_only=True, slots=True)
class _WriterDirectoryExistsError(WriterError):
    destination: Path

    @override
    def __str__(self) -> str:
        return f"Cannot write to {str(self.destination)!r} as directory already exists"


@dataclass(kw_only=True, slots=True)
class _WriterTypeError(WriterError):
    temp_path: Path

    @override
    def __str__(self) -> str:
        return (
            f"Temporary path {str(self.temp_path)!r} is neither a file nor a directory"
        )


__all__ = ["WriterError", "writer"]
