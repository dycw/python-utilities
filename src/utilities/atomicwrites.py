from __future__ import annotations

import shutil
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, assert_never, override

from atomicwrites import move_atomic, replace_atomic

from utilities.iterables import transpose
from utilities.tempfile import TemporaryDirectory

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


def move(
    source: PathLike, destination: PathLike, /, *, overwrite: bool = False
) -> None:
    """Move/replace a file/directory atomically."""
    source, destination = map(Path, [source, destination])
    match (
        source.is_file(),
        source.is_dir(),
        destination.is_file(),
        destination.is_dir(),
        overwrite,
    ):
        case False, False, _, _, _:
            raise _MoveSourceNotFoundError(source=source)
        # files
        case (True, False, True, False, False) | (True, False, False, True, False):
            raise _MoveFileExistsError(source=source, destination=destination) from None
        case True, False, False, True, _:
            rmtree(destination, ignore_errors=True)
            return replace_atomic(source, destination)
        case True, False, _, _, _:
            return replace_atomic(source, destination)
        # directories
        case (False, True, True, False, False) | (False, True, False, True, False):
            raise _MoveDirectoryExistsError(source=source, destination=destination)
        case False, True, False, True, _:
            rmtree(destination, ignore_errors=True)
            return shutil.move(source, destination)
        case False, True, _, _, _:
            destination.unlink(missing_ok=True)
            return shutil.move(source, destination)
        case True, True, _, _, _:
            raise _MoveTypeError(source=source)
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class MoveError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _MoveSourceNotFoundError(MoveError):
    source: Path

    @override
    def __str__(self) -> str:
        return f"Source {str(self.source)!r} does not exist"


@dataclass(kw_only=True, slots=True)
class _MoveFileExistsError(MoveError):
    source: Path
    destination: Path

    @override
    def __str__(self) -> str:
        return f"Cannot move file {str(self.source)!r} as destination {str(self.destination)!r} already exists"


@dataclass(kw_only=True, slots=True)
class _MoveDirectoryExistsError(MoveError):
    source: Path
    destination: Path

    @override
    def __str__(self) -> str:
        return f"Cannot move directory {str(self.source)!r} as destination {str(self.destination)!r} already exists"


@dataclass(kw_only=True, slots=True)
class _MoveTypeError(MoveError):
    source: Path

    @override
    def __str__(self) -> str:
        return f"Source {str(self.source)!r} is neither a file nor a directory"


##


def move_atomic_concurrent(*paths: tuple[PathLike, PathLike]) -> None:
    """Move a set of files concurrently."""
    _move_or_replace_atomic_concurrent(*paths, overwrite=False)


def replace_atomic_concurrent(*paths: tuple[PathLike, PathLike]) -> None:
    """Replace a set of files concurrently."""
    _move_or_replace_atomic_concurrent(*paths, overwrite=True)


def _move_or_replace_atomic_concurrent(
    *paths: tuple[PathLike, PathLike], overwrite: bool = False
) -> None:
    """Move a set of files concurrently."""
    sources, destinations = transpose(paths)
    sources, destinations = [
        list(map(Path, paths)) for paths in [sources, destinations]
    ]
    with ExitStack() as stack:
        temp_paths = [
            stack.enter_context(writer(p, overwrite=overwrite)) for p in destinations
        ]
        for source, temp_path in zip(sources, temp_paths, strict=True):
            move_atomic(source, temp_path)


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
                return shutil.move(temp_path, path)
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


__all__ = ["MoveError", "WriterError", "move", "move", "writer"]
