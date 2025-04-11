from __future__ import annotations

import shutil
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, assert_never, override

from atomicwrites import replace_atomic

from utilities.errors import ImpossibleCaseError
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
            return replace_atomic(str(source), str(destination))  # must be `str`s
        case True, False, _, _, _:
            return replace_atomic(str(source), str(destination))  # must be `str`s
        # directories
        case (False, True, True, False, False) | (False, True, False, True, False):
            raise _MoveDirectoryExistsError(source=source, destination=destination)
        case False, True, False, True, _:
            rmtree(destination, ignore_errors=True)
            return shutil.move(source, destination)
        case False, True, _, _, _:
            destination.unlink(missing_ok=True)
            return shutil.move(source, destination)
        case True, True, _, _, _:  # pragma: no cover
            raise ImpossibleCaseError(
                case=[f"{source.is_file()=}", f"{source.is_dir()=}"]
            )
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


##


def move_many(*paths: tuple[PathLike, PathLike], overwrite: bool = False) -> None:
    """Move a set of files concurrently."""
    sources, destinations = transpose(paths)
    with ExitStack() as stack:
        temp_paths = [
            stack.enter_context(writer(p, overwrite=overwrite)) for p in destinations
        ]
        for source, temp_path in zip(sources, temp_paths, strict=True):
            move(source, temp_path, overwrite=overwrite)


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
            try:
                move(temp_path, path, overwrite=overwrite)
            except _MoveSourceNotFoundError as error:
                raise _WriterTemporaryPathEmptyError(temp_path=error.source) from None
            except _MoveFileExistsError as error:
                raise _WriterFileExistsError(destination=error.destination) from None
            except _MoveDirectoryExistsError as error:
                raise _WriterDirectoryExistsError(
                    destination=error.destination
                ) from None


@dataclass(kw_only=True, slots=True)
class WriterError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _WriterTemporaryPathEmptyError(WriterError):
    temp_path: Path

    @override
    def __str__(self) -> str:
        return f"Temporary path {str(self.temp_path)!r} is empty"


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


__all__ = ["MoveError", "WriterError", "move", "move_many", "writer"]
