from __future__ import annotations

import gzip
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from shutil import copyfileobj, copytree, rmtree
from typing import TYPE_CHECKING, assert_never, override

from utilities.contextlib import enhanced_context_manager
from utilities.iterables import transpose
from utilities.pathlib import file_or_dir
from utilities.tempfile import TemporaryDirectory, TemporaryFile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


def copy(src: PathLike, dest: PathLike, /, *, overwrite: bool = False) -> None:
    """Copy a file/directory atomically."""
    src, dest = map(Path, [src, dest])
    match file_or_dir(src):
        case "file":
            with TemporaryFile(data=src.read_bytes()) as temp:
                try:
                    move(temp, dest, overwrite=overwrite)
                except _MoveFileExistsError as error:
                    raise _CopyFileExistsError(src=error.src, dest=error.dest) from None
        case "dir":
            with TemporaryDirectory() as temp:
                temp_sub_dir = temp / "sub_dir"
                _ = copytree(src, temp_sub_dir)
                try:
                    move(temp_sub_dir, dest, overwrite=overwrite)
                except _MoveDirectoryExistsError as error:
                    raise _CopyDirectoryExistsError(
                        src=error.src, dest=error.dest
                    ) from None
        case None:
            raise _CopySourceNotFoundError(src=src)
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class CopyError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _CopySourceNotFoundError(CopyError):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {str(self.src)!r} does not exist"


@dataclass(kw_only=True, slots=True)
class _CopyFileExistsError(CopyError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot copy file {str(self.src)!r} as destination {str(self.dest)!r} already exists"


@dataclass(kw_only=True, slots=True)
class _CopyDirectoryExistsError(CopyError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot copy directory {str(self.src)!r} as destination {str(self.dest)!r} already exists"


##


def move_many(*paths: tuple[PathLike, PathLike], overwrite: bool = False) -> None:
    """Move a set of files concurrently."""
    srcs, dests = transpose(paths)
    with ExitStack() as stack:
        temps = [stack.enter_context(writer(d, overwrite=overwrite)) for d in dests]
        for src, temp in zip(srcs, temps, strict=True):
            move(src, temp, overwrite=overwrite)


##


@enhanced_context_manager
def writer(
    path: PathLike, /, *, compress: bool = False, overwrite: bool = False
) -> Iterator[Path]:
    """Yield a path for atomically writing files to disk."""
    path = Path(path)
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    name = path.name
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=parent) as temp_dir:
        temp_path1 = Path(temp_dir, name)
        try:
            yield temp_path1
        except KeyboardInterrupt:
            rmtree(temp_dir)
        else:
            if compress:
                temp_path2 = Path(temp_dir, f"{name}.gz")
                with (
                    temp_path1.open("rb") as source,
                    gzip.open(temp_path2, mode="wb") as dest,
                ):
                    copyfileobj(source, dest)
            else:
                temp_path2 = temp_path1
            try:
                move(temp_path2, path, overwrite=overwrite)
            except _MoveSourceNotFoundError as error:
                raise _WriterTemporaryPathEmptyError(temp_path=error.src) from None
            except _MoveFileExistsError as error:
                raise _WriterFileExistsError(destination=error.dest) from None
            except _MoveDirectoryExistsError as error:
                raise _WriterDirectoryExistsError(destination=error.dest) from None


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


__all__ = [
    "CopyError",
    "MoveError",
    "WriterError",
    "copy",
    "move",
    "move_many",
    "writer",
]
