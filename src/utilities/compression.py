from __future__ import annotations

from pathlib import Path
from shutil import copyfileobj
from tarfile import ReadError, TarFile
from typing import TYPE_CHECKING, assert_never

from utilities.atomicwrites import writer
from utilities.contextlib import enhanced_context_manager
from utilities.errors import ImpossibleCaseError
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.pathlib import file_or_dir
from utilities.tempfile import TemporaryDirectory, TemporaryFile

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from utilities.types import PathLike, PathToBinaryIO


def compress_paths(
    func: PathToBinaryIO, src_or_dest: PathLike, /, *srcs_or_dest: PathLike
) -> None:
    """Compress a set of files and/or directories."""
    *srcs, dest = list(map(Path, [src_or_dest, *srcs_or_dest]))
    with writer(dest, overwrite=True) as temp, func(temp) as buffer:
        match srcs:
            case [src]:
                match file_or_dir(src):
                    case "file":
                        with src.open(mode="rb") as fh:
                            copyfileobj(fh, buffer)
                    case "dir":
                        with TarFile(mode="w", fileobj=buffer) as tar:
                            _compress_paths_add_dir(src, tar)
                    case None:
                        ...
                    case never:
                        assert_never(never)
            case _:
                with TarFile(mode="w", fileobj=buffer) as tar:
                    for src_i in sorted(srcs):
                        match file_or_dir(src_i):
                            case "file":
                                tar.add(src_i, src_i.name)
                            case "dir":
                                _compress_paths_add_dir(src_i, tar)
                            case None:
                                ...
                            case never:
                                assert_never(never)


def _compress_paths_add_dir(path: PathLike, tar: TarFile, /) -> None:
    path = Path(path)
    for p in sorted(path.rglob("**/*")):
        tar.add(p, p.relative_to(path))


##


@enhanced_context_manager
def yield_compressed_contents(
    path: PathLike, func: PathToBinaryIO, /
) -> Iterator[Path]:
    """Yield the contents of a compressed file/directory."""
    with func(path) as gz:
        try:
            with TarFile(fileobj=gz) as tf, TemporaryDirectory() as temp:
                tf.extractall(path=temp, filter="data")
                try:
                    yield one(temp.iterdir())
                except (OneEmptyError, OneNonUniqueError):
                    yield temp
        except ReadError as error:
            (arg,) = error.args
            if arg == "empty file":
                with TemporaryDirectory() as temp:
                    yield temp
            elif arg == "truncated header":
                _ = gz.seek(0)
                with TemporaryFile() as temp, temp.open(mode="wb") as fh:
                    copyfileobj(gz, fh)
                    _ = fh.seek(0)
                    yield temp
            else:  # pragma: no cover
                raise ImpossibleCaseError(case=[f"{arg=}"]) from None


__all__ = ["compress_paths", "yield_compressed_contents"]
