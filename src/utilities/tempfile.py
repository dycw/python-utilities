from __future__ import annotations

import tempfile
from pathlib import Path
from shutil import copyfile, move
from tempfile import NamedTemporaryFile as _NamedTemporaryFile
from tempfile import gettempdir as _gettempdir
from typing import TYPE_CHECKING, override

from utilities.contextlib import enhanced_context_manager
from utilities.warnings import suppress_warnings

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from utilities.types import PathLike


class TemporaryDirectory:
    """Wrapper around `TemporaryDirectory` with a `Path` attribute."""

    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,  # noqa: A002
        ignore_cleanup_errors: bool = False,
        delete: bool = True,
    ) -> None:
        super().__init__()
        self._temp_dir = _TemporaryDirectoryNoResourceWarning(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
            delete=delete,
        )
        self.path = Path(self._temp_dir.name)

    def __enter__(self) -> Path:
        return Path(self._temp_dir.__enter__())

    def __exit__(
        self,
        exc: type[BaseException] | None,
        val: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._temp_dir.__exit__(exc, val, tb)


class _TemporaryDirectoryNoResourceWarning(tempfile.TemporaryDirectory):
    @classmethod
    @override
    def _cleanup(  # pyright: ignore[reportGeneralTypeIssues]
        cls,
        name: str,
        warn_message: str,
        ignore_errors: bool = False,
        delete: bool = True,
    ) -> None:
        with suppress_warnings(category=ResourceWarning):
            return super()._cleanup(  # pyright: ignore[reportAttributeAccessIssue]
                name, warn_message, ignore_errors=ignore_errors, delete=delete
            )


##


@enhanced_context_manager
def TemporaryFile(  # noqa: N802
    *,
    dir: PathLike | None = None,  # noqa: A002
    suffix: str | None = None,
    prefix: str | None = None,
    ignore_cleanup_errors: bool = False,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    """Yield a temporary file."""
    if dir is None:
        with (
            TemporaryDirectory(
                suffix=suffix,
                prefix=prefix,
                dir=dir,
                ignore_cleanup_errors=ignore_cleanup_errors,
                delete=delete,
            ) as temp_dir,
            _temporary_file_outer(
                temp_dir,
                suffix=suffix,
                prefix=prefix,
                delete=delete,
                name=name,
                data=data,
                text=text,
            ) as temp,
        ):
            yield temp
    else:
        with _temporary_file_outer(
            dir,
            suffix=suffix,
            prefix=prefix,
            delete=delete,
            name=name,
            data=data,
            text=text,
        ) as temp:
            yield temp


@enhanced_context_manager
def _temporary_file_outer(
    path: PathLike,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    with _temporary_file_inner(
        path, suffix=suffix, prefix=prefix, delete=delete, name=name
    ) as temp:
        if data is not None:
            _ = temp.write_bytes(data)
        if text is not None:
            _ = temp.write_text(text)
        yield temp


@enhanced_context_manager
def _temporary_file_inner(
    path: PathLike,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
) -> Iterator[Path]:
    path = Path(path)
    temp = _NamedTemporaryFile(  # noqa: SIM115
        suffix=suffix, prefix=prefix, dir=path, delete=delete, delete_on_close=False
    )
    if name is None:
        yield path / temp.name
    else:
        _ = move(path / temp.name, path / name)
        yield path / name


##


def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""
    return Path(_gettempdir())


TEMP_DIR = gettempdir()


##


def yield_temp_dir_at(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary dir for a target path."""

    path = Path(path)
    with TemporaryDirectory(suffix=".tmp", prefix=path.name, dir=path.parent) as temp:
        yield temp


def yield_temp_file_at(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary file for a target path."""

    path = Path(path)
    with TemporaryFile(dir=path.parent, suffix=".tmp", prefix=path.name) as temp:
        yield temp


__all__ = [
    "TEMP_DIR",
    "TemporaryDirectory",
    "TemporaryFile",
    "gettempdir",
    "yield_temp_dir_at",
    "yield_temp_file_at",
]
