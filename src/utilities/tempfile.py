from __future__ import annotations

import tempfile
from contextlib import contextmanager
from pathlib import Path
from shutil import move
from tempfile import NamedTemporaryFile as _NamedTemporaryFile
from tempfile import gettempdir as _gettempdir
from typing import TYPE_CHECKING, override

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


@contextmanager
def TemporaryFile(  # noqa: N802
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    dir: PathLike | None = None,  # noqa: A002
    ignore_cleanup_errors: bool = False,
    delete: bool = True,
    name: str | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    """Yield a temporary file."""
    with _temporary_file_inner(
        suffix=suffix,
        prefix=prefix,
        dir=dir,
        ignore_cleanup_errors=ignore_cleanup_errors,
        delete=delete,
        name=name,
    ) as temp:
        if text is not None:
            _ = temp.write_text(text)
        yield temp


@contextmanager
def _temporary_file_inner(
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    dir: PathLike | None = None,  # noqa: A002
    ignore_cleanup_errors: bool = False,
    delete: bool = True,
    name: str | None = None,
) -> Iterator[Path]:
    with TemporaryDirectory(
        suffix=suffix,
        prefix=prefix,
        dir=dir,
        ignore_cleanup_errors=ignore_cleanup_errors,
        delete=delete,
    ) as temp_dir:
        temp_file = _NamedTemporaryFile(  # noqa: SIM115
            dir=temp_dir, delete=delete, delete_on_close=False
        )
        if name is None:
            yield temp_dir / temp_file.name
        else:
            _ = move(temp_dir / temp_file.name, temp_dir / name)
            yield temp_dir / name


##


def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""
    return Path(_gettempdir())


TEMP_DIR = gettempdir()


__all__ = ["TEMP_DIR", "TemporaryDirectory", "TemporaryFile", "gettempdir"]
