from __future__ import annotations

from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir
from typing import TYPE_CHECKING

from utilities.pathlib import ensure_path

if TYPE_CHECKING:
    from pathlib import Path
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
        validate: bool = False,
    ) -> None:
        super().__init__()
        self._temp_dir = _TemporaryDirectory(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
        )
        self._validate = validate
        self.path = ensure_path(self._temp_dir.name, validate=self._validate)

    def __enter__(self) -> Path:
        return ensure_path(self._temp_dir.__enter__(), validate=self._validate)

    def __exit__(
        self,
        exc: type[BaseException] | None,
        val: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._temp_dir.__exit__(exc, val, tb)


def gettempdir(*, validate: bool = False) -> Path:
    """Get the name of the directory used for temporary files."""
    return ensure_path(_gettempdir(), validate=validate)


TEMP_DIR = gettempdir()


__all__ = ["TEMP_DIR", "TemporaryDirectory", "gettempdir"]
