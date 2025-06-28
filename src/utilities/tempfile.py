from __future__ import annotations

import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
    from utilities.types import PathLike


class TemporaryDirectory(tempfile.TemporaryDirectory):
    """Wrapper around `TemporaryDirectory` with a `Path` attribute."""

    @override
    def __init__(
        self,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,
        ignore_cleanup_errors: bool = False,
        *,
        delete: bool = True,
    ) -> None:
        super().__init__(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
            delete=delete,
        )
        self._temp_dir = _TemporaryDirectory(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
        )
        self.path = Path(self._temp_dir.name)

    @override
    def __enter__(self) -> Path:
        return Path(super().__enter__())


##


def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""
    return Path(_gettempdir())


TEMP_DIR = gettempdir()


__all__ = ["TEMP_DIR", "TemporaryDirectory", "gettempdir"]
