from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir

from typing_extensions import override

from utilities.pathlib import PathLike


class TemporaryDirectory(_TemporaryDirectory):
    """Sub-class of TemporaryDirectory whose name attribute is a Path."""

    name: Path

    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,  # noqa: A002
    ) -> None:
        super().__init__(suffix=suffix, prefix=prefix, dir=dir)
        self.name = Path(self.name)

    @override
    def __enter__(self) -> Path:
        return super().__enter__()


def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""
    return Path(_gettempdir())


TEMP_DIR = gettempdir()
