from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir
from typing import Optional

from beartype import beartype

from utilities.pathlib import PathLike


class TemporaryDirectory(_TemporaryDirectory):
    """Sub-class of TemporaryDirectory whose name attribute is a Path."""

    name: Path

    @beartype
    def __init__(
        self,
        *,
        suffix: Optional[str] = None,
        prefix: Optional[str] = None,
        dir: Optional[PathLike] = None,  # noqa: A002
    ) -> None:
        super().__init__(suffix=suffix, prefix=prefix, dir=dir)
        self.name = Path(self.name)

    @beartype
    def __enter__(self) -> Path:
        return super().__enter__()


@beartype
def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""
    return Path(_gettempdir())


TEMP_DIR = gettempdir()
