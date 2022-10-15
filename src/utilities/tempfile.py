from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir

from beartype import beartype

from utilities.pathlib import PathLike


class TemporaryDirectory(_TemporaryDirectory):
    """Sub-class of TemporaryDirectory whose name attribute is a Path."""

    name: Path

    @beartype
    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,
        ignore_cleanup_errors: bool = False,
    ) -> None:
        super().__init__(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
        )
        self.name = Path(self.name)

    @beartype
    def __enter__(self) -> Path:
        return super().__enter__()


@beartype
def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""

    return Path(_gettempdir())
