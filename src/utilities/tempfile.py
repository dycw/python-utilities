from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory
from tempfile import gettempdir as _gettempdir


class TemporaryDirectory(_TemporaryDirectory):  # type: ignore
    """Sub-class of TemporaryDirectory whose name attribute is a Path."""

    name: Path

    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: str | None = None,
        ignore_cleanup_errors: bool = False,
    ) -> None:
        super().__init__(  # type: ignore
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
        )
        self.name = Path(self.name)

    def __enter__(self) -> Path:
        return super().__enter__()


def gettempdir() -> Path:
    """Get the name of the directory used for temporary files."""

    return Path(_gettempdir())
