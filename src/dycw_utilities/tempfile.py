from pathlib import Path
from tempfile import TemporaryDirectory as _TemporaryDirectory


class TemporaryDirectory(_TemporaryDirectory):
    """Sub-class of TemporaryDirectory whose"""

    def __enter__(self) -> Path:
        return Path(super().__enter__())
