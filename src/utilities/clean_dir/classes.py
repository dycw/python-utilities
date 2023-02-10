from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from beartype import beartype

from utilities.tempfile import TEMP_DIR


@beartype
@dataclass(frozen=True)
class Config:
    """Settings for the `clean_dir` script."""

    path: Path = TEMP_DIR
    days: int = 7
    chunk_size: Optional[int] = None
    dry_run: bool = False


@beartype
@dataclass(frozen=True)
class Item:
    """An item to clean up."""

    path: Path
    clean: Callable[[], None]
