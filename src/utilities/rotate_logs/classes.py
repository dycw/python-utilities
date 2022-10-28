from dataclasses import dataclass
from pathlib import Path

from beartype import beartype


@beartype
@dataclass(frozen=True)
class Config:
    """Settings for the `rotate_logs` script."""

    path: Path = Path.cwd()
    extension: str = "log"
    size: int = int(100 * 1024)
    keep: int = 3
    dry_run: bool = False


@beartype
@dataclass(frozen=True)
class Item:
    """A log file, identified."""

    path: Path
    head: Path
    num: int = 0
