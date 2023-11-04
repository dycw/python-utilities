from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from utilities.tempfile import TEMP_DIR
from utilities.typed_settings import click_field


@dataclass(frozen=True)
class Config:
    """Settings for the `clean_dir` script."""

    paths: set[Path] = click_field(
        default=frozenset([TEMP_DIR]), param_decls=("-p", "--path")
    )
    days: int = click_field(default=7, param_decls=("-d", "--days"))
    chunk_size: Optional[int] = click_field(  # noqa: UP007
        default=None, param_decls=("-cs", "--chunk-size")
    )
    dry_run: bool = click_field(default=False, param_decls=("-dr", "--dry-run"))


@dataclass(frozen=True)
class Item:
    """An item to clean up."""

    path: Path
    clean: Callable[[], None]
