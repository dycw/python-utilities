from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from typed_settings import option, settings

from utilities.tempfile import TEMP_DIR


@settings(frozen=True)
class Config:
    """Settings for the `clean_dir` script."""

    paths: list[Path] = option(
        default=[TEMP_DIR], click={"param_decls": ("-p", "--path")}
    )
    days: int = option(default=7, click={"param_decls": ("-d", "--days")})
    chunk_size: int | None = option(
        default=None, click={"param_decls": ("-cs", "--chunk-size")}
    )
    dry_run: bool = option(
        default=False, click={"param_decls": ("-dr", "--dry-run")}
    )


@dataclass(frozen=True)
class Item:
    """An item to clean up."""

    path: Path
    clean: Callable[[], None]
