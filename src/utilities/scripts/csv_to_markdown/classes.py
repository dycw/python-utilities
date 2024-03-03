from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path  # noqa: TCH003

from utilities.pathlib import ensure_path
from utilities.typed_settings import click_field


@dataclass(frozen=True)
class Config:
    """Settings for the `monitor_memory` script."""

    path: Path = click_field(
        default=ensure_path("input.csv"), param_decls=("-p", "--path")
    )
