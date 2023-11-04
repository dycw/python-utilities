from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utilities.typed_settings import click_field


@dataclass(frozen=True)
class Config:
    """Settings for the `monitor_memory` script."""

    path: Path = click_field(default=Path("input.csv"), param_decls=("-p", "--path"))
