from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from utilities.pathlib import ensure_path
from utilities.typed_settings import click_field

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class Config:
    """Settings for the `monitor_memory` script."""

    path: Path = click_field(
        default=ensure_path("input.csv"), param_decls=("-p", "--path")
    )
