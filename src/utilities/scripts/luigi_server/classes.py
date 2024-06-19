from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path  # noqa: TCH003

from utilities.pathlib import ensure_path
from utilities.typed_settings import click_field


@dataclass(frozen=True)
class Config:
    """Settings for the `pypi_server` script."""

    pid_file: Path = click_field(
        default=ensure_path("pidfile"), param_decls=("-pf", "--pidfile")
    )
    log_dir: Path = click_field(
        default=ensure_path("logs"), param_decls=("-ld", "--log-dir")
    )
    state_path: Path = click_field(
        default=ensure_path("luigi-state.pickle"), param_decls=("-sp", "--state-path")
    )
    port: int = click_field(default=1456, param_decls=("-po", "--port"))
    dry_run: bool = click_field(default=False, param_decls=("-dr", "--dry-run"))
    exist_ok: bool = click_field(default=False, param_decls=("-e", "--exist-ok"))
