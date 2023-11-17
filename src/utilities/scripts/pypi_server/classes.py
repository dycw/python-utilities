from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utilities.typed_settings import click_field


@dataclass(frozen=True)
class Config:
    """Settings for the `pypi_server` script."""

    path_password: Path = click_field(
        default=Path("password"),
        help="generate using 'htpasswd -nbB username password'",
        param_decls=("-pw", "--path-password"),
    )
    path_packages: Path = click_field(
        default=Path("packages"), param_decls=("-pk", "--path-packages")
    )
    port: int = click_field(default=1461, param_decls=("-po", "--port"))
    dry_run: bool = click_field(default=False, param_decls=("-dr", "--dry-run"))
    exist_ok: bool = click_field(default=False, param_decls=("-e", "--exist-ok"))
