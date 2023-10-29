from __future__ import annotations

from pathlib import Path

from typed_settings import option, settings


@settings(frozen=True)
class Config:
    """Settings for the `monitor_memory` script."""

    path: Path = option(
        default=Path("input.csv"), click={"param_decls": ("-p", "--path")}
    )
