from dataclasses import dataclass
from pathlib import Path

from beartype import beartype
from typed_settings import option, settings


@settings(frozen=True)
class Config:
    """Settings for the `rotate_logs` script."""

    path: Path = option(
        default=Path.cwd(),
        click={"param_decls": ("-p", "--path")},
    )
    extension: str = option(
        default="log",
        click={"param_decls": ("-e", "--extension")},
    )
    size: int = option(
        default=int(100 * 1024),
        click={"param_decls": ("-s", "--size")},
    )
    keep: int = option(default=3, click={"param_decls": ("-k", "--keep")})
    dry_run: bool = option(
        default=False,
        click={"param_decls": ("-dr", "--dry-run")},
    )


@beartype
@dataclass(frozen=True)
class Item:
    """A log file, identified."""

    path: Path
    head: Path
    num: int = 0
