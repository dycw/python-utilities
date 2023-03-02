from pathlib import Path

from beartype import beartype
from typed_settings import option, settings


@beartype
@settings(frozen=True)
class Config:
    """Settings for the `pypi_server` script."""

    pid_file: Path = option(
        default=Path("pidfile"), click={"param_decls": ("-pf", "--pidfile")}
    )
    log_dir: Path = option(
        default=Path("logs"), click={"param_decls": ("-ld", "--log-dir")}
    )
    state_path: Path = option(
        default=Path("luigi-state.pickle"),
        click={"param_decls": ("-sp", "--state-path")},
    )
    port: int = option(default=1456, click={"param_decls": ("-po", "--port")})
    dry_run: bool = option(default=False, click={"param_decls": ("-dr", "--dry-run")})
    exist_ok: bool = option(default=False, click={"param_decls": ("-e", "--exist-ok")})
