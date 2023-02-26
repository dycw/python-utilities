from pathlib import Path

from beartype import beartype
from typed_settings import option
from typed_settings import settings


@beartype
@settings(frozen=True)
class Config:
    """Settings for the `pypi_server` script."""

    path_password: Path = option(
        default=Path("password"),
        click={"param_decls": ("-pw", "--path-password")},
    )  # generate using "htpasswd -nbB username password"
    path_packages: Path = option(
        default=Path("packages"),
        click={"param_decls": ("-pk", "--path-packages")},
    )
    port: int = option(default=1461, click={"param_decls": ("-po", "--port")})
    dry_run: bool = option(
        default=False,
        click={"param_decls": ("-dr", "--dry-run")},
    )
    exist_ok: bool = option(
        default=False,
        click={"param_decls": ("-e", "--exist-ok")},
    )
