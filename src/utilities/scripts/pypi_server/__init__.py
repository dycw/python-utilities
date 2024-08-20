from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from click import command
from loguru import logger

from utilities.scripts.pypi_server.classes import Config
from utilities.subprocess import run_accept_address_in_use
from utilities.typed_settings import click_options

if TYPE_CHECKING:
    from utilities.types import PathLike

_CONFIG = Config()


@command()
@click_options(Config, appname="pypiserver")
def main(config: Config, /) -> None:
    """CLI for starting the PyPI server."""
    _check_password_file(path_password=config.path_password)
    config.path_packages.mkdir(parents=True, exist_ok=True)
    args = _get_args(
        port=config.port,
        path_password=config.path_password,
        path_packages=config.path_packages,
    )
    if not config.dry_run:
        run_accept_address_in_use(args, exist_ok=config.exist_ok)  # pragma: no cover


def _check_password_file(*, path_password: PathLike = _CONFIG.path_password) -> None:
    if not Path(path_password).exists():
        msg = f"{path_password=!s}"
        raise FileNotFoundError(msg)


def _get_args(
    *,
    port: int = _CONFIG.port,
    path_password: PathLike = _CONFIG.path_password,
    path_packages: PathLike = _CONFIG.path_packages,
) -> list[str]:
    path_password, path_packages = map(Path, [path_password, path_packages])
    args = [
        "pypi-server",
        "run",
        f"--port={port}",
        "--authenticate=download,list,update",
        f"--passwords={path_password}",
        str(path_packages),
    ]
    logger.info("cmd = {cmd!r}", cmd=" ".join(args))
    return args
