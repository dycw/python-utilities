from pathlib import Path

from attrs import asdict
from beartype import beartype
from click import command
from loguru import logger

from utilities.loguru import setup_loguru
from utilities.luigi.server.classes import Config
from utilities.pathlib import PathLike
from utilities.subprocess import run_accept_address_in_use
from utilities.typed_settings import click_options

_CONFIG = Config()


@command()
@click_options(Config, appname="pypiserver")
@beartype
def main(config: Config, /) -> None:
    """CLI for starting the luigi server."""
    setup_loguru()
    _log_config(config)
    config.log_dir.mkdir(parents=True, exist_ok=True)
    args = _get_args(
        pid_file=config.pid_file,
        log_dir=config.log_dir,
        state_path=config.state_path,
        port=config.port,
    )
    if not config.dry_run:
        run_accept_address_in_use(args, exist_ok=config.exist_ok)  # pragma: no cover


@beartype
def _log_config(config: Config, /) -> None:
    for key, value in asdict(config).items():
        logger.info("{key:10} = {value}", key=key, value=value)


@beartype
def _get_args(
    *,
    pid_file: PathLike = _CONFIG.pid_file,
    log_dir: PathLike = _CONFIG.log_dir,
    state_path: PathLike = _CONFIG.state_path,
    port: int = _CONFIG.port,
) -> list[str]:
    pid_file, log_dir, state_path = map(Path, [pid_file, log_dir, state_path])
    args = [
        "luigid",
        f"--pidfile={pid_file.as_posix()}",
        f"--logdir={log_dir.as_posix()}",
        f"--state-path={state_path.as_posix()}",
        f"--port={port}",
    ]
    logger.info("cmd = {cmd!r}", cmd=" ".join(args))
    return args
