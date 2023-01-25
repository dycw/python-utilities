import datetime as dt
from collections.abc import Iterator
from functools import partial
from getpass import getuser
from itertools import islice
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Optional

from beartype import beartype
from click import command, option

from utilities.clean_dir.classes import Config, Item
from utilities.datetime import UTC
from utilities.logging import basic_config

_LOGGER = getLogger(__name__)


@command()
@option("-p", "--path", type=Path, default=Config.path, show_default=True)
@option("-d", "--days", type=int, default=Config.days, show_default=True)
@option(
    "-c",
    "--chunk-size",
    type=int,
    default=Config.chunk_size,
    show_default=True,
)
@option("-dr", "--dry-run", is_flag=True, show_default=True)
@beartype
def main(
    *,
    path: Path,
    days: int,
    chunk_size: Optional[int],
    dry_run: bool,
) -> None:
    """CLI for the `clean_dir` script."""
    basic_config()
    config = Config(
        path=path,
        days=days,
        chunk_size=chunk_size,
        dry_run=dry_run,
    )
    _log_config(config)
    if config.dry_run:
        for item in _yield_items(config):
            _LOGGER.debug("%s", item.path)
    else:
        _clean_dir(config)


@beartype
def _log_config(config: Config, /) -> None:
    _LOGGER.info("path        = %s", config.path)
    _LOGGER.info("days        = %s", config.days)
    _LOGGER.info("chunk_sizes = %s", config.chunk_size)
    _LOGGER.info("dry_run     = %s", config.dry_run)


@beartype
def _clean_dir(config: Config, /) -> None:
    while True:
        if len(items := list(_yield_items(config))) >= 1:
            for item in items:
                item.clean()
        else:
            return


@beartype
def _yield_items(config: Config, /) -> Iterator[Item]:
    it = _yield_inner(config)
    if (chunk_size := config.chunk_size) is not None:
        return islice(it, chunk_size)
    return it


@beartype
def _yield_inner(config: Config, /) -> Iterator[Item]:
    for p in config.path.rglob("*"):
        yield from _yield_from_path(p, config)


@beartype
def _yield_from_path(path: Path, config: Config, /) -> Iterator[Item]:
    if path.is_symlink():
        yield from _yield_from_path(path.resolve(), config)
    elif _is_owned_and_relative(path, config):  # pragma: no cover
        if (path.is_file() or path.is_socket()) and _is_old(path, config):
            yield Item(path, partial(_unlink_path, path))
        elif path.is_dir() and _is_empty(path):
            yield Item(path, partial(_unlink_dir, path))


@beartype
def _is_owned_and_relative(path: Path, config: Config, /) -> bool:
    try:
        return (path.owner() == getuser()) and path.is_relative_to(config.path)
    except FileNotFoundError:  # pragma: no cover
        return False


@beartype
def _is_empty(path: Path, /) -> bool:
    return len(list(path.iterdir())) == 0


@beartype
def _is_old(path: Path, config: Config, /) -> bool:
    age = dt.datetime.now(tz=UTC) - dt.datetime.fromtimestamp(
        path.stat().st_mtime,
        tz=UTC,
    )
    return age >= dt.timedelta(days=config.days)


@beartype
def _unlink_path(path: Path, /) -> None:
    _LOGGER.info("Removing file:      %s", path)
    path.unlink(missing_ok=True)


@beartype
def _unlink_dir(path: Path, /) -> None:
    _LOGGER.info("Removing directory: %s", path)
    rmtree(path)
