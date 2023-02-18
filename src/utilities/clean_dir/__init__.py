import datetime as dt
from collections.abc import Iterator
from functools import partial
from getpass import getuser
from itertools import islice
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from typing import Optional

from attrs import asdict
from beartype import beartype
from click import command

from utilities.clean_dir.classes import Config, Item
from utilities.datetime import UTC
from utilities.logging import basic_config
from utilities.typed_settings import click_options

_CONFIG = Config()
_LOGGER = getLogger(__name__)


@command()
@click_options(Config)
@beartype
def main(config: Config, /) -> None:
    """CLI for the `clean_dir` script."""
    basic_config()
    _log_config(config)
    if config.dry_run:
        for item in _yield_items(
            path=config.path,
            days=config.days,
            chunk_size=config.chunk_size,
        ):
            _LOGGER.debug("%s", item.path)
    else:
        _clean_dir(
            path=config.path,
            days=config.days,
            chunk_size=config.chunk_size,
        )


@beartype
def _log_config(config: Config, /) -> None:
    for key, value in asdict(config).items():
        _LOGGER.info("%-10s = %s", key, value)


@beartype
def _clean_dir(
    *,
    path: Path = _CONFIG.path,
    days: int = _CONFIG.days,
    chunk_size: Optional[int] = _CONFIG.chunk_size,
) -> None:
    while True:
        iterator = _yield_items(path=path, days=days, chunk_size=chunk_size)
        if len(items := list(iterator)) >= 1:
            for item in items:
                item.clean()
        else:
            return


@beartype
def _yield_items(
    *,
    path: Path = _CONFIG.path,
    days: int = _CONFIG.days,
    chunk_size: Optional[int] = _CONFIG.chunk_size,
) -> Iterator[Item]:
    it = _yield_inner(path=path, days=days)
    if chunk_size is not None:
        return islice(it, chunk_size)
    return it


@beartype
def _yield_inner(
    *,
    path: Path = _CONFIG.path,
    days: int = _CONFIG.days,
) -> Iterator[Item]:
    for p in path.rglob("*"):
        yield from _yield_from_path(p, path=path, days=days)


@beartype
def _yield_from_path(
    p: Path,
    /,
    *,
    path: Path = _CONFIG.path,
    days: int = _CONFIG.days,
) -> Iterator[Item]:
    if p.is_symlink():
        yield from _yield_from_path(p.resolve(), path=path, days=days)
    elif _is_owned_and_relative(p, path=path):  # pragma: no cover
        if (p.is_file() or p.is_socket()) and _is_old(p, days=days):
            yield Item(p, partial(_unlink_path, p))
        elif p.is_dir() and _is_empty(p):
            yield Item(p, partial(_unlink_dir, p))


@beartype
def _is_owned_and_relative(p: Path, /, *, path: Path = _CONFIG.path) -> bool:
    try:
        return (p.owner() == getuser()) and p.is_relative_to(path)
    except FileNotFoundError:  # pragma: no cover
        return False


@beartype
def _is_empty(path: Path, /) -> bool:
    return len(list(path.iterdir())) == 0


@beartype
def _is_old(path: Path, /, *, days: int = _CONFIG.days) -> bool:
    age = dt.datetime.now(tz=UTC) - dt.datetime.fromtimestamp(
        path.stat().st_mtime,
        tz=UTC,
    )
    return age >= dt.timedelta(days=days)


@beartype
def _unlink_path(path: Path, /) -> None:
    _LOGGER.info("Removing file:      %s", path)
    path.unlink(missing_ok=True)


@beartype
def _unlink_dir(path: Path, /) -> None:
    _LOGGER.info("Removing directory: %s", path)
    rmtree(path)
