from __future__ import annotations

import datetime as dt
from functools import partial
from getpass import getuser
from itertools import islice
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING

from click import command
from loguru import logger
from typed_settings import find

from utilities.datetime import get_now
from utilities.loguru import setup_loguru
from utilities.scripts.clean_dir.classes import Config, Item
from utilities.typed_settings import click_options
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from utilities.types import PathLike

_CONFIG = Config()


@command()
@click_options(Config, appname="cleandir", config_files=[find("config.toml")])
def main(config: Config, /) -> None:
    """CLI for the `clean_dir` script."""
    setup_loguru()  # pragma: os-ne-windows
    if config.dry_run:  # pragma: os-ne-windows
        for item in _yield_items(  # pragma: os-ne-windows
            paths=config.paths, days=config.days, chunk_size=config.chunk_size
        ):
            logger.debug("{path}", path=item.path)  # pragma: os-ne-windows
    else:
        _clean_dir(  # pragma: os-ne-windows
            paths=config.paths, days=config.days, chunk_size=config.chunk_size
        )


def _clean_dir(
    *,
    paths: Iterable[PathLike] = _CONFIG.paths,
    days: int = _CONFIG.days,
    chunk_size: int | None = _CONFIG.chunk_size,
) -> None:
    while True:  # pragma: os-ne-windows
        iterator = _yield_items(  # pragma: os-ne-windows
            paths=paths, days=days, chunk_size=chunk_size
        )
        if len(items := list(iterator)) >= 1:  # pragma: os-ne-windows
            for item in items:  # pragma: os-ne-windows
                item.clean()  # pragma: os-ne-windows
        else:
            return


def _yield_items(
    *,
    paths: Iterable[PathLike] = _CONFIG.paths,
    days: int = _CONFIG.days,
    chunk_size: int | None = _CONFIG.chunk_size,
) -> Iterator[Item]:
    it = _yield_inner(paths=paths, days=days)  # pragma: os-ne-windows
    if chunk_size is not None:  # pragma: os-ne-windows
        return islice(it, chunk_size)  # pragma: os-ne-windows
    return it  # pragma: os-ne-windows


def _yield_inner(
    *, paths: Iterable[PathLike] = _CONFIG.paths, days: int = _CONFIG.days
) -> Iterator[Item]:
    for path in map(Path, paths):  # pragma: os-ne-windows
        for p in path.rglob("*"):  # pragma: os-ne-windows
            yield from _yield_from_path(p, path, days=days)  # pragma: os-ne-windows


def _yield_from_path(
    p: Path, path: Path, /, *, days: int = _CONFIG.days
) -> Iterator[Item]:
    p, path = map(Path, [p, path])  # pragma: os-ne-windows
    if p.is_symlink():  # pragma: os-ne-windows
        yield from _yield_from_path(
            p.resolve(), path, days=days
        )  # pragma: os-ne-windows
    elif _is_owned_and_relative(p, path):  # pragma: no cover
        if (p.is_file() or p.is_socket()) and _is_old(p, days=days):
            yield Item(p, partial(_unlink_path, p))
        elif p.is_dir() and _is_empty(p):
            yield Item(p, partial(_unlink_dir, p))


def _is_owned_and_relative(p: Path, path: Path, /) -> bool:
    try:  # pragma: os-ne-windows
        return (p.owner() == getuser()) and p.is_relative_to(path)
    except FileNotFoundError:  # pragma: no cover
        return False


def _is_empty(path: Path, /) -> bool:
    return len(list(path.iterdir())) == 0  # pragma: os-ne-windows


def _is_old(path: Path, /, *, days: int = _CONFIG.days) -> bool:
    age = get_now(time_zone=UTC) - dt.datetime.fromtimestamp(  # pragma: os-ne-windows
        path.stat().st_mtime, tz=UTC
    )
    return age >= dt.timedelta(days=days)  # pragma: os-ne-windows


def _unlink_path(path: Path, /) -> None:
    logger.info("Removing file:      {path}", path=path)  # pragma: os-ne-windows
    path.unlink(missing_ok=True)  # pragma: os-ne-windows


def _unlink_dir(path: Path, /) -> None:
    logger.info("Removing directory: {path}", path=path)  # pragma: os-ne-windows
    rmtree(path, ignore_errors=True)  # pragma: os-ne-windows
