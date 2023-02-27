import datetime as dt
from collections.abc import Iterable
from collections.abc import Iterator
from functools import partial
from getpass import getuser
from itertools import islice
from pathlib import Path
from shutil import rmtree
from typing import Optional

from attrs import asdict
from beartype import beartype
from click import command
from loguru import logger
from typed_settings import find

from utilities.clean_dir.classes import Config
from utilities.clean_dir.classes import Item
from utilities.datetime import UTC
from utilities.loguru import setup_loguru
from utilities.pathlib import PathLike
from utilities.typed_settings import click_options

_CONFIG = Config()


@command()
@click_options(Config, appname="cleandir", config_files=[find("config.toml")])
@beartype
def main(config: Config, /) -> None:
    """CLI for the `clean_dir` script."""
    setup_loguru()
    _log_config(config)
    if config.dry_run:
        for item in _yield_items(
            paths=config.paths,
            days=config.days,
            chunk_size=config.chunk_size,
        ):
            logger.debug("{path}", path=item.path)
    else:
        _clean_dir(
            paths=config.paths,
            days=config.days,
            chunk_size=config.chunk_size,
        )


@beartype
def _log_config(config: Config, /) -> None:
    for key, value in asdict(config).items():
        logger.info("{key:10} = {value}", key=key, value=value)


@beartype
def _clean_dir(
    *,
    paths: Iterable[PathLike] = _CONFIG.paths,
    days: int = _CONFIG.days,
    chunk_size: Optional[int] = _CONFIG.chunk_size,
) -> None:
    while True:
        iterator = _yield_items(paths=paths, days=days, chunk_size=chunk_size)
        if len(items := list(iterator)) >= 1:
            for item in items:
                item.clean()
        else:
            return


@beartype
def _yield_items(
    *,
    paths: Iterable[PathLike] = _CONFIG.paths,
    days: int = _CONFIG.days,
    chunk_size: Optional[int] = _CONFIG.chunk_size,
) -> Iterator[Item]:
    it = _yield_inner(paths=paths, days=days)
    if chunk_size is not None:
        return islice(it, chunk_size)
    return it


@beartype
def _yield_inner(
    *,
    paths: Iterable[PathLike] = _CONFIG.paths,
    days: int = _CONFIG.days,
) -> Iterator[Item]:
    for path in map(Path, paths):
        for p in path.rglob("*"):
            yield from _yield_from_path(p, path, days=days)


@beartype
def _yield_from_path(
    p: Path,
    path: Path,
    /,
    *,
    days: int = _CONFIG.days,
) -> Iterator[Item]:
    p, path = map(Path, [p, path])
    if p.is_symlink():
        yield from _yield_from_path(p.resolve(), path, days=days)
    elif _is_owned_and_relative(p, path):  # pragma: no cover
        if (p.is_file() or p.is_socket()) and _is_old(p, days=days):
            yield Item(p, partial(_unlink_path, p))
        elif p.is_dir() and _is_empty(p):
            yield Item(p, partial(_unlink_dir, p))


@beartype
def _is_owned_and_relative(p: Path, path: Path, /) -> bool:
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
    logger.info("Removing file:      {path}", path=path)
    path.unlink(missing_ok=True)


@beartype
def _unlink_dir(path: Path, /) -> None:
    logger.info("Removing directory: {path}", path=path)
    rmtree(path, ignore_errors=True)
