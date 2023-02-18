from collections.abc import Iterable, Iterator
from logging import getLogger
from pathlib import Path

from attrs import asdict
from beartype import beartype
from click import command

from utilities.logging import basic_config
from utilities.re import NoMatchesError, extract_group
from utilities.rotate_logs.classes import Config, Item
from utilities.typed_settings import click_options

_CONFIG = Config()
_LOGGER = getLogger(__name__)


@command()
@click_options(Config)
@beartype
def main(config: Config, /) -> None:
    """CLI for the `rotate_logs` script."""
    basic_config()
    _log_config(config)
    item_lists = _yield_items(
        path=config.path,
        extension=config.extension,
        size=config.size,
    )
    if config.dry_run:
        for items in item_lists:
            for item in items:
                _LOGGER.debug("%s", item)
    else:
        for items in item_lists:
            _rotate_items(items, keep=config.keep)


@beartype
def _log_config(config: Config, /) -> None:
    for key, value in asdict(config).items():
        _LOGGER.info("%-9s = %s", key, value)


@beartype
def _yield_items(
    *,
    path: Path = _CONFIG.path,
    extension: str = _CONFIG.extension,
    size: int = _CONFIG.size,
) -> Iterator[frozenset[Item]]:
    for p in path.rglob("*"):
        if p.suffix == f".{extension}" and p.stat().st_size >= size:
            yield frozenset(_yield_for_head(p, extension=extension))


@beartype
def _yield_for_head(
    path: Path,
    /,
    *,
    extension: str = _CONFIG.extension,
) -> Iterator[Item]:
    yield Item(path, path)
    pattern = rf"^{path.stem}\.{extension}\.(\d+)$"
    for p in path.parent.iterdir():
        try:
            num = extract_group(pattern, p.name)
        except NoMatchesError:
            pass
        else:
            yield Item(p, path, num=int(num))


@beartype
def _rotate_items(
    items: Iterable[Item],
    /,
    *,
    keep: int = _CONFIG.keep,
) -> None:
    for item in sorted(items, key=_key, reverse=True):
        _rotate_item(item, keep=keep)


@beartype
def _key(item: Item, /) -> int:
    return item.num


@beartype
def _rotate_item(item: Item, /, *, keep: int = _CONFIG.keep) -> None:
    path = item.path
    if item.num >= keep:
        _LOGGER.info("Removing file: %s", path)
        path.unlink(missing_ok=True)
    else:
        head = item.head
        new_num = item.num + 1
        new_path = head.with_name(f"{head.name}.{new_num}")
        _LOGGER.info("Rotating file: %s -> %s", path, new_path)
        _ = path.rename(new_path)
