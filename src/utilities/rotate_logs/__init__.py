from collections.abc import Iterable, Iterator
from logging import getLogger
from pathlib import Path

from beartype import beartype
from click import command, option

from utilities.logging import basic_config
from utilities.re import NoMatchesError, extract_group
from utilities.rotate_logs.classes import Config, Item

_LOGGER = getLogger(__name__)


@command()
@option("-p", "--path", type=Path, default=Config.path, show_default=True)
@option(
    "-e",
    "--extension",
    type=str,
    default=Config.extension,
    show_default=True,
)
@option("-s", "--size", type=int, default=Config.size, show_default=True)
@option("-k", "--keep", type=int, default=Config.keep, show_default=True)
@option("-dr", "--dry-run", is_flag=True, show_default=True)
@beartype
def main(
    *,
    path: Path,
    extension: str,
    size: int,
    keep: int,
    dry_run: bool,
) -> None:
    """CLI for the `rotate_logs` script."""
    basic_config()

    # log config
    _LOGGER.info("path      = %s", path)
    _LOGGER.info("extension = %s", extension)
    _LOGGER.info("size      = %s", size)
    _LOGGER.info("keep      = %s", keep)
    _LOGGER.info("dry_run   = %s", dry_run)

    # main
    item_lists = _yield_items(path=path, extension=extension, size=size)
    if dry_run:
        for items in item_lists:
            for item in items:
                _LOGGER.debug("%s", item)
    else:
        for items in item_lists:
            _rotate_items(items, keep=keep)


@beartype
def _rotate_items(items: Iterable[Item], /, *, keep: int = Config.keep) -> None:
    for item in sorted(items, key=_key, reverse=True):
        _rotate_item(item, keep=keep)


@beartype
def _key(item: Item, /) -> int:
    return item.num


@beartype
def _rotate_item(item: Item, /, *, keep: int = Config.keep) -> None:
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


@beartype
def _yield_items(
    *,
    path: Path = Config.path,
    extension: str = Config.extension,
    size: int = Config.size,
) -> Iterator[frozenset[Item]]:
    for p in path.rglob("*"):
        if p.suffix == f".{extension}" and p.stat().st_size >= size:
            yield frozenset(_yield_for_head(p, extension=extension))


@beartype
def _yield_for_head(
    path: Path,
    /,
    *,
    extension: str = Config.extension,
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
