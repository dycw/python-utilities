from __future__ import annotations

import dataclasses
import datetime as dt
from contextlib import contextmanager
from csv import DictWriter
from dataclasses import fields
from pathlib import Path  # noqa: TCH003
from time import sleep
from typing import TYPE_CHECKING, Any, cast

from click import command
from loguru import logger
from psutil import swap_memory, virtual_memory
from typing_extensions import assert_never

from utilities.datetime import UTC, get_now
from utilities.loguru import setup_loguru
from utilities.platform import SYSTEM, System
from utilities.scripts.monitor_memory.classes import Config, Item
from utilities.timer import Timer
from utilities.typed_settings import click_options

if TYPE_CHECKING:
    from collections.abc import Iterator

_CONFIG = Config()


@command()
@click_options(Config, appname="monitormemory")
def main(config: Config, /) -> None:
    """CLI for the `monitor_memory` script."""
    setup_loguru()
    _monitor_memory(path=config.path, freq=config.freq, duration=config.duration)


def _monitor_memory(
    *,
    path: Path = _CONFIG.path,
    freq: int = _CONFIG.freq,
    duration: int | None = _CONFIG.duration,
) -> None:
    max_timedelta = None if duration is None else dt.timedelta(seconds=duration)
    timer = Timer()
    while True:
        with _yield_writer(path=path, mode="w") as writer:
            writer.writeheader()
        if (max_timedelta is None) or (timer.timedelta <= max_timedelta):
            memory = _get_memory_usage()
            logger.info("{memory}", memory=memory)
            with _yield_writer(path=path, mode="a") as writer:
                writer.writerow(dataclasses.asdict(memory))
            sleep(freq)
        else:
            return


@contextmanager
def _yield_writer(
    *, path: Path = _CONFIG.path, mode: str = "r"
) -> Iterator[DictWriter]:
    fieldnames = [f.name for f in fields(cast(Any, Item))]
    with path.open(mode=mode) as fh:
        yield DictWriter(fh, fieldnames=fieldnames)


def _get_memory_usage() -> Item:  # type: ignore[]
    virtual = cast(Any, virtual_memory())
    virtual_kwargs: dict[str, Any] = {}
    match SYSTEM:
        case System.windows:  # pragma: os-ne-windows
            pass
        case System.mac:  # pragma: os-ne-macos
            virtual_kwargs["virtual_active"] = virtual.active
            virtual_kwargs["virtual_inactive"] = virtual.inactive
            virtual_kwargs["virtual_wired"] = virtual.wired
        case System.linux:  # pragma: os-ne-linux
            virtual_kwargs["virtual_active"] = virtual.active
            virtual_kwargs["virtual_inactive"] = virtual.inactive
            virtual_kwargs["virtual_buffers"] = virtual.buffers
            virtual_kwargs["virtual_cached"] = virtual.cached
            virtual_kwargs["virtual_shared"] = virtual.shared
            virtual_kwargs["virtual_slab"] = virtual.slab
        case _ as never:  # type: ignore[]
            assert_never(never)
    swap = swap_memory()
    return Item(
        datetime=get_now(tz=UTC),
        virtual_total=virtual.total,
        virtual_available=virtual.available,
        virtual_percent=virtual.percent,
        virtual_used=virtual.used,
        virtual_free=virtual.free,
        **virtual_kwargs,
        swap_total=swap.total,
        swap_used=swap.used,
        swap_free=swap.free,
        swap_percent=swap.percent,
        swap_sin=swap.sin,
        swap_sout=swap.sout,
    )
