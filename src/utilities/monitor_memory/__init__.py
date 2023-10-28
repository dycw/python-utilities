from __future__ import annotations

import dataclasses
import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from csv import DictWriter
from dataclasses import fields
from pathlib import Path
from time import sleep
from typing import Any, cast

import attrs
from click import command
from loguru import logger
from psutil import swap_memory, virtual_memory

from utilities.datetime import UTC
from utilities.loguru import setup_loguru
from utilities.monitor_memory.classes import Config, Item
from utilities.platform import SYSTEM, System
from utilities.timer import Timer
from utilities.typed_settings import click_options
from utilities.typing import never

_CONFIG = Config()


@command()
@click_options(Config, appname="monitormemory")
def main(config: Config, /) -> None:
    """CLI for the `clean_dir` script."""
    setup_loguru()
    _log_config(config)
    _monitor_memory(path=config.path, freq=config.freq, duration=config.duration)


def _log_config(config: Config, /) -> None:
    for key, value in attrs.asdict(config).items():
        logger.info("{key:8} = {value}", key=key, value=value)


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
) -> Iterator[DictWriter[Any]]:
    fieldnames = [f.name for f in fields(cast(Any, Item))]
    with path.open(mode=mode) as fh:
        yield DictWriter(fh, fieldnames=fieldnames)


def _get_memory_usage() -> Item:
    virtual = cast(Any, virtual_memory())
    virtual_kwargs: dict[str, Any] = {}
    if SYSTEM is System.windows:  # pragma: os-ne-windows
        pass
    elif SYSTEM is System.mac_os:  # pragma: os-ne-macos
        virtual_kwargs["virtual_active"] = virtual.active
        virtual_kwargs["virtual_inactive"] = virtual.inactive
        virtual_kwargs["virtual_wired"] = virtual.wired
    elif SYSTEM is System.linux:  # pragma: os-ne-linux
        virtual_kwargs["virtual_active"] = virtual.active
        virtual_kwargs["virtual_inactive"] = virtual.inactive
        virtual_kwargs["virtual_buffers"] = virtual.buffers
        virtual_kwargs["virtual_cached"] = virtual.cached
        virtual_kwargs["virtual_shared"] = virtual.shared
        virtual_kwargs["virtual_slab"] = virtual.slab
    else:  # pragma: no cover
        never(SYSTEM)
    swap = swap_memory()
    return Item(
        datetime=dt.datetime.now(tz=UTC),
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
