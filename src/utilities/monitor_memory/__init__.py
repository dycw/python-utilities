import dataclasses
import datetime as dt
from collections.abc import Iterator
from contextlib import contextmanager
from csv import DictWriter
from dataclasses import fields
from pathlib import Path
from time import sleep
from typing import Any, Optional, cast

import attrs
from beartype import beartype
from click import command
from loguru import logger
from psutil import swap_memory, virtual_memory

from utilities.datetime import UTC
from utilities.loguru import setup_loguru
from utilities.monitor_memory.classes import Config, Item
from utilities.timer import Timer
from utilities.typed_settings import click_options

_CONFIG = Config()


@command()
@click_options(Config, appname="monitormemory")
@beartype
def main(config: Config, /) -> None:
    """CLI for the `clean_dir` script."""
    setup_loguru()
    _log_config(config)
    _monitor_memory(path=config.path, freq=config.freq, duration=config.duration)


@beartype
def _log_config(config: Config, /) -> None:
    for key, value in attrs.asdict(config).items():
        logger.info("{key:8} = {value}", key=key, value=value)


@beartype
def _monitor_memory(
    *,
    path: Path = _CONFIG.path,
    freq: int = _CONFIG.freq,
    duration: Optional[int] = _CONFIG.duration,
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
@beartype
def _yield_writer(
    *, path: Path = _CONFIG.path, mode: str = "r"
) -> Iterator[DictWriter]:
    fieldnames = [f.name for f in fields(cast(Any, Item))]
    with path.open(mode=mode) as fh:
        yield DictWriter(fh, fieldnames=fieldnames)


@beartype
def _get_memory_usage() -> Item:
    virtual = virtual_memory()
    swap = swap_memory()
    return Item(
        datetime=dt.datetime.now(tz=UTC),
        virtual_total=virtual.total,
        virtual_available=virtual.available,
        virtual_percent=virtual.percent,
        virtual_used=virtual.used,
        virtual_free=virtual.free,
        virtual_active=virtual.active,
        virtual_inactive=virtual.inactive,
        virtual_buffers=virtual.buffers,
        virtual_cached=virtual.cached,
        virtual_shared=virtual.shared,
        virtual_slab=virtual.slab,
        swap_total=swap.total,
        swap_used=swap.used,
        swap_free=swap.free,
        swap_percent=swap.percent,
        swap_sin=swap.sin,
        swap_sout=swap.sout,
    )
