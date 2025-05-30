from __future__ import annotations

from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from json import dumps
from logging import getLogger
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, override

from psutil import swap_memory, virtual_memory

from utilities.asyncio import Looper
from utilities.datetime import (
    MINUTE,
    SECOND,
    datetime_duration_to_timedelta,
    get_now,
    sub_duration,
)

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Set as AbstractSet
    from logging import Logger

    from utilities.types import Duration, PathLike


@dataclass(kw_only=True)
class MemoryUsageMonitor(Looper[None]):
    """Monitor memory usage."""

    # base
    freq: Duration = field(default=10 * SECOND, repr=False)
    backoff: Duration = field(default=10 * SECOND, repr=False)
    # self
    console: str | None = field(default=None, repr=False)
    path: PathLike = "memory.txt"
    averages: AbstractSet[Duration] | None = {MINUTE}
    _cache: deque[_MemoryUsage] = field(default_factory=deque, init=False, repr=False)
    _console: Logger | None = field(init=False, repr=False)
    _max_age: dt.timedelta | None = field(default=None, init=False, repr=False)
    _path: Path = field(init=False, repr=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.averages is not None:
            averages = set(map(datetime_duration_to_timedelta, self.averages))
            self._max_age = max(averages, default=None)
        if self.console is not None:
            self._console = getLogger(self.console)
        self._path = Path(self.path)

    @override
    async def core(self) -> None:
        await super().core()
        virtual = virtual_memory()
        virtual_total = virtual.total
        swap = swap_memory()
        usage = _MemoryUsage(
            virtual_used=virtual_total - virtual.available,
            virtual_total=virtual_total,
            swap_used=swap.used,
            swap_total=swap.total,
        )
        self._cache.append(usage)
        if self._max_age is not None:
            min_datetime = sub_duration(get_now(), duration=self._max_age)
            while (len(self._cache) >= 1) and (min_datetime <= self._cache[0].datetime):
                _ = self._cache.popleft()
        mapping = {
            "datetime": usage.datetime,
            "virtual used (mb)": usage.virtual_used_mb,
            "virtual total (mb)": usage.virtual_total_mb,
            "virtual (%)": usage.virtual_pct,
            "swap used (mb)": usage.swap_used_mb,
            "swap total (mb)": usage.swap_total_mb,
            "swap (%)": usage.swap_pct,
        }
        if self.averages is not None:
            for average in self.averages:
                min_datetime = sub_duration(get_now(), duration=average)
                usages = [u for u in self._cache if min_datetime <= u.datetime]
                mapping[f"virtual ({average}, %)"] = mean(u.virtual_pct for u in usages)
                mapping[f"swap ({average}, %)"] = mean(u.swap_pct for u in usages)
        with self._path.open(mode="+a") as fh:
            _ = fh.write(dumps(mapping))
        if self._console is not None:
            self._console.info("%s", mapping)


@dataclass(kw_only=True)
class _MemoryUsage:
    """A memory usage."""

    datetime: dt.datetime = field(default_factory=get_now)
    virtual_used: int = field(repr=False)
    virtual_used_mb: int = field(init=False)
    virtual_total: int = field(repr=False)
    virtual_total_mb: int = field(init=False)
    virtual_pct: float = field(init=False)
    swap_used: int = field(repr=False)
    swap_used_mb: int = field(init=False)
    swap_total: int = field(repr=False)
    swap_total_mb: int = field(init=False)
    swap_pct: float = field(init=False)

    @override
    def __post_init__(self) -> None:
        # TODO: use reraise mixin
        with suppress():
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self.virtual_used_mb = self._to_mb(self.virtual_used)
        self.virtual_total_mb = self._to_mb(self.virtual_total)
        self.virtual_pct = self.virtual_used / self.virtual_total
        self.swap_used_mb = self._to_mb(self.swap_used)
        self.swap_total_mb = self._to_mb(self.swap_total)
        self.swap_pct = self.swap_used / self.swap_total

    def _to_mb(self, bytes_: int) -> int:
        return round(bytes_ / (1024**2))
