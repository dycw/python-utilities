from __future__ import annotations

from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AbstractSet, override

from psutil import swap_memory, virtual_memory

from utilities.asyncio import Looper, sleep_dur
from utilities.datetime import MINUTE, SECOND, datetime_duration_to_timedelta, get_now

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Sequence

    from utilities.types import Duration, PathLike


@dataclass(kw_only=True)
class MemoryUsageMonitor(Looper[None]):
    """Monitor memory usage."""

    # base
    freq: Duration = field(default=10 * SECOND, repr=False)
    backoff: Duration = field(default=10 * SECOND, repr=False)
    logger: str | None = field(default=__name__, repr=False)
    # self
    console: bool = False
    path: PathLike = "memory.txt"
    averages: AbstractSet[Duration] | None = {MINUTE}
    _max_duration: dt.timedelta | None = field(default=None, init=False, repr=False)
    _cache: deque[_MemoryUsage] = field(default_factory=deque, init=False, repr=False)
    _scale: int = field(default=1024**2, init=False, repr=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.averages is not None:
            averages = set(map(datetime_duration_to_timedelta, self.averages))
            self._max_duration = max(averages, default=None)

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
        self._rows.append(row)
        self._upserter.put_right_nowait(row)
        if self.log:
            _LOGGER.info("\n%s", pretty_repr(self._df.row(-1, named=True)))

    @property
    def _df(self) -> DataFrame:
        df = DataFrame(
            data=self._rows,
            schema={
                DATETIME: DatetimeUTC,
                "user": String,
                "host": String,
                "virtual_used_mb": Int64,
                "virtual_total_mb": Int64,
                "swap_used_mb": Int64,
                "swap_total_mb": Int64,
            },
            orient="row",
        )
        return df.select(
            col(DATETIME).dt.convert_time_zone(get_time_zone_name("local")),
            *self._df_columns("virtual", abbreviation="virt"),
            *self._df_columns("swap"),
        )

    def _df_columns(
        self, key: str, /, *, abbreviation: str | None = None
    ) -> SequenceExpr:
        name = key if abbreviation is None else abbreviation
        used = col(f"{key}_used_mb").alias(f"{name}-used (MB)")
        total = col(f"{key}_total_mb").alias(f"{name}-total (MB)")
        pct = (used / total).alias(f"{name} (%)")
        pct10m = pct.rolling_mean_by(DATETIME, MINUTE).alias(f"{name} (%-10m)")
        pct1h = pct.rolling_mean_by(DATETIME, MINUTE).alias(f"{name} (%-1h)")
        return [used, total, pct, pct10m, pct1h]


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
