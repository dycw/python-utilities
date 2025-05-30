from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from utilities.asyncio import Looper, sleep_dur
from utilities.datetime import SECOND

if TYPE_CHECKING:
    from utilities.types import Duration, PathLike


@dataclass(kw_only=True)
class MemoryUsageMonitor(Looper[None]):
    """Monitor memory usage."""

    # base
    freq: Duration = field(default=10 * SECOND, repr=False)
    backoff: Duration = field(default=10 * SECOND, repr=False)
    logger: str | None = field(default=__name__, repr=False)
    path: PathLike = field(default="memory.txt")
    # self
    database: DatabaseAsyncLitOrEngine = DATABASE_LOCAL_OR_CLOUD
    log: bool = False
    _upserter: Upserter = field(init=False, repr=False)
    _rows: list[MemoryUsage] = field(default_factory=list)

    @override
    def __post_init__(self) -> None:
        UpserterMixin.__post_init__(self)
        InfiniteLooper.__post_init__(self)

    @override
    async def _core(self) -> None:
        virtual = virtual_memory()
        virtual_total = virtual.total
        swap = swap_memory()
        scale = 1024**2
        row = MemoryUsage(
            datetime=get_now(),
            user=USER,
            host=HOSTNAME,
            virtual_used_mb=round((virtual_total - virtual.available) / scale),
            virtual_total_mb=round(virtual_total / scale),
            swap_used_mb=round(swap.used / scale),
            swap_total_mb=round(swap.total / scale),
        )
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
    ) -> Sequence[Expr]:
        name = key if abbreviation is None else abbreviation
        used = col(f"{key}_used_mb").alias(f"{name}-used (MB)")
        total = col(f"{key}_total_mb").alias(f"{name}-total (MB)")
        pct = (used / total).alias(f"{name} (%)")
        pct10m = pct.rolling_mean_by(DATETIME, MINUTE).alias(f"{name} (%-10m)")
        pct1h = pct.rolling_mean_by(DATETIME, MINUTE).alias(f"{name} (%-1h)")
        return [used, total, pct, pct10m, pct1h]

    @override
    def _yield_loopers(self) -> Iterator[InfiniteLooper[Any]]:
        yield from UpserterMixin._yield_loopers(self)  # noqa: SLF001
