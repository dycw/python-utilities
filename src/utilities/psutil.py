from __future__ import annotations

from dataclasses import dataclass, field
from json import dumps
from logging import getLogger
from math import isclose, nan
from pathlib import Path
from typing import TYPE_CHECKING, Self, override

from psutil import swap_memory, virtual_memory

from utilities.asyncio import Looper
from utilities.contextlib import suppress_super_object_attribute_error
from utilities.datetime import SECOND, get_now

if TYPE_CHECKING:
    import datetime as dt
    from logging import Logger

    from utilities.types import Duration, PathLike


@dataclass(kw_only=True)
class MemoryMonitorService(Looper[None]):
    """Service to monitor memory usage."""

    # base
    freq: Duration = field(default=10 * SECOND, repr=False)
    backoff: Duration = field(default=10 * SECOND, repr=False)
    # self
    console: str | None = field(default=None, repr=False)
    path: PathLike = "memory.txt"
    _console: Logger | None = field(init=False, repr=False)
    _path: Path = field(init=False, repr=False)

    @override
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.console is not None:
            self._console = getLogger(self.console)
        self._path = Path(self.path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @override
    async def core(self) -> None:
        await super().core()
        memory = MemoryUsage.new()
        mapping = {
            "datetime": memory.datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "virtual used (mb)": memory.virtual_used_mb,
            "virtual total (mb)": memory.virtual_total_mb,
            "virtual (%)": memory.virtual_pct,
            "swap used (mb)": memory.swap_used_mb,
            "swap total (mb)": memory.swap_total_mb,
            "swap (%)": memory.swap_pct,
        }
        ser = dumps(mapping)
        with self._path.open(mode="a") as fh:
            _ = fh.write(f"{ser}\n")
        if self._console is not None:
            self._console.info("%s", mapping)


##


@dataclass(kw_only=True)
class MemoryUsage:
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

    def __post_init__(self) -> None:
        with suppress_super_object_attribute_error():
            super().__post_init__()  # pyright: ignore[reportAttributeAccessIssue]
        self.virtual_used_mb = self._to_mb(self.virtual_used)
        self.virtual_total_mb = self._to_mb(self.virtual_total)
        self.virtual_pct = (
            nan
            if isclose(self.virtual_total, 0.0)
            else self.virtual_used / self.virtual_total
        )
        self.swap_used_mb = self._to_mb(self.swap_used)
        self.swap_total_mb = self._to_mb(self.swap_total)
        self.swap_pct = (
            nan if isclose(self.swap_total, 0.0) else self.swap_used / self.swap_total
        )

    @classmethod
    def new(cls) -> Self:
        virtual = virtual_memory()
        virtual_total = virtual.total
        swap = swap_memory()
        return cls(
            virtual_used=virtual_total - virtual.available,
            virtual_total=virtual_total,
            swap_used=swap.used,
            swap_total=swap.total,
        )

    def _to_mb(self, bytes_: int) -> int:
        return round(bytes_ / (1024**2))


__all__ = ["MemoryMonitorService", "MemoryUsage"]
