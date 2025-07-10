from __future__ import annotations

from dataclasses import dataclass, field
from math import isclose, nan
from typing import TYPE_CHECKING, Self

from psutil import swap_memory, virtual_memory

from utilities.contextlib import suppress_super_object_attribute_error
from utilities.whenever import get_now

if TYPE_CHECKING:
    from whenever import ZonedDateTime


@dataclass(order=True, unsafe_hash=True, kw_only=True)
class MemoryUsage:
    """A memory usage."""

    datetime: ZonedDateTime = field(default_factory=get_now)
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


__all__ = ["MemoryUsage"]
