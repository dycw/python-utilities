from __future__ import annotations

from asyncio import get_running_loop
from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter

if TYPE_CHECKING:
    from collections.abc import Hashable

_LIMITERS: dict[tuple[int, Hashable], AsyncLimiter] = {}


def get_async_limiter(key: Hashable, /, *, rate: float = 1.0) -> AsyncLimiter:
    """Get a loop-aware rate limiter."""
    id_ = id(get_running_loop())
    full = (id_, key)
    try:
        return _LIMITERS[full]
    except KeyError:
        limiter = _LIMITERS[full] = AsyncLimiter(1.0, time_period=rate)
        return limiter


__all__ = ["get_async_limiter"]
