from __future__ import annotations

from asyncio import get_running_loop
from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter

if TYPE_CHECKING:
    from collections.abc import Hashable

_LIMITERS: dict[Hashable, AsyncLimiter] = {}


def get_async_limiter(key: Hashable, /, *, max_rate: float = 1.0) -> AsyncLimiter:
    """Get a loop-aware rate limiter."""
    try:
        loop = get_running_loop()
    except RuntimeError:
        return AsyncLimiter(max_rate, time_period=1.0)
    key = id(loop)
    try:
        return _LIMITERS[key]
    except KeyError:
        limiter = _LIMITERS[key] = AsyncLimiter(max_rate, time_period=1.0)
        return limiter


__all__ = ["get_async_limiter"]
