from __future__ import annotations

from typing import TYPE_CHECKING

from aiolimiter import AsyncLimiter

if TYPE_CHECKING:
    from collections.abc import Hashable

_LIMITERS: dict[Hashable, AsyncLimiter] = {}


def get_async_limiter(key: Hashable, /, *, rate: float = 1.0) -> AsyncLimiter:
    """Get a loop-aware rate limiter."""
    try:
        return _LIMITERS[key]
    except KeyError:
        limiter = _LIMITERS[key] = AsyncLimiter(1.0, time_period=rate)
        return limiter


__all__ = ["get_async_limiter"]
