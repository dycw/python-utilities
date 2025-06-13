from __future__ import annotations

from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock
from redis.asyncio import Redis

from utilities.asyncio import sleep_td, timeout_td
from utilities.iterables import always_iterable
from utilities.whenever2 import MILLISECOND, SECOND

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from whenever import TimeDelta

    from utilities.types import MaybeIterable


@asynccontextmanager
async def yield_access(
    redis: MaybeIterable[Redis],
    key: str,
    /,
    *,
    num: int = 1,
    timeout_acquire: TimeDelta | None = None,
    timeout_release: TimeDelta = 10 * SECOND,
    sleep_delay: TimeDelta = MILLISECOND,
    throttle: TimeDelta | None = None,
) -> AsyncIterator[None]:
    """Acquire access to a locked resource, amongst 1 of multiple connections."""
    if num <= 0:
        raise _YieldAccessNumLocksError(key=key, num=num)
    masters = (  # skipif-ci-and-not-linux
        {redis} if isinstance(redis, Redis) else set(always_iterable(redis))
    )
    locks = [  # skipif-ci-and-not-linux
        AIORedlock(
            key=f"{key}_{i}_of_{num}",
            masters=masters,
            auto_release_time=timeout_release.in_seconds(),
        )
        for i in range(1, num + 1)
    ]
    lock: AIORedlock | None = None  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        lock = await _get_first_available_lock(
            key, locks, num=num, timeout_delay=timeout_acquire, sleep_delay=sleep_delay
        )
        yield
    finally:  # skipif-ci-and-not-linux
        await sleep_td(throttle)
        if lock is not None:
            with suppress(ReleaseUnlockedLock):
                await lock.release()


async def _get_first_available_lock(
    key: str,
    locks: Iterable[AIORedlock],
    /,
    *,
    num: int = 1,
    timeout_delay: TimeDelta | None = None,
    sleep_delay: TimeDelta | None = None,
) -> AIORedlock:
    locks = list(locks)  # skipif-ci-and-not-linux
    error = _YieldAccessUnableToAcquireLockError(  # skipif-ci-and-not-linux
        key=key, num=num, delay=timeout_delay
    )
    async with timeout_td(timeout_delay, error=error):  # skipif-ci-and-not-linux
        while True:
            if (result := await _get_first_available_lock_if_any(locks)) is not None:
                return result
            await sleep_td(sleep_delay)


async def _get_first_available_lock_if_any(
    locks: Iterable[AIORedlock], /
) -> AIORedlock | None:
    for lock in locks:  # skipif-ci-and-not-linux
        if await lock.acquire(blocking=False):
            return lock
    return None  # skipif-ci-and-not-linux


@dataclass(kw_only=True, slots=True)
class YieldAccessError(Exception):
    key: str
    num: int


@dataclass(kw_only=True, slots=True)
class _YieldAccessNumLocksError(YieldAccessError):
    @override
    def __str__(self) -> str:
        return f"Number of locks for {self.key!r} must be positive; got {self.num}"


@dataclass(kw_only=True, slots=True)
class _YieldAccessUnableToAcquireLockError(YieldAccessError):
    delay: TimeDelta | None

    @override
    def __str__(self) -> str:
        return f"Unable to acquire any 1 of {self.num} locks for {self.key!r} after {self.delay}"  # skipif-ci-and-not-linux


__all__ = ["YieldAccessError", "yield_access"]
