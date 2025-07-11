from __future__ import annotations

import sys
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from pottery import AIORedlock
from pottery.exceptions import ReleaseUnlockedLock
from redis.asyncio import Redis

from utilities.asyncio import sleep_td, timeout_td
from utilities.contextlib import enhanced_async_context_manager
from utilities.errors import ImpossibleCaseError
from utilities.iterables import always_iterable
from utilities.logging import get_logger
from utilities.warnings import suppress_warnings
from utilities.whenever import MILLISECOND, SECOND, to_seconds

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable

    from whenever import Delta

    from utilities.types import Coro, LoggerOrName, MaybeIterable

_NUM: int = 1
_TIMEOUT_ACQUIRE: Delta | None = None
_TIMEOUT_RELEASE: Delta = 10 * SECOND
_SLEEP: Delta = MILLISECOND
_THROTTLE: Delta | None = None


##


async def run_as_service(
    redis: MaybeIterable[Redis],
    make_func: Callable[[], Coro[None]],
    /,
    *,
    key: str | None = None,
    num: int = _NUM,
    timeout_acquire: Delta | None = _TIMEOUT_ACQUIRE,
    timeout_release: Delta = _TIMEOUT_RELEASE,
    sleep_access: Delta = _SLEEP,
    throttle: Delta | None = _THROTTLE,
    logger: LoggerOrName | None = None,
    sleep_error: Delta | None = None,
) -> None:
    """Run a function as a service."""
    func = make_func()  # skipif-ci-and-not-linux
    name = func.__name__  # skipif-ci-and-not-linux
    with suppress_warnings(
        message="coroutine '.*' was never awaited", category=RuntimeWarning
    ):
        del func
    try:  # skipif-ci-and-not-linux
        async with (
            yield_access(
                redis,
                name if key is None else key,
                num=num,
                timeout_acquire=timeout_acquire,
                timeout_release=timeout_release,
                sleep=sleep_access,
                throttle=throttle,
            ),
            timeout_td(timeout_release),
        ):
            while True:
                try:
                    return await make_func()
                except Exception:  # noqa: BLE001
                    if logger is not None:
                        get_logger(logger=logger).exception(
                            "Error running %r as a service", name
                        )
                    exc_type, exc_value, traceback = sys.exc_info()
                    if (exc_type is None) or (exc_value is None):  # pragma: no cover
                        raise ImpossibleCaseError(
                            case=[f"{exc_type=}", f"{exc_value=}"]
                        ) from None
                    sys.excepthook(exc_type, exc_value, traceback)
                    await sleep_td(sleep_error)
    except _YieldAccessUnableToAcquireLockError as error:  # skipif-ci-and-not-linux
        if logger is not None:
            get_logger(logger=logger).info("%s", error)


##


@enhanced_async_context_manager
async def yield_access(
    redis: MaybeIterable[Redis],
    key: str,
    /,
    *,
    num: int = _NUM,
    timeout_acquire: Delta | None = _TIMEOUT_ACQUIRE,
    timeout_release: Delta = _TIMEOUT_RELEASE,
    sleep: Delta = _SLEEP,
    throttle: Delta | None = _THROTTLE,
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
            auto_release_time=to_seconds(timeout_release),
        )
        for i in range(1, num + 1)
    ]
    lock: AIORedlock | None = None  # skipif-ci-and-not-linux
    try:  # skipif-ci-and-not-linux
        lock = await _get_first_available_lock(
            key, locks, num=num, timeout=timeout_acquire, sleep=sleep
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
    num: int = _NUM,
    timeout: Delta | None = _TIMEOUT_ACQUIRE,
    sleep: Delta | None = _SLEEP,
) -> AIORedlock:
    locks = list(locks)  # skipif-ci-and-not-linux
    error = _YieldAccessUnableToAcquireLockError(  # skipif-ci-and-not-linux
        key=key, num=num, timeout=timeout
    )
    async with timeout_td(timeout, error=error):  # skipif-ci-and-not-linux
        while True:
            if (result := await _get_first_available_lock_if_any(locks)) is not None:
                return result
            await sleep_td(sleep)


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
    timeout: Delta | None

    @override
    def __str__(self) -> str:
        return f"Unable to acquire any 1 of {self.num} locks for {self.key!r} after {self.timeout}"  # skipif-ci-and-not-linux


__all__ = ["YieldAccessError", "yield_access"]
