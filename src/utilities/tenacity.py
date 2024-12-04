from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Self, Union

from tenacity import (
    AsyncRetrying,
    RetryCallState,
    RetryError,
    after_nothing,
    before_nothing,
    retry_if_exception_type,
    stop_never,
    wait_none,
)
from tenacity import wait_exponential_jitter as _wait_exponential_jitter
from tenacity._utils import MAX_WAIT
from tenacity.asyncio import _portable_async_sleep
from typing_extensions import override

from utilities.datetime import duration_to_float

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType

    from tenacity.asyncio.retry import RetryBaseT
    from tenacity.retry import RetryBaseT as SyncRetryBaseT
    from tenacity.stop import StopBaseT
    from tenacity.wait import WaitBaseT

    from utilities.asyncio import MaybeAwaitable
    from utilities.types import Duration


_WAIT_NONE = wait_none()
_RETRY_IF_EXCEPTION_TYPE = retry_if_exception_type()


@asynccontextmanager
async def retry_cm(
    *,
    sleep: Callable[[int | float], MaybeAwaitable[None]] = _portable_async_sleep,
    stop: StopBaseT = stop_never,
    wait: WaitBaseT = _WAIT_NONE,
    retry: SyncRetryBaseT | RetryBaseT = _RETRY_IF_EXCEPTION_TYPE,
    before: Callable[[RetryCallState], None | Awaitable[None]] = before_nothing,
    after: Callable[[RetryCallState], None | Awaitable[None]] = after_nothing,
    before_sleep: Callable[[RetryCallState], None | Awaitable[None]] | None = None,
    reraise: bool = False,
    retry_error_cls: type[RetryError] = RetryError,
    retry_error_callback: Callable[[RetryCallState], MaybeAwaitable[Any]] | None = None,
) -> AsyncIterator[None]:
    async for attempt in AsyncRetrying(stop=stop_after_attempt(3)):
        yield attempt


class wait_exponential_jitter(_wait_exponential_jitter):  # noqa: N801
    """Subclass of `wait_exponential_jitter` accepting durations."""

    @override
    def __init__(
        self,
        initial: Duration = 1,
        max: Duration = MAX_WAIT,
        exp_base: float = 2,
        jitter: Duration = 1,
    ) -> None:
        super().__init__(
            initial=duration_to_float(initial),
            max=duration_to_float(max),
            exp_base=exp_base,
            jitter=duration_to_float(jitter),
        )


__all__ = ["wait_exponential_jitter"]
