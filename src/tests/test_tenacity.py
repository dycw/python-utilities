from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from pytest import raises
from tenacity import RetryError, stop_after_attempt

from utilities.asyncio import sleep_delta, timeout_delta
from utilities.hypothesis import time_deltas
from utilities.tenacity import (
    wait_exponential_jitter,
    yield_attempts,
    yield_timeout_attempts,
)
from utilities.whenever import SECOND, ZERO_TIME

if TYPE_CHECKING:
    from whenever import TimeDelta


class TestWaitExponentialJitter:
    @given(
        initial=time_deltas(min_value=ZERO_TIME, max_value=SECOND),
        max_=time_deltas(min_value=ZERO_TIME, max_value=SECOND),
        exp_base=time_deltas(min_value=ZERO_TIME, max_value=SECOND),
        jitter=time_deltas(min_value=ZERO_TIME, max_value=SECOND),
    )
    def test_main(
        self,
        *,
        initial: TimeDelta,
        max_: TimeDelta,
        exp_base: TimeDelta,
        jitter: TimeDelta,
    ) -> None:
        wait = wait_exponential_jitter(
            initial=initial, max=max_, exp_base=exp_base, jitter=jitter
        )
        assert isinstance(wait, wait_exponential_jitter)


class TestYieldAttempts:
    async def test_main(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_attempts(stop=stop_after_attempt(10)):
                i += 1
                with attempt:
                    raise RuntimeError
        assert isinstance(exc_info.value.last_attempt.exception(), RuntimeError)
        assert i == 10

    async def test_disabled(self) -> None:
        i = 0
        with raises(RuntimeError):  # noqa: PT012
            async for attempt in yield_attempts():
                i += 1
                with attempt:
                    raise RuntimeError
        assert i == 1

    async def test_timeout_success(self) -> None:
        i = 10
        async for attempt in yield_attempts(stop=stop_after_attempt(10)):
            i -= 1
            with attempt:
                async with timeout_delta(0.05 * SECOND):
                    await sleep_delta(i * 0.01 * SECOND)
        assert 2 <= i <= 4

    async def test_timeout_fail(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_attempts(stop=stop_after_attempt(10)):
                i += 1
                with attempt:
                    async with timeout_delta(0.01 * SECOND):
                        await sleep_delta(0.02 * SECOND)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10


class TestYieldTimeoutAttempts:
    async def test_main(self) -> None:
        i = 10
        async for attempt in yield_timeout_attempts(
            stop=stop_after_attempt(10), delta=0.05 * SECOND
        ):
            i -= 1
            async with attempt:
                await sleep_delta(i * 0.01 * SECOND)
        assert 2 <= i <= 4

    async def test_success_with_follow(self) -> None:
        i = 10
        try:
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), delta=0.05 * SECOND
            ):
                i -= 1
                async with attempt:
                    _ = await sleep_delta(i * 0.01 * SECOND)
        except TimeoutError as error:
            raise NotImplementedError from error
        assert 2 <= i <= 4

    async def test_error(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), delta=0.01 * SECOND
            ):
                i += 1
                async with attempt:
                    await sleep_delta(0.02 * SECOND)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10
