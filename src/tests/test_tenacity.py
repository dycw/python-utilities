from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from hypothesis import given
from hypothesis.strategies import floats
from pytest import raises
from tenacity import RetryError, stop_after_attempt

from utilities.asyncio import sleep_td, timeout_td
from utilities.hypothesis import datetime_durations
from utilities.tenacity import (
    wait_exponential_jitter,
    yield_attempts,
    yield_timeout_attempts,
)
from utilities.whenever2 import SECOND

if TYPE_CHECKING:
    from whenever import TimeDelta

    from utilities.types import Duration


class TestWaitExponentialJitter:
    @given(
        initial=datetime_durations(),
        max_=datetime_durations(),
        exp_base=floats(),
        jitter=datetime_durations(),
    )
    def test_main(
        self, *, initial: Duration, max_: Duration, exp_base: float, jitter: Duration
    ) -> None:
        wait = wait_exponential_jitter(
            initial=initial, max=max_, exp_base=exp_base, jitter=jitter
        )
        assert isinstance(wait, wait_exponential_jitter)


class TestYieldAttempts:
    delay: ClassVar[TimeDelta] = 0.01 * SECOND

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
                async with timeout_td(5 * self.delay):
                    await sleep_td(i * self.delay)
        assert 2 <= i <= 4

    async def test_timeout_fail(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_attempts(stop=stop_after_attempt(10)):
                i += 1
                with attempt:
                    async with timeout_td(self.delay):
                        await sleep_td(2 * self.delay)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10


class TestYieldTimeoutAttempts:
    delay: ClassVar[TimeDelta] = 0.01 * SECOND

    async def test_main(self) -> None:
        i = 10
        async for attempt in yield_timeout_attempts(
            stop=stop_after_attempt(10), timeout_delay=5 * self.delay
        ):
            i -= 1
            async with attempt:
                await sleep_td(i * self.delay)
        assert 2 <= i <= 4

    async def test_success_with_follow(self) -> None:
        i = 10
        try:
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), timeout_delay=5 * self.delay
            ):
                i -= 1
                async with attempt:
                    _ = await sleep_td(i * self.delay)
        except TimeoutError as error:
            raise NotImplementedError from error
        assert 2 <= i <= 4

    async def test_error(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), timeout_delay=self.delay
            ):
                i += 1
                async with attempt:
                    await sleep_td(2 * self.delay)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10
