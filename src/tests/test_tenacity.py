from __future__ import annotations

from asyncio import sleep, timeout
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import floats
from pytest import raises
from tenacity import RetryError, stop_after_attempt

from utilities.hypothesis import datetime_durations
from utilities.tenacity import (
    wait_exponential_jitter,
    yield_attempts,
    yield_timeout_attempts,
)

if TYPE_CHECKING:
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
                async with timeout(0.05):
                    await sleep(i * 0.01)
        assert 2 <= i <= 4

    async def test_timeout_fail(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_attempts(stop=stop_after_attempt(10)):
                i += 1
                with attempt:
                    async with timeout(0.01):
                        await sleep(0.02)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10


class TestYieldTimeoutAttempts:
    async def test_main(self) -> None:
        i = 10
        async for attempt in yield_timeout_attempts(
            stop=stop_after_attempt(10), timeout=0.05
        ):
            i -= 1
            async with attempt:
                await sleep(i * 0.01)
        assert 2 <= i <= 4

    async def test_success_with_follow(self) -> None:
        i = 10
        try:
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), timeout=0.05
            ):
                i -= 1
                async with attempt:
                    _ = await sleep(i * 0.01)
        except TimeoutError as error:
            raise NotImplementedError from error
        assert 2 <= i <= 4

    async def test_error(self) -> None:
        i = 0
        with raises(RetryError) as exc_info:  # noqa: PT012
            async for attempt in yield_timeout_attempts(
                stop=stop_after_attempt(10), timeout=0.01
            ):
                i += 1
                async with attempt:
                    await sleep(0.02)
        assert isinstance(exc_info.value.last_attempt.exception(), TimeoutError)
        assert i == 10
