from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import floats
from pytest import raises
from tenacity import RetryError, stop_after_attempt

from utilities.hypothesis import durations
from utilities.tenacity import wait_exponential_jitter, yield_attempts

if TYPE_CHECKING:
    from utilities.types import Duration


class TestWaitExponentialJitter:
    @given(initial=durations(), max_=durations(), exp_base=floats(), jitter=durations())
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
        with raises(RetryError):  # noqa: PT012
            async for attempt in yield_attempts(stop=stop_after_attempt(3)):
                with attempt:
                    i += 1
                    raise RuntimeError
        assert i == 3

    async def test_disabled(self) -> None:
        i = 0
        with raises(RuntimeError):  # noqa: PT012
            async for attempt in yield_attempts():
                with attempt:
                    i += 1
                    raise RuntimeError
        assert i == 1
