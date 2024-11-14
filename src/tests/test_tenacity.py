from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import floats

from utilities.hypothesis import durations
from utilities.tenacity import wait_exponential_jitter

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
