from __future__ import annotations

import sys
from re import search
from typing import TYPE_CHECKING, Any, cast

from hypothesis import given
from hypothesis.strategies import floats
from loguru import logger

from tests.test_loguru_functions import func_test_tenacity_before_sleep_log
from utilities.hypothesis import durations
from utilities.tenacity import wait_exponential_jitter

if TYPE_CHECKING:
    from pytest import CaptureFixture

    from utilities.loguru import HandlerConfiguration
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


class TestLoguruAdapter:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        handler: HandlerConfiguration = {"sink": sys.stdout}
        _ = logger.configure(handlers=[cast(dict[str, Any], handler)])

        assert func_test_tenacity_before_sleep_log() == 3
        out = capsys.readouterr().out
        lines = out.splitlines()
        assert len(lines) == 2
        for i, line in enumerate(lines, start=1):
            expected = (
                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \| INFO     \| utilities\.tenacity:log:\d+ - Retrying tests\.test_loguru_functions\.func_test_tenacity_before_sleep_log in 0\.01 seconds as it raised ValueError: "
                + str(i)
                + r"\."
            )
            assert search(expected, line), line
