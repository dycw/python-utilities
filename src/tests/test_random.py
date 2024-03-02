from __future__ import annotations

from random import SystemRandom

from typing_extensions import Self

from utilities.random import SYSTEM_RANDOM


class TestSystemRandom:
    def test_main(self: Self) -> None:
        assert isinstance(SYSTEM_RANDOM, SystemRandom)
