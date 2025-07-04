from __future__ import annotations

from random import Random


class TestRandomState:
    def test_main(self, *, random_state: Random) -> None:
        assert isinstance(random_state, Random)
