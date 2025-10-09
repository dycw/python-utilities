from __future__ import annotations

from utilities.pwd import EFFECTIVE_USER_NAME


class TestEffectiveUserName:
    def test_main(self) -> None:
        assert isinstance(EFFECTIVE_USER_NAME, str)
