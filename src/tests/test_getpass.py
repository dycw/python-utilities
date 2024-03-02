from __future__ import annotations

from typing_extensions import Self

from utilities.getpass import USER


class TestUser:
    def test_main(self: Self) -> None:
        assert isinstance(USER, str)
