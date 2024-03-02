from __future__ import annotations

from typing_extensions import Self

from utilities.socket import HOSTNAME


class TestHostname:
    def test_main(self: Self) -> None:
        assert isinstance(HOSTNAME, str)
