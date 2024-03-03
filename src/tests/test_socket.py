from __future__ import annotations

from utilities.socket import HOSTNAME


class TestHostname:
    def test_main(self) -> None:
        assert isinstance(HOSTNAME, str)
