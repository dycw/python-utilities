from __future__ import annotations

from ipaddress import IPv4Address

from utilities.core import get_local_ip


class TestGetLocalIP:
    def test_main(self) -> None:
        assert isinstance(get_local_ip(), IPv4Address)
