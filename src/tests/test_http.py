from __future__ import annotations

from ipaddress import IPv4Address

from utilities.http import get_public_ip, yield_connection
from utilities.pytest import throttle
from utilities.whenever import MINUTE


class TestGetPublicIP:
    @throttle(delta=5 * MINUTE)
    def test_main(self) -> None:
        ip = get_public_ip(timeout=10.0)
        assert isinstance(ip, IPv4Address)


class TestYieldConnection:
    def test_main(self) -> None:
        with yield_connection("api.ipify.org"):
            pass
