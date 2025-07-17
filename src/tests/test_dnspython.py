from __future__ import annotations

from ipaddress import IPv4Address

from tests.conftest import SKIPIF_CI_AND_MAC
from utilities.dnspython import nslookup


class TestNSLookup:
    @SKIPIF_CI_AND_MAC
    def test_main(self) -> None:
        result = nslookup("localhost")
        expected = [IPv4Address("127.0.0.1")]
        assert result == expected
