from __future__ import annotations

from ipaddress import IPv4Address

from dns.resolver import resolve

from utilities.functools import cache


@cache
def nslookup(address: str, /) -> list[IPv4Address]:
    """Look up a set of addresses."""
    ans = resolve(address)  # skipif-ci-and-mac
    return [IPv4Address(str(rd)) for rd in ans]  # skipif-ci-and-mac


__all__ = ["nslookup"]
