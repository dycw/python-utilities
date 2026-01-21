from __future__ import annotations

from contextlib import contextmanager
from http.client import HTTPSConnection
from ipaddress import IPv4Address
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def get_public_ip(*, timeout: float | None = None) -> IPv4Address:
    """Get your public IP address."""
    with yield_connection("api.ipify.org", timeout=timeout) as conn:  # pragma: no cover
        conn.request("GET", "/?format=text")
        response = conn.getresponse()
        address = response.read().decode("utf8")
    return IPv4Address(address)  # pragma: no cover


##


@contextmanager
def yield_connection(
    host: str, /, *, timeout: float | None = None
) -> Iterator[HTTPSConnection]:
    """Yield an HTTP connection."""
    conn = HTTPSConnection(host, timeout=timeout)
    try:
        yield conn
    finally:
        conn.close()


__all__ = ["get_public_ip", "yield_connection"]
