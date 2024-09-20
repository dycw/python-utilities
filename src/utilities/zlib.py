from __future__ import annotations

from typing import Any
from zlib import crc32


def crc32_hash(obj: Any, /) -> int:
    """Compute the CRC32 hash of an arbitrary object."""
    from utilities.orjson import serialize

    return crc32(serialize(obj))


__all__ = ["crc32_hash"]
