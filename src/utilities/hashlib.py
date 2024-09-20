from __future__ import annotations

from hashlib import md5
from typing import Any


def md5_hash(obj: Any, /) -> str:
    """Compute the MD5 hash of an arbitrary object."""
    from utilities.orjson import serialize

    return md5(serialize(obj), usedforsecurity=False).hexdigest()


__all__ = ["md5_hash"]
