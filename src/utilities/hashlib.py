from __future__ import annotations

from hashlib import md5
from pickle import dumps
from typing import Any

from utilities.json import JsonSerializationError, serialize


def md5_hash(obj: Any, /) -> str:
    """Compute the MD5 hash of an arbitrary object."""
    try:
        ser = serialize(obj).encode()
    except JsonSerializationError:
        ser = dumps(obj)
    return md5(ser, usedforsecurity=False).hexdigest()


__all__ = ["md5_hash"]
