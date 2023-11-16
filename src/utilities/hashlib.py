from __future__ import annotations

from hashlib import md5
from operator import itemgetter
from pickle import dumps
from typing import Any


def md5_hash(obj: Any, /) -> str:
    """Compute the MD5 hash of an arbitrary object."""
    if isinstance(obj, bytes):
        return md5(obj, usedforsecurity=False).hexdigest()
    if isinstance(obj, str):
        return md5_hash(obj.encode())
    if isinstance(obj, dict):
        return md5_hash(sorted(obj.items(), key=itemgetter(0)))
    if isinstance(obj, set | frozenset):
        return md5_hash(sorted(obj))
    return md5_hash(dumps(obj))


__all__ = [
    "md5_hash",
]
