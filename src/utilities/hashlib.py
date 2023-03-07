from hashlib import md5
from pickle import dumps
from typing import Any

from beartype import beartype


@beartype
def md5_hash(obj: Any, /) -> str:
    """Compute the MD5 hash of an arbitrary object."""
    if isinstance(obj, bytes):
        return md5(obj, usedforsecurity=False).hexdigest()
    if isinstance(obj, str):
        return md5_hash(obj.encode())
    return md5_hash(dumps(obj))
