from hashlib import md5

from beartype import beartype


@beartype
def md5_hash(x: str, /) -> str:
    """Compute the MD5 hash of a string."""

    return md5(x.encode(), usedforsecurity=False).hexdigest()
