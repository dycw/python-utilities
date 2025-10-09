from __future__ import annotations

from pwd import getpwuid

from utilities.os import EFFECTIVE_USER_ID


def get_uid_name(uid: int, /) -> str:
    """Get the name of a user ID."""
    return getpwuid(uid).pw_name


ROOT_USER_NAME = get_uid_name(0)
EFFECTIVE_USER_NAME = get_uid_name(EFFECTIVE_USER_ID)


__all__ = ["EFFECTIVE_USER_NAME", "ROOT_USER_NAME", "get_uid_name"]
