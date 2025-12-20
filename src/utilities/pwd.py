from __future__ import annotations

from typing import assert_never

from utilities.os import EFFECTIVE_USER_ID
from utilities.platform import SYSTEM


def get_uid_name(uid: int, /) -> str | None:
    """Get the name of a user ID."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from pwd import getpwuid

            return getpwuid(uid).pw_name
        case never:
            assert_never(never)


ROOT_USER_NAME = get_uid_name(0)
EFFECTIVE_USER_NAME = (
    None if EFFECTIVE_USER_ID is None else get_uid_name(EFFECTIVE_USER_ID)
)


__all__ = ["EFFECTIVE_USER_NAME", "ROOT_USER_NAME", "get_uid_name"]
