from __future__ import annotations

from typing import assert_never

from utilities.os import EFFECTIVE_GROUP_ID
from utilities.platform import SYSTEM


def get_gid_name(gid: int, /) -> str | None:
    """Get the name of a group."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return None
        case "mac" | "linux":  # skipif-windows
            from grp import getgrgid

            return getgrgid(gid).gr_name
        case never:
            assert_never(never)


ROOT_GROUP_NAME = get_gid_name(0)
EFFECTIVE_GROUP_NAME = (
    None if EFFECTIVE_GROUP_ID is None else get_gid_name(EFFECTIVE_GROUP_ID)
)


__all__ = ["EFFECTIVE_GROUP_NAME", "ROOT_GROUP_NAME", "get_gid_name"]
