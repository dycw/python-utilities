from __future__ import annotations

from grp import getgrgid

from utilities.os import EFFECTIVE_GROUP_ID


def get_gid_name(gid: int, /) -> str:
    """Get the name of a group."""
    return getgrgid(gid).gr_name


ROOT_GROUP_NAME = get_gid_name(0)
EFFECTIVE_GROUP_NAME = (
    None if EFFECTIVE_GROUP_ID is None else get_gid_name(EFFECTIVE_GROUP_ID)
)


__all__ = ["EFFECTIVE_GROUP_NAME", "ROOT_GROUP_NAME", "get_gid_name"]
