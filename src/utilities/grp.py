from __future__ import annotations

from grp import getgrgid

from utilities.os import EFFECTIVE_GROUP_ID

EFFECTIVE_GROUP_NAME = getgrgid(EFFECTIVE_GROUP_ID).gr_name


__all__ = ["EFFECTIVE_GROUP_NAME"]
