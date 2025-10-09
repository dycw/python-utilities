from __future__ import annotations

from pwd import getpwuid

from utilities.os import EFFECTIVE_USER_ID

EFFECTIVE_USER_NAME = getpwuid(EFFECTIVE_USER_ID).pw_name


__all__ = ["EFFECTIVE_USER_NAME"]
