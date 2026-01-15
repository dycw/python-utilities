from __future__ import annotations

import time
from typing import TYPE_CHECKING

from utilities.functions import in_seconds

if TYPE_CHECKING:
    from utilities.types import Duration


def sleep(duration: Duration | None = None, /) -> None:
    """Sleep which accepts deltas."""
    if duration is not None:
        time.sleep(in_seconds(duration))


__all__ = ["sleep"]
