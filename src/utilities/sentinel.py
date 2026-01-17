from __future__ import annotations

from typing import Any

from typing_extensions import TypeIs

from utilities.constants import Sentinel, sentinel


def is_sentinel(obj: Any, /) -> TypeIs[Sentinel]:
    """Check if an object is the sentinel."""
    return obj is sentinel


__all__ = ["is_sentinel"]
