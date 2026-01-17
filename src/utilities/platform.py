from __future__ import annotations

from re import sub
from typing import assert_never

from utilities.constants import SYSTEM


def get_strftime(text: str, /) -> str:
    """Get a platform-specific format string."""
    match SYSTEM:
        case "windows":  # skipif-not-windows
            return text
        case "mac":  # skipif-not-macos
            return text
        case "linux":  # skipif-not-linux
            return sub("%Y", "%4Y", text)
        case never:
            assert_never(never)


##


def maybe_lower_case(text: str, /) -> str:
    """Lower-case text if the platform is case-insensitive w.r.t. filenames."""
    match SYSTEM:
        case "windows" | "mac":  # skipif-linux
            return text.lower()
        case "linux":  # skipif-not-linux
            return text
        case never:
            assert_never(never)


__all__ = ["get_strftime", "maybe_lower_case"]
