from __future__ import annotations

from typing import Any


def try_pretty_repr(obj: Any, /) -> str:
    """Try pretty-format an object."""
    try:
        from rich.pretty import pretty_repr
    except ImportError:
        return repr(obj)
    return pretty_repr(obj)


__all__ = ["try_pretty_repr"]
