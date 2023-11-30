from __future__ import annotations

from textwrap import dedent
from typing import Any


def ensure_str(obj: Any, /) -> str:
    """Ensure an object is a string."""
    if isinstance(obj, str):
        return obj
    msg = f"{obj=}"
    raise EnsureStrError(msg)


class EnsureStrError(Exception):
    ...


def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")


__all__ = ["ensure_str", "EnsureStrError", "strip_and_dedent"]
