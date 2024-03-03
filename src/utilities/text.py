from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import Any

from typing_extensions import override

from utilities.types import EnsureClassError, ensure_class, get_class_name


def ensure_str(obj: Any, /) -> str:
    """Ensure an object is a string."""
    try:
        return ensure_class(obj, str)
    except EnsureClassError as error:
        raise EnsureStrError(obj=error.obj) from None


@dataclass(kw_only=True)
class EnsureStrError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return f"Object {self.obj} must be a string; got {get_class_name(self.obj)} instead"


def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")


__all__ = ["EnsureStrError", "ensure_str", "strip_and_dedent"]
