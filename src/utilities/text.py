from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Literal, overload

from typing_extensions import override

from utilities.types import EnsureClassError, ensure_class, get_class_name


@overload
def ensure_str(obj: Any, /, *, nullable: bool) -> str | None: ...
@overload
def ensure_str(obj: Any, /, *, nullable: Literal[False] = False) -> str: ...
def ensure_str(obj: Any, /, *, nullable: bool = False) -> str | None:
    """Ensure an object is a string."""
    try:
        return ensure_class(obj, str, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureStrError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureStrError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a string{desc}; got {get_class_name(self.obj)} instead"


def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")


__all__ = ["EnsureStrError", "ensure_str", "strip_and_dedent"]
