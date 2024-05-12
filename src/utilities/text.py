from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Literal, overload

from typing_extensions import override

from utilities.sentinel import sentinel
from utilities.types import EnsureClassError, ensure_class, get_class_name

if TYPE_CHECKING:
    from collections.abc import Iterable


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


def join_strs(
    texts: Iterable[str], /, *, separator: str = ",", empty: str = str(sentinel)
) -> str:
    """Join a collection of strings, with a special provision for the empty list."""
    texts = list(texts)
    if len(texts) >= 1:
        return separator.join(texts)
    return empty


def split_str(
    text: str, /, *, separator: str = ",", empty: str = str(sentinel)
) -> list[str]:
    """Split a string, with a special provision for the empty string."""
    return [] if text == empty else text.split(separator)


def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")


__all__ = ["EnsureStrError", "ensure_str", "join_strs", "split_str", "strip_and_dedent"]
