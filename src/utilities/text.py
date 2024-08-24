from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Literal, overload

from typing_extensions import override

from utilities.functions import get_class_name
from utilities.sentinel import SENTINEL_REPR
from utilities.types import EnsureClassError, ensure_class

if TYPE_CHECKING:
    from collections.abc import Iterable


@overload
def ensure_bytes(obj: Any, /, *, nullable: bool) -> bytes | None: ...
@overload
def ensure_bytes(obj: Any, /, *, nullable: Literal[False] = False) -> bytes: ...
def ensure_bytes(obj: Any, /, *, nullable: bool = False) -> bytes | None:
    """Ensure an object is a bytesean."""
    try:
        return ensure_class(obj, bytes, nullable=nullable)
    except EnsureClassError as error:
        raise EnsureBytesError(obj=error.obj, nullable=nullable) from None


@dataclass(kw_only=True)
class EnsureBytesError(Exception):
    obj: Any
    nullable: bool

    @override
    def __str__(self) -> str:
        desc = " or None" if self.nullable else ""
        return f"Object {self.obj} must be a byte string{desc}; got {get_class_name(self.obj)} instead"


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
    texts: Iterable[str], /, *, separator: str = ",", empty: str = SENTINEL_REPR
) -> str:
    """Join a collection of strings, with a special provision for the empty list."""
    texts = list(texts)
    if len(texts) >= 1:
        return separator.join(texts)
    return empty


def split_str(
    text: str, /, *, separator: str = ",", empty: str = SENTINEL_REPR
) -> list[str]:
    """Split a string, with a special provision for the empty string."""
    return [] if text == empty else text.split(separator)


def strip_and_dedent(text: str, /, *, trailing: bool = False) -> str:
    """Strip and dedent a string."""
    result = dedent(text.strip("\n")).strip("\n")
    return f"{result}\n" if trailing else result


__all__ = [
    "EnsureBytesError",
    "EnsureStrError",
    "ensure_bytes",
    "ensure_str",
    "join_strs",
    "split_str",
    "strip_and_dedent",
]
