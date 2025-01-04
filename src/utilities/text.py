from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any

from utilities.sentinel import SENTINEL_REPR

if TYPE_CHECKING:
    from collections.abc import Iterable


def join_strs(
    texts: Iterable[str],
    /,
    *,
    sort: bool = False,
    separator: str = ",",
    empty: str = SENTINEL_REPR,
) -> str:
    """Join a collection of strings, with a special provision for the empty list."""
    texts = sorted(texts) if sort else list(texts)
    if len(texts) >= 1:
        return separator.join(texts)
    return empty


def repr_encode(obj: Any, /) -> bytes:
    """Return the representation of the object encoded as bytes."""
    return repr(obj).encode()


def split_str(
    text: str, /, *, separator: str = ",", empty: str = SENTINEL_REPR
) -> list[str]:
    """Split a string, with a special provision for the empty string."""
    return [] if text == empty else text.split(separator)


def str_encode(obj: Any, /) -> bytes:
    """Return the string representation of the object encoded as bytes."""
    return str(obj).encode()


def strip_and_dedent(text: str, /, *, trailing: bool = False) -> str:
    """Strip and dedent a string."""
    result = dedent(text.strip("\n")).strip("\n")
    return f"{result}\n" if trailing else result


__all__ = ["join_strs", "repr_encode", "split_str", "str_encode", "strip_and_dedent"]
