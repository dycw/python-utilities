from __future__ import annotations

import re
from re import Match, search
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


##


def repr_encode(obj: Any, /) -> bytes:
    """Return the representation of the object encoded as bytes."""
    return repr(obj).encode()


##


_ACRONYM_PATTERN = re.compile(r"([A-Z\d]+)(?=[A-Z\d]|$)")
_SPACES_PATTERN = re.compile(r"\s+")
_SPLIT_PATTERN = re.compile(r"([\-_]*[A-Z][^A-Z]*[\-_]*)")


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    text = _SPACES_PATTERN.sub("", text)
    if not text.isupper():
        text = _ACRONYM_PATTERN.sub(_snake_case_title, text)
        text = "_".join(s for s in _SPLIT_PATTERN.split(text) if s)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


def _snake_case_title(match: Match[str], /) -> str:
    return match.group(0).title()


##


def split_str(
    text: str, /, *, separator: str = ",", empty: str = SENTINEL_REPR
) -> list[str]:
    """Split a string, with a special provision for the empty string."""
    return [] if text == empty else text.split(separator)


##


def str_encode(obj: Any, /) -> bytes:
    """Return the string representation of the object encoded as bytes."""
    return str(obj).encode()


##


def strip_and_dedent(text: str, /, *, trailing: bool = False) -> str:
    """Strip and dedent a string."""
    result = dedent(text.strip("\n")).strip("\n")
    return f"{result}\n" if trailing else result


__all__ = [
    "join_strs",
    "repr_encode",
    "snake_case",
    "split_str",
    "str_encode",
    "strip_and_dedent",
]
