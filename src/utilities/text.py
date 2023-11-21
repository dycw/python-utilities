from __future__ import annotations

from re import search, sub
from textwrap import dedent
from typing import Any

from utilities.itertools import IterableContainsDuplicatesError, check_duplicates
from utilities.typing import IterableStrs


def ensure_str(obj: Any, /) -> str:
    """Ensure an object is a string."""
    if isinstance(obj, str):
        return obj
    msg = f"{obj=}"
    raise NotAStringError(msg)


class NotAStringError(Exception):
    """Raised when an object is not a string."""


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    text = text.replace(" ", "")
    text = "".join(c for c in text if str.isidentifier(c) or str.isdigit(c))
    while search("__", text):
        text = text.replace("__", "_")
    text = sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", text)
    text = sub(r"([a-z\d])([A-Z])", r"\1_\2", text)
    text = text.replace("-", "_")
    return text.lower()


def snake_case_mappings(
    text: IterableStrs, /, *, inverse: bool = False
) -> dict[str, str]:
    """Map a set of text into their snake cases."""
    as_list = list(text)
    check_duplicates(as_list)
    snaked = list(map(snake_case, as_list))
    try:
        check_duplicates(snaked)
    except IterableContainsDuplicatesError:
        msg = f"{text=}"
        raise SnakeCaseContainsDuplicatesError(msg) from None
    if inverse:
        return {v: k for k, v in snake_case_mappings(as_list).items()}
    return dict(zip(as_list, snaked, strict=True))


class SnakeCaseContainsDuplicatesError(Exception):
    """Raised when the snake case values contain duplicates."""


def strip_and_dedent(text: str, /) -> str:
    """Strip and dedent a string."""
    return dedent(text.strip("\n")).strip("\n")


__all__ = [
    "ensure_str",
    "NotAStringError",
    "snake_case_mappings",
    "snake_case",
    "SnakeCaseContainsDuplicatesError",
    "strip_and_dedent",
]
