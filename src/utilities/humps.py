from __future__ import annotations

from re import search

from humps import decamelize

from utilities.iterables import CheckDuplicatesError, check_duplicates
from utilities.types import IterableStrs


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""

    text = decamelize(text)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


class SnakeCaseMappingsError(Exception):
    ...


def snake_case_mappings(
    text: IterableStrs, /, *, inverse: bool = False
) -> dict[str, str]:
    """Map a set of text into their snake cases."""
    as_list = list(text)
    check_duplicates(as_list)
    snaked = list(map(snake_case, as_list))
    try:
        check_duplicates(snaked)
    except CheckDuplicatesError:
        msg = f"{text=}"
        raise SnakeCaseMappingsError(msg) from None
    if inverse:
        return {v: k for k, v in snake_case_mappings(as_list).items()}
    return dict(zip(as_list, snaked, strict=True))


__all__ = ["SnakeCaseMappingsError", "snake_case", "snake_case_mappings"]
