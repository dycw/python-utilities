from __future__ import annotations

from re import search

from bidict import ValueDuplicationError, bidict
from humps import decamelize

from utilities.errors import redirect_error
from utilities.iterables import CheckDuplicatesError, check_duplicates
from utilities.types import IterableStrs


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""

    text = decamelize(text)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


def snake_case_mappings(text: IterableStrs, /) -> bidict[str, str]:
    """Map a set of text into their snake cases."""

    text_as_list = list(text)
    with redirect_error(
        CheckDuplicatesError, SnakeCaseMappingsError(f"{text_as_list=}")
    ):
        check_duplicates(text_as_list)

    with redirect_error(
        ValueDuplicationError, SnakeCaseMappingsError(f"{text_as_list=}")
    ):
        return bidict({t: snake_case(t) for t in text_as_list})


class SnakeCaseMappingsError(Exception):
    ...


__all__ = ["SnakeCaseMappingsError", "snake_case", "snake_case_mappings"]
