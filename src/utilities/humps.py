from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass
from re import search

from bidict import bidict
from humps import decamelize
from typing_extensions import override

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

    keys = list(text)
    try:
        check_duplicates(keys)
    except CheckDuplicatesError as error:
        raise _SnakeCaseMappingsDuplicateKeysError(
            text=keys, counts=error.counts
        ) from None
    values = list(map(snake_case, keys))
    try:
        check_duplicates(values)
    except CheckDuplicatesError as error:
        raise _SnakeCaseMappingsDuplicateValuesError(
            text=values, counts=error.counts
        ) from None
    return bidict(zip(keys, values, strict=True))


@dataclass(frozen=True, kw_only=True, slots=True)
class SnakeCaseMappingsError(Exception):
    text: list[str]
    counts: dict[Hashable, int]


@dataclass(frozen=True, kw_only=True, slots=True)
class _SnakeCaseMappingsDuplicateKeysError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return "Strings {} must not contain duplicates; got {}".format(
            self.text, self.counts
        )


@dataclass(frozen=True, kw_only=True, slots=True)
class _SnakeCaseMappingsDuplicateValuesError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return "Snake-cased strings {} must not contain duplicates; got {}".format(
            self.text, self.counts
        )


__all__ = ["SnakeCaseMappingsError", "snake_case", "snake_case_mappings"]
