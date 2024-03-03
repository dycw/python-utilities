from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING

from humps import decamelize
from typing_extensions import override

from utilities.iterables import CheckDuplicatesError, check_duplicates

if TYPE_CHECKING:
    from collections.abc import Hashable

    from bidict import bidict

    from utilities.types import IterableStrs


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""

    text = decamelize(text)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


def snake_case_mappings(text: IterableStrs, /) -> bidict[str, str]:
    """Map a set of text into their snake cases."""

    from bidict import bidict

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


@dataclass(kw_only=True)
class SnakeCaseMappingsError(Exception):
    text: list[str]
    counts: dict[Hashable, int]


@dataclass(kw_only=True)
class _SnakeCaseMappingsDuplicateKeysError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return f"Strings {self.text} must not contain duplicates; got {self.counts}"


@dataclass(kw_only=True)
class _SnakeCaseMappingsDuplicateValuesError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return f"Snake-cased strings {self.text} must not contain duplicates; got {self.counts}"


__all__ = ["SnakeCaseMappingsError", "snake_case", "snake_case_mappings"]
