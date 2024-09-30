from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING

from humps import decamelize
from typing_extensions import override

from utilities.iterables import (
    CheckBijectionError,
    CheckDuplicatesError,
    check_bijection,
    check_duplicates,
)

if TYPE_CHECKING:
    from collections.abc import Hashable, Iterable, Mapping


def snake_case(text: str, /) -> str:
    """Convert text into snake case."""
    text = decamelize(text)
    while search("__", text):
        text = text.replace("__", "_")
    return text.lower()


def snake_case_mappings(text: Iterable[str], /) -> dict[str, str]:
    """Map a set of text into their snake cases."""
    keys = list(text)
    try:
        check_duplicates(keys)
    except CheckDuplicatesError as error:
        raise _SnakeCaseMappingsDuplicateKeysError(
            text=keys, counts=error.counts
        ) from None
    values = list(map(snake_case, keys))
    mapping = dict(zip(keys, values, strict=True))
    try:
        check_bijection(mapping)
    except CheckBijectionError as error:
        raise _SnakeCaseMappingsDuplicateValuesError(
            text=values, counts=error.counts
        ) from None
    return mapping


@dataclass(kw_only=True, slots=True)
class SnakeCaseMappingsError(Exception):
    text: list[str]
    counts: Mapping[Hashable, int]


@dataclass(kw_only=True, slots=True)
class _SnakeCaseMappingsDuplicateKeysError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return f"Strings {self.text} must not contain duplicates; got {self.counts}"


@dataclass(kw_only=True, slots=True)
class _SnakeCaseMappingsDuplicateValuesError(SnakeCaseMappingsError):
    @override
    def __str__(self) -> str:
        return f"Snake-cased strings {self.text} must not contain duplicates; got {self.counts}"


__all__ = ["SnakeCaseMappingsError", "snake_case", "snake_case_mappings"]
