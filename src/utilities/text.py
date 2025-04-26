from __future__ import annotations

import re
from dataclasses import dataclass
from re import IGNORECASE, Match, search
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Literal, overload, override

from utilities.iterables import CheckDuplicatesError, check_duplicates
from utilities.reprlib import get_repr

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from utilities.types import StrStrMapping


def parse_bool(text: str, /) -> bool:
    """Parse text into a boolean value."""
    if text == "0" or search("false", text, flags=IGNORECASE):
        return False
    if text == "1" or search("true", text, flags=IGNORECASE):
        return True
    raise ParseBoolError(text=text)


@dataclass(kw_only=True, slots=True)
class ParseBoolError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse boolean value; got {self.text!r}"


##


def parse_none(text: str, /) -> None:
    """Parse text into the None value."""
    if text == "" or search("None", text, flags=IGNORECASE):
        return
    raise ParseNoneError(text=text)


@dataclass(kw_only=True, slots=True)
class ParseNoneError(Exception):
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse null value; got {self.text!r}"


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


@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    mapping: Literal[True],
) -> StrStrMapping: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    mapping: Literal[False] = False,
) -> Sequence[tuple[str, str]]: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    mapping: bool = False,
) -> Sequence[tuple[str, str]] | StrStrMapping: ...
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = ",",
    pair_separator: str = "=",
    mapping: bool = False,
) -> Sequence[tuple[str, str]] | StrStrMapping:
    """Split a string into key-value pairs."""
    try:
        pairs = [
            split_str(text_i, separator=pair_separator, n=2)
            for text_i in split_str(text, separator=list_separator)
        ]
    except SplitStrError as error:
        raise _SplitKeyValuePairsSplitError(text=text, inner=error.text) from None
    if not mapping:
        return pairs
    try:
        check_duplicates(k for k, _ in pairs)
    except CheckDuplicatesError as error:
        raise _SplitKeyValuePairsDuplicateKeysError(
            text=text, counts=error.counts
        ) from None
    return dict(pairs)


@dataclass(kw_only=True, slots=True)
class SplitKeyValuePairsError(Exception):
    text: str


@dataclass(kw_only=True, slots=True)
class _SplitKeyValuePairsSplitError(SplitKeyValuePairsError):
    inner: str

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r} into key-value pairs; got {self.inner!r}"


@dataclass(kw_only=True, slots=True)
class _SplitKeyValuePairsDuplicateKeysError(SplitKeyValuePairsError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r} into a mapping since there are duplicate keys; got {get_repr(self.counts)}"


##


@overload
def split_str(text: str, /, *, separator: str = ",", n: Literal[1]) -> tuple[str]: ...
@overload
def split_str(
    text: str, /, *, separator: str = ",", n: Literal[2]
) -> tuple[str, str]: ...
@overload
def split_str(
    text: str, /, *, separator: str = ",", n: Literal[3]
) -> tuple[str, str, str]: ...
@overload
def split_str(
    text: str, /, *, separator: str = ",", n: Literal[4]
) -> tuple[str, str, str, str]: ...
@overload
def split_str(
    text: str, /, *, separator: str = ",", n: Literal[5]
) -> tuple[str, str, str, str, str]: ...
@overload
def split_str(
    text: str, /, *, separator: str = ",", n: int | None = None
) -> Sequence[str]: ...
def split_str(
    text: str, /, *, separator: str = ",", n: int | None = None
) -> Sequence[str]:
    """Split a string, with a special provision for the empty string."""
    if text == "":
        texts = []
    elif text == _escape_separator(separator=separator):
        texts = [""]
    else:
        texts = text.split(separator)
    if n is None:
        return texts
    if len(texts) != n:
        raise SplitStrError(text=text, n=n, texts=texts)
    return tuple(texts)


@dataclass(kw_only=True, slots=True)
class SplitStrError(Exception):
    text: str
    n: int
    texts: Sequence[str]

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r} into {self.n} part(s); got {len(self.texts)}"


def join_strs(
    texts: Iterable[str], /, *, sort: bool = False, separator: str = ","
) -> str:
    """Join a collection of strings, with a special provision for the empty list."""
    texts = list(texts)
    if sort:
        texts = sorted(texts)
    if texts == []:
        return ""
    if texts == [""]:
        return _escape_separator(separator=separator)
    return separator.join(texts)


def _escape_separator(*, separator: str = ",") -> str:
    return f"\\{separator}"


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
    "ParseBoolError",
    "ParseNoneError",
    "SplitKeyValuePairsError",
    "SplitStrError",
    "join_strs",
    "parse_bool",
    "parse_none",
    "repr_encode",
    "snake_case",
    "split_key_value_pairs",
    "split_str",
    "str_encode",
    "strip_and_dedent",
]
