from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass
from itertools import chain
from os import getpid
from re import IGNORECASE, Match, escape, search
from textwrap import dedent
from threading import get_ident
from time import time_ns
from typing import TYPE_CHECKING, Any, Literal, overload, override
from uuid import uuid4

from utilities.iterables import CheckDuplicatesError, check_duplicates, transpose
from utilities.reprlib import get_repr

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from utilities.types import StrStrMapping


DEFAULT_SEPARATOR = ","


##


def parse_bool(text: str, /) -> bool:
    """Parse text into a boolean value."""
    if search(r"^(0|False|N|No|Off)$", text, flags=IGNORECASE):
        return False
    if search(r"^(1|True|Y|Yes|On)$", text, flags=IGNORECASE):
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
    if search(r"^(|None)$", text, flags=IGNORECASE):
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


LIST_SEPARATOR = DEFAULT_SEPARATOR
PAIR_SEPARATOR = "="
BRACKETS = [("(", ")"), ("[", "]"), ("{", "}")]


@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = DEFAULT_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: Literal[True],
) -> StrStrMapping: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = DEFAULT_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: Literal[False] = False,
) -> Sequence[tuple[str, str]]: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = DEFAULT_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: bool = False,
) -> Sequence[tuple[str, str]] | StrStrMapping: ...
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = DEFAULT_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: bool = False,
) -> Sequence[tuple[str, str]] | StrStrMapping:
    """Split a string into key-value pairs."""
    try:
        texts = split_str(text, separator=list_separator, brackets=brackets)
    except SplitStrError as error:
        raise _SplitKeyValuePairsSplitError(text=text, inner=error.text) from None
    try:
        pairs = [
            split_str(text_i, separator=pair_separator, brackets=brackets, n=2)
            for text_i in texts
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
        return f"Unable to split {self.text!r} into key-value pairs"


@dataclass(kw_only=True, slots=True)
class _SplitKeyValuePairsDuplicateKeysError(SplitKeyValuePairsError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r} into a mapping since there are duplicate keys; got {get_repr(self.counts)}"


##


@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[1],
) -> tuple[str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[2],
) -> tuple[str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[3],
) -> tuple[str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[4],
) -> tuple[str, str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[5],
) -> tuple[str, str, str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: int | None = None,
) -> Sequence[str]: ...
def split_str(
    text: str,
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: int | None = None,
) -> Sequence[str]:
    """Split a string, with a special provision for the empty string."""
    if text == "":
        texts = []
    elif text == _escape_separator(separator=separator):
        texts = [""]
    elif brackets is None:
        texts = text.split(separator)
    else:
        texts = _split_str_brackets(text, brackets, separator=separator)
    if n is None:
        return texts
    if len(texts) != n:
        raise _SplitStrCountError(text=text, n=n, texts=texts)
    return tuple(texts)


def _split_str_brackets(
    text: str,
    brackets: Iterable[tuple[str, str]],
    /,
    *,
    separator: str = DEFAULT_SEPARATOR,
) -> Sequence[str]:
    brackets = list(brackets)
    opens, closes = transpose(brackets)
    close_to_open = {close: open_ for open_, close in brackets}

    escapes = map(escape, chain(chain.from_iterable(brackets), [separator]))
    pattern = re.compile("|".join(escapes))

    results: Sequence[str] = []
    stack: deque[tuple[str, int]] = deque()
    last = 0

    for match in pattern.finditer(text):
        token, position = match.group(), match.start()
        if token in opens:
            stack.append((token, position))
        elif token in closes:
            if len(stack) == 0:
                raise _SplitStrClosingBracketUnmatchedError(
                    text=text, token=token, position=position
                )
            open_token, open_position = stack.pop()
            if open_token != close_to_open[token]:
                raise _SplitStrClosingBracketMismatchedError(
                    text=text,
                    opening_token=open_token,
                    opening_position=open_position,
                    closing_token=token,
                    closing_position=position,
                )
        elif (token == separator) and (len(stack) == 0):
            results.append(text[last:position].strip())
            last = position + 1
    results.append(text[last:].strip())
    if len(stack) >= 1:
        token, position = stack.pop()
        raise _SplitStrOpeningBracketUnmatchedError(
            text=text, token=token, position=position
        )
    return results


@dataclass(kw_only=True, slots=True)
class SplitStrError(Exception):
    text: str


@dataclass(kw_only=True, slots=True)
class _SplitStrCountError(SplitStrError):
    n: int
    texts: Sequence[str]

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r} into {self.n} part(s); got {len(self.texts)}"


@dataclass(kw_only=True, slots=True)
class _SplitStrClosingBracketMismatchedError(SplitStrError):
    opening_token: str
    opening_position: int
    closing_token: str
    closing_position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r}; got mismatched {self.opening_token!r} at position {self.opening_position} and {self.closing_token!r} at position {self.closing_position}"


@dataclass(kw_only=True, slots=True)
class _SplitStrClosingBracketUnmatchedError(SplitStrError):
    token: str
    position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r}; got unmatched {self.token!r} at position {self.position}"


@dataclass(kw_only=True, slots=True)
class _SplitStrOpeningBracketUnmatchedError(SplitStrError):
    token: str
    position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {self.text!r}; got unmatched {self.token!r} at position {self.position}"


def join_strs(
    texts: Iterable[str], /, *, sort: bool = False, separator: str = DEFAULT_SEPARATOR
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


def _escape_separator(*, separator: str = DEFAULT_SEPARATOR) -> str:
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


##


def unique_str() -> str:
    """Generate at unique string."""
    now = time_ns()
    pid = getpid()
    ident = get_ident()
    key = str(uuid4()).replace("-", "")
    return f"{now}_{pid}_{ident}_{key}"


__all__ = [
    "BRACKETS",
    "DEFAULT_SEPARATOR",
    "LIST_SEPARATOR",
    "PAIR_SEPARATOR",
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
    "unique_str",
]
