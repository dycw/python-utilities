from __future__ import annotations

import re
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from re import IGNORECASE, escape, search
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    assert_never,
    overload,
    override,
)

from rich.pretty import pretty_repr

from utilities.constants import BRACKETS, LIST_SEPARATOR, PAIR_SEPARATOR, Sentinel
from utilities.core import CheckUniqueError, check_unique, transpose

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from utilities.types import MaybeCallableBoolLike, MaybeCallableStr, StrStrMapping


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
        return f"Unable to parse boolean value; got {pretty_repr(self.text)}"


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
        return f"Unable to parse null value; got {pretty_repr(self.text)}"


##


def prompt_bool(prompt: object = "", /, *, confirm: bool = False) -> bool:
    """Prompt for a boolean."""
    return True if confirm else parse_bool(input(prompt))


##


def split_f_str_equals(text: str, /) -> tuple[str, str]:
    """Split an `f`-string with `=`."""
    first, second = text.split(sep="=", maxsplit=1)
    return first, second


##


@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: Literal[True],
) -> StrStrMapping: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: Literal[False] = False,
) -> Sequence[tuple[str, str]]: ...
@overload
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
    pair_separator: str = PAIR_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = BRACKETS,
    mapping: bool = False,
) -> Sequence[tuple[str, str]] | StrStrMapping: ...
def split_key_value_pairs(
    text: str,
    /,
    *,
    list_separator: str = LIST_SEPARATOR,
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
        check_unique(*(k for k, _ in pairs))
    except CheckUniqueError as error:
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
        return f"Unable to split {pretty_repr(self.text)} into key-value pairs"


@dataclass(kw_only=True, slots=True)
class _SplitKeyValuePairsDuplicateKeysError(SplitKeyValuePairsError):
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Unable to split {pretty_repr(self.text)} into a mapping since there are duplicate keys; got {pretty_repr(self.counts)}"


##


@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[1],
) -> tuple[str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[2],
) -> tuple[str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[3],
) -> tuple[str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[4],
) -> tuple[str, str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: Literal[5],
) -> tuple[str, str, str, str, str]: ...
@overload
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: int | None = None,
) -> tuple[str, ...]: ...
def split_str(
    text: str,
    /,
    *,
    separator: str = LIST_SEPARATOR,
    brackets: Iterable[tuple[str, str]] | None = None,
    n: int | None = None,
) -> tuple[str, ...]:
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
        return tuple(texts)
    if len(texts) != n:
        raise _SplitStrCountError(text=text, n=n, texts=texts)
    return tuple(texts)


def _split_str_brackets(
    text: str,
    brackets: Iterable[tuple[str, str]],
    /,
    *,
    separator: str = LIST_SEPARATOR,
) -> list[str]:
    brackets = list(brackets)
    opens, closes = transpose(brackets)
    close_to_open = {close: open_ for open_, close in brackets}

    escapes = map(escape, chain(chain.from_iterable(brackets), [separator]))
    pattern = re.compile("|".join(escapes))

    results: list[str] = []
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
    texts: list[str]

    @override
    def __str__(self) -> str:
        return f"Unable to split {pretty_repr(self.text)} into {self.n} part(s); got {len(self.texts)}"


@dataclass(kw_only=True, slots=True)
class _SplitStrClosingBracketMismatchedError(SplitStrError):
    opening_token: str
    opening_position: int
    closing_token: str
    closing_position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {pretty_repr(self.text)}; got mismatched {pretty_repr(self.opening_token)} at position {self.opening_position} and {pretty_repr(self.closing_token)} at position {self.closing_position}"


@dataclass(kw_only=True, slots=True)
class _SplitStrClosingBracketUnmatchedError(SplitStrError):
    token: str
    position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {pretty_repr(self.text)}; got unmatched {pretty_repr(self.token)} at position {self.position}"


@dataclass(kw_only=True, slots=True)
class _SplitStrOpeningBracketUnmatchedError(SplitStrError):
    token: str
    position: int

    @override
    def __str__(self) -> str:
        return f"Unable to split {pretty_repr(self.text)}; got unmatched {pretty_repr(self.token)} at position {self.position}"


def join_strs(
    texts: Iterable[str], /, *, sort: bool = False, separator: str = LIST_SEPARATOR
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


def _escape_separator(*, separator: str = LIST_SEPARATOR) -> str:
    return f"\\{separator}"


##


class secret_str(str):  # noqa: N801
    """A string with an obfuscated representation."""

    __slots__ = ("_text",)
    _REPR: ClassVar[str] = "***"

    def __init__(self, text: str, /) -> None:
        super().__init__()
        self._text = text

    @override
    def __repr__(self) -> str:
        return self._REPR

    @override
    def __str__(self) -> str:
        return self._REPR

    @property
    def str(self) -> str:
        return self._text


##


def str_encode(obj: Any, /) -> bytes:
    """Return the string representation of the object encoded as bytes."""
    return str(obj).encode()


##


@overload
def to_bool(bool_: MaybeCallableBoolLike, /) -> bool: ...
@overload
def to_bool(bool_: None, /) -> None: ...
@overload
def to_bool(bool_: Sentinel, /) -> Sentinel: ...
def to_bool(
    bool_: MaybeCallableBoolLike | None | Sentinel, /
) -> bool | None | Sentinel:
    """Convert to a bool."""
    match bool_:
        case bool() | None | Sentinel():
            return bool_
        case str():
            return parse_bool(bool_)
        case Callable() as func:
            return to_bool(func())
        case never:
            assert_never(never)


##


@overload
def to_str(text: MaybeCallableStr, /) -> str: ...
@overload
def to_str(text: None, /) -> None: ...
@overload
def to_str(text: Sentinel, /) -> Sentinel: ...
def to_str(text: MaybeCallableStr | None | Sentinel, /) -> str | None | Sentinel:
    """Convert to a string."""
    match text:
        case str() | None | Sentinel():
            return text
        case Callable() as func:
            return to_str(func())
        case never:
            assert_never(never)


##


__all__ = [
    "ParseBoolError",
    "ParseNoneError",
    "SplitKeyValuePairsError",
    "SplitStrError",
    "join_strs",
    "parse_bool",
    "parse_none",
    "prompt_bool",
    "secret_str",
    "split_f_str_equals",
    "split_key_value_pairs",
    "split_str",
    "str_encode",
    "to_bool",
    "to_str",
]
