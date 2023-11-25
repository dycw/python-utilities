from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Sized
from re import escape
from typing import Any, TypeGuard, TypeVar

from more_itertools import one as _one

from utilities.errors import redirect_error

_T = TypeVar("_T")


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise CheckDuplicatesError(msg)


class CheckDuplicatesError(Exception):
    """Raised when an iterable contains duplicates."""


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    try:
        iter(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


def is_sized_not_str(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized, but not a string."""
    try:
        _ = len(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


def one(iterable: Iterable[_T], /) -> _T:
    """Return the only item from iterable."""
    try:
        return _one(iterable)
    except ValueError as error:
        (msg,) = error.args
        try:
            pattern = "too few items in iterable (expected 1)"
            redirect_error(error, escape(pattern), OneEmptyError(msg))
        except ValueError:
            pattern = (
                "Expected exactly one item in iterable, but got .*, .*, and "
                "perhaps more"
            )
            redirect_error(error, pattern, OneNonUniqueError(msg))


class OneEmptyError(Exception):
    ...


class OneNonUniqueError(Exception):
    ...


__all__ = [
    "check_duplicates",
    "CheckDuplicatesError",
    "is_iterable_not_str",
    "is_sized_not_str",
    "one",
    "OneEmptyError",
    "OneNonUniqueError",
]
