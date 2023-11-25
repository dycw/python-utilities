from __future__ import annotations

from collections.abc import Iterable
from re import escape
from typing import TypeVar

from more_itertools import one as _one

from utilities.errors import redirect_error

_T = TypeVar("_T")


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


class OneError(Exception):
    ...


class OneEmptyError(OneError):
    ...


class OneNonUniqueError(OneError):
    ...


__all__ = ["one", "OneError", "OneEmptyError", "OneNonUniqueError"]
