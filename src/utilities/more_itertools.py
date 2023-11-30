from __future__ import annotations

from collections.abc import Iterable, Iterator
from re import escape
from typing import Any, TypeVar

from more_itertools import always_iterable as _always_iterable
from more_itertools import one as _one

from utilities.errors import redirect_error

_T = TypeVar("_T")


def always_iterable(
    obj: _T | Iterable[_T],
    /,
    *,
    base_type: type[Any] | tuple[type[Any], ...] | None = (str, bytes),
) -> Iterator[_T]:
    """Typed version of `always_iterable`."""
    return _always_iterable(obj, base_type=base_type)


def one(iterable: Iterable[_T], /) -> _T:
    """Custom version of `one` with separate exceptions."""
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


__all__ = ["OneEmptyError", "OneError", "OneNonUniqueError", "always_iterable", "one"]
