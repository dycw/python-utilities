from collections.abc import Iterable
from typing import TypeVar

from beartype import beartype
from more_itertools import one as _one

from utilities.errors import redirect_error

_T = TypeVar("_T")


@beartype
def one(iterable: Iterable[_T], /) -> _T:
    """Return the unique item from `iterable`."""
    try:
        return _one(iterable)
    except ValueError as error1:
        try:
            redirect_error(
                error1,
                "too few items in iterable",
                EmptyIterableError(error1.args[0]),
            )
        except ValueError as error2:
            redirect_error(
                error2,
                "Expected exactly one item in iterable",
                MultipleElementsError(error2.args[0]),
            )


class EmptyIterableError(Exception):
    """Raised when an iterable is empty."""


class MultipleElementsError(Exception):
    """Raised when an iterable contains multiple elements."""
