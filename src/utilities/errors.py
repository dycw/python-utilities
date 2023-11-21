from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from re import search
from typing import NoReturn, TypeVar, cast

from utilities.text import ensure_str


class DirectoryExistsError(Exception):
    """Raised when a directory already exists."""


def redirect_error(
    old: Exception, pattern: str, new: Exception | type[Exception], /
) -> NoReturn:
    """Redirect an error if a matching string is found."""
    args = old.args
    try:
        (msg,) = args
    except ValueError:
        raise NoUniqueArgError(args) from None
    else:
        if search(pattern, ensure_str(msg)):
            raise new from None
        raise old


class NoUniqueArgError(Exception):
    """Raised when no unique argument can be found."""


_T = TypeVar("_T")
_TExc = TypeVar("_TExc", bound=Exception)


def retry(
    func: Callable[[], _T],
    error: type[Exception] | tuple[type[Exception], ...],
    callback: Callable[[_TExc], None],
    /,
    *,
    predicate: Callable[[_TExc], bool] | None = None,
) -> Callable[[], _T]:
    """Retry a function if an error is caught after the callback."""

    @wraps(func)
    def inner() -> _T:
        try:
            return func()
        except error as caught:
            caught = cast(_TExc, caught)
            if (predicate is None) or predicate(caught):
                callback(caught)
                return func()
            raise

    return inner


__all__ = ["DirectoryExistsError", "NoUniqueArgError", "redirect_error", "retry"]
