from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from re import search
from typing import NoReturn, TypeVar, cast


class DirectoryExistsError(Exception):
    ...


def redirect_error(
    old: Exception, pattern: str, new: Exception | type[Exception], /
) -> NoReturn:
    """Redirect an error if a matching string is found."""
    args = old.args
    try:
        (msg,) = args
    except ValueError:
        raise RedirectErrorError(args) from None
    else:
        if isinstance(msg, str) and search(pattern, msg):
            raise new from None
        raise old


class RedirectErrorError(Exception):
    ...


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


__all__ = ["DirectoryExistsError", "RedirectErrorError", "redirect_error", "retry"]
