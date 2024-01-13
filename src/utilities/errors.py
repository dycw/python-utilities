from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from re import search
from typing import TypeVar, cast

from more_itertools import one
from typing_extensions import override


@dataclass(frozen=True, kw_only=True, slots=True)
class ImpossibleCaseError(Exception):
    case: list[str]

    @override
    def __str__(self) -> str:
        return (  # pragma: no cover
            "Arguments must be possible: {}".format(", ".join(self.case))
        )


@contextmanager
def redirect_error(
    old: type[Exception] | tuple[type[Exception], ...],
    new: Exception | type[Exception],
    /,
    *,
    match: str | None = None,
) -> Iterator[None]:
    """Context-manager for redirecting a specific type of error."""

    try:
        yield
    except Exception as error:
        if not isinstance(error, old):
            raise
        if match is None:
            raise new from error
        arg = one(
            error.args,
            too_short=RedirectErrorError(f"{error.args=}"),
            too_long=RedirectErrorError(f"{error.args=}"),
        )
        if not isinstance(arg, str):
            msg = f"{arg=}"
            raise RedirectErrorError(msg) from error
        if search(match, arg):
            raise new from error
        raise


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


__all__ = ["ImpossibleCaseError", "RedirectErrorError", "redirect_error", "retry"]
