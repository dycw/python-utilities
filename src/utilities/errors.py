from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from utilities.text import EnsureStrError, ensure_str

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(kw_only=True, slots=True)
class ImpossibleCaseError(Exception):
    case: list[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(self.case)
        return f"Case must be possible: {desc}."


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
        match error.args:  #  do not import from utilities.iterables
            case (arg,):
                pass
            case _:
                raise _RedirectErrorNonUniqueArgError(
                    old=old, new=new, match=match, args=error.args
                ) from None
        try:
            msg = ensure_str(arg)
        except EnsureStrError:
            raise _RedirectErrorArgNotStringError(
                old=old, new=new, match=match, arg=arg
            ) from None
        if search(match, msg):
            raise new from error
        raise


@dataclass(kw_only=True, slots=True)
class RedirectErrorError(Exception):
    old: type[Exception] | tuple[type[Exception], ...]
    new: Exception | type[Exception]
    match: str | None = None


@dataclass(kw_only=True, slots=True)
class _RedirectErrorNonUniqueArgError(RedirectErrorError):
    args: tuple[Any, ...]

    @override
    def __str__(self) -> str:
        return f"Error must contain a unique argument; got {self.args}."


@dataclass(kw_only=True, slots=True)
class _RedirectErrorArgNotStringError(RedirectErrorError):
    arg: Any

    @override
    def __str__(self) -> str:
        return f"Error argument must be a string; got {self.arg}."


__all__ = ["ImpossibleCaseError", "RedirectErrorError", "redirect_error"]
