from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial as _partial
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import override

from utilities.iterables import one
from utilities.text import ensure_str

if TYPE_CHECKING:
    from collections.abc import Iterator

_T = TypeVar("_T")


class partial(_partial[_T]):  # noqa: N801
    """Partial which accepts Ellipsis for positional arguments."""

    @override
    def __call__(self, *args: Any, **kwargs: Any) -> _T:
        iter_args = iter(args)
        head = (next(iter_args) if arg is ... else arg for arg in self.args)
        return self.func(*head, *iter_args, **{**self.keywords, **kwargs})


@contextmanager
def redirect_empty_reduce() -> Iterator[None]:
    """Redirect to the `EmptyReduceError`."""
    try:
        yield
    except TypeError as error:
        arg = ensure_str(one(error.args))
        if arg == "reduce() of empty iterable with no initial value":
            raise EmptyReduceError from None
        raise


@dataclass(kw_only=True)
class EmptyReduceError(Exception):
    @override
    def __str__(self) -> str:
        return "reduce() must not be called over an empty iterable, or must have an initial value."


__all__ = ["EmptyReduceError", "partial", "redirect_empty_reduce"]
