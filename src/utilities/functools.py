from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial as _partial
from typing import Any, TypeVar

from typing_extensions import override

from utilities.errors import redirect_error

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
    with redirect_error(
        TypeError,
        EmptyReduceError,
        match=r"reduce\(\) of empty iterable with no initial value",
    ):
        yield


class EmptyReduceError(Exception):
    ...


__all__ = ["EmptyReduceError", "partial", "redirect_empty_reduce"]
