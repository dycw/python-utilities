from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any, TypeVar

from more_itertools import always_iterable as _always_iterable
from more_itertools import one as _one

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
    return _one(
        iterable,
        too_short=OneEmptyError(f"{iterable=}"),
        too_long=OneNonUniqueError(f"{iterable=}"),
    )


class OneError(Exception):
    ...


class OneEmptyError(OneError):
    ...


class OneNonUniqueError(OneError):
    ...


__all__ = ["OneEmptyError", "OneError", "OneNonUniqueError", "always_iterable", "one"]
