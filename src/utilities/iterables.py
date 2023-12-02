from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Sized
from typing import Any, TypeGuard

from utilities.math import is_equal_or_approx
from utilities.types import ensure_hashable


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise CheckDuplicatesError(msg)


class CheckDuplicatesError(Exception):
    ...


def check_length(
    obj: Sized,
    /,
    *,
    equal: int | None = None,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    """Check the length of an object."""
    if (equal is not None) and (len(obj) != equal):
        msg = f"{obj=}, {equal=}"
        raise CheckLengthError(msg)
    if (equal_or_approx is not None) and not is_equal_or_approx(
        len(obj), equal_or_approx
    ):
        msg = f"{obj=}, {equal_or_approx=}"
        raise CheckLengthError(msg)
    if (min is not None) and (len(obj) < min):
        msg = f"{obj=}, {min=}"
        raise CheckLengthError(msg)
    if (max is not None) and (len(obj) > max):
        msg = f"{obj=}, {max=}"
        raise CheckLengthError(msg)


class CheckLengthError(Exception):
    ...


def ensure_hashables(
    *args: Any, **kwargs: Any
) -> tuple[list[Hashable], dict[str, Hashable]]:
    """Ensure a set of positional & keyword arguments are all hashable."""
    hash_args = list(map(ensure_hashable, args))
    hash_kwargs = {k: ensure_hashable(v) for k, v in kwargs.items()}
    return hash_args, hash_kwargs


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    try:
        iter(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


__all__ = [
    "CheckDuplicatesError",
    "check_duplicates",
    "ensure_hashables",
    "is_iterable_not_str",
]
