from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Iterator, Sized
from functools import partial
from itertools import islice
from typing import Any, TypeGuard, TypeVar, cast

_T = TypeVar("_T")


def always_iterable(
    obj: _T | Iterable[_T],
    /,
    *,
    base_type: type[Any] | tuple[type[Any], ...] | None = (str, bytes),
) -> Iterator[_T]:
    """If *obj* is iterable, return an iterator over its items."""
    if obj is None:
        return iter(())
    if (base_type is not None) and isinstance(obj, base_type):
        return iter(cast(Iterable[_T], (obj,)))
    try:
        return iter(cast(Iterable[_T], obj))
    except TypeError:
        return iter(cast(Iterable[_T], (obj,)))


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise IterableContainsDuplicatesError(msg)


class IterableContainsDuplicatesError(Exception):
    """Raised when an iterable contains duplicates."""


def chunked(
    iterable: Iterable[_T], /, *, n: int | None = None, strict: bool = False
) -> Iterator[list[_T]]:
    """Break iterable into lists of length n."""
    iterator = cast(
        Iterator[list[_T]],
        iter(partial(take, n, iter(iterable)), []),  # type: ignore
    )
    if strict:  # pragma: no cover
        if n is None:
            msg = "n must not be None when using strict mode."
            raise ValueError(msg)

        def ret() -> Iterator[list[_T]]:
            for chunk in iterator:
                if len(chunk) != n:
                    msg = "iterable is not divisible by n."
                    raise ValueError(msg)
                yield chunk

        return iter(ret())
    return iterator


def is_iterable_not_str(obj: Any, /) -> TypeGuard[Iterable[Any]]:
    """Check if an object is iterable, but not a string."""
    try:
        iter(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


def is_sized_not_str(obj: Any, /) -> TypeGuard[Sized]:
    """Check if an object is sized, but not a string."""
    try:
        _ = len(obj)
    except TypeError:
        return False
    return not isinstance(obj, str)


def one(iterable: Iterable[_T], /) -> _T:
    """Return the only item from iterable."""
    it = iter(iterable)
    try:
        first = next(it)
    except StopIteration:
        raise EmptyIterableError from None
    try:
        second = next(it)
    except StopIteration:
        return first
    else:
        msg = (
            f"Expected exactly one item in iterable, but got {first!r}, "
            f"{second!r}, and perhaps more."
        )
        raise MultipleElementsError(msg)


class EmptyIterableError(Exception):
    """Raised when an iterable is empty."""


class MultipleElementsError(Exception):
    """Raised when an iterable contains multiple elements."""


def take(n: int, iterable: Iterable[_T], /) -> list[_T]:
    """Return first n items of the iterable as a list."""
    return list(islice(iterable, n))


__all__ = [
    "always_iterable",
    "check_duplicates",
    "chunked",
    "EmptyIterableError",
    "is_iterable_not_str",
    "is_sized_not_str",
    "IterableContainsDuplicatesError",
    "MultipleElementsError",
    "one",
    "take",
]
