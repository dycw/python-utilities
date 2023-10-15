from __future__ import annotations

from collections import Counter
from collections.abc import Hashable
from collections.abc import Iterable
from collections.abc import Iterator
from functools import partial
from itertools import islice
from typing import Any
from typing import TypeVar
from typing import cast

_T = TypeVar("_T")


def check_duplicates(iterable: Iterable[Hashable], /) -> None:
    """Check if an iterable contains any duplicates."""
    dup = {k: v for k, v in Counter(iterable).items() if v > 1}
    if len(dup) >= 1:
        msg = f"{dup=}"
        raise IterableContainsDuplicatesError(msg)


class IterableContainsDuplicatesError(ValueError):
    """Raised when an iterable contains duplicates."""


def chunked(
    iterable: Iterable[_T], /, *, n: int | None = None, strict: bool = False
) -> Iterator[list[_T]]:
    """Break iterable into lists of length n."""
    iterator = cast(
        Iterator[list[_T]], iter(partial(take, n, iter(iterable)), [])  # type: ignore
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


def is_iterable_not_str(x: Any, /) -> bool:
    """Check if an object is iterable, but not a string."""
    try:
        iter(x)
    except TypeError:
        return False
    return not isinstance(x, str)


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
