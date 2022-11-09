from collections.abc import Iterable
from collections.abc import Iterator
from functools import partial
from itertools import islice
from typing import TypeVar

from beartype import beartype


_T = TypeVar("_T")


@beartype
def take(n: int | None, iterable: Iterable[_T], /) -> list[_T]:
    """Return first n items of the iterable as a list."""

    return list(islice(iterable, n))


@beartype
def chunked(iterable: Iterable[_T], n: int | None, /) -> Iterator[Iterable[_T]]:
    """Return first n items of the iterable as a list."""

    return iter(partial(take, n, iter(iterable)), [])
