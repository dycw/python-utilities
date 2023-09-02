import datetime as dt
from collections.abc import Callable
from numbers import Number
from operator import eq, ge, gt, le, lt, ne
from timeit import default_timer
from typing import Any, Optional

from beartype import beartype
from typing_extensions import Self, override


class Timer:
    """Context manager for timing blocks of code."""

    @beartype
    def __init__(self: Self) -> None:
        super().__init__()
        self._start = default_timer()
        self._end: Optional[float] = None

    @beartype
    def __enter__(self: "Timer") -> "Timer":
        self._start = default_timer()
        return self

    @beartype
    def __exit__(self: Self, *_: object) -> bool:
        self._end = default_timer()
        return False

    @beartype
    def __float__(self: Self) -> float:
        end_use = default_timer() if (end := self._end) is None else end
        return end_use - self._start

    @override
    @beartype
    def __repr__(self: Self) -> str:
        return str(self.timedelta)

    @override
    @beartype
    def __str__(self: Self) -> str:
        return str(self.timedelta)

    @override
    @beartype
    def __eq__(self: Self, other: object) -> bool:
        return self._compare(other, eq)

    @beartype
    def __ge__(self: Self, other: Any) -> bool:
        return self._compare(other, ge)

    @beartype
    def __gt__(self: Self, other: Any) -> bool:
        return self._compare(other, gt)

    @beartype
    def __le__(self: Self, other: Any) -> bool:
        return self._compare(other, le)

    @beartype
    def __lt__(self: Self, other: Any) -> bool:
        return self._compare(other, lt)

    @override
    @beartype
    def __ne__(self: Self, other: object) -> bool:
        return self._compare(other, ne)

    @property
    @beartype
    def timedelta(self: Self) -> dt.timedelta:
        """The elapsed time, as a `timedelta` object."""
        return dt.timedelta(seconds=float(self))

    @beartype
    def _compare(self: Self, other: Any, op: Callable[[Any, Any], bool], /) -> bool:
        if isinstance(other, (Number, Timer)):
            return op(float(self), other)
        if isinstance(other, dt.timedelta):
            return op(self.timedelta, other)
        msg = f"Invalid type: {other=}"
        raise TypeError(msg)
