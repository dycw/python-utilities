from __future__ import annotations

import datetime as dt
from operator import add, eq, ge, gt, le, lt, mul, ne, sub, truediv
from timeit import default_timer
from typing import TYPE_CHECKING, Any, Self, overload

from typing_extensions import override

from utilities.types import Number

if TYPE_CHECKING:
    from collections.abc import Callable


class Timer:
    """Context manager for timing blocks of code."""

    def __init__(self) -> None:
        super().__init__()
        self._start = default_timer()
        self._end: float | None = None

    # arithmetic

    def __add__(self, other: Any) -> dt.timedelta:
        if isinstance(other, Number):
            return dt.timedelta(seconds=self._apply_op(add, other))
        if isinstance(other, Timer | dt.timedelta):
            return self._apply_op(add, other)
        return NotImplemented

    def __float__(self) -> float:
        end_use = default_timer() if (end := self._end) is None else end
        return end_use - self._start

    def __sub__(self, other: Any) -> dt.timedelta:
        if isinstance(other, Number):
            return dt.timedelta(seconds=self._apply_op(sub, other))
        if isinstance(other, Timer | dt.timedelta):
            return self._apply_op(sub, other)
        return NotImplemented

    def __mul__(self, other: Any) -> dt.timedelta:
        if isinstance(other, Number):
            return dt.timedelta(seconds=self._apply_op(mul, other))
        return NotImplemented

    @overload
    def __truediv__(self, other: Number) -> dt.timedelta: ...
    @overload
    def __truediv__(self, other: Timer | dt.timedelta) -> float: ...
    def __truediv__(self, other: Any) -> dt.timedelta | float:
        if isinstance(other, Number):
            return dt.timedelta(seconds=self._apply_op(truediv, other))
        if isinstance(other, Timer | dt.timedelta):
            return self._apply_op(truediv, other)
        return NotImplemented

    # context manager

    def __enter__(self) -> Self:
        self._start = default_timer()
        return self

    def __exit__(self, *_: object) -> bool:
        self._end = default_timer()
        return False

    # repr

    @override
    def __repr__(self) -> str:
        return str(self.timedelta)

    @override
    def __str__(self) -> str:
        return str(self.timedelta)

    # comparison

    @override
    def __eq__(self, other: object) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(eq, other)
        return False

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(ge, other)
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(gt, other)
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(le, other)
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(lt, other)
        return NotImplemented

    @override
    def __ne__(self, other: object) -> bool:
        if isinstance(other, Number | Timer | dt.timedelta):
            return self._apply_op(ne, other)
        return True

    # properties

    @property
    def timedelta(self) -> dt.timedelta:
        """The elapsed time, as a `timedelta` object."""
        return dt.timedelta(seconds=float(self))

    # private

    def _apply_op(self, op: Callable[[Any, Any], Any], other: Any, /) -> Any:
        if isinstance(other, Number):
            return op(float(self), other)
        if isinstance(other, Timer):
            return op(self.timedelta, other.timedelta)
        if isinstance(other, dt.timedelta):
            return op(self.timedelta, other)
        return NotImplemented  # pragma: no cover


__all__ = ["Timer"]
