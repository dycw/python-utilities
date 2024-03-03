from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from numbers import Number
from operator import eq, ge, gt, le, lt, ne
from timeit import default_timer
from typing import TYPE_CHECKING, Any

from typing_extensions import Self, override

from utilities.types import EnsureClassError, ensure_class, get_class_name

if TYPE_CHECKING:
    from collections.abc import Callable


class Timer:
    """Context manager for timing blocks of code."""

    def __init__(self) -> None:
        super().__init__()
        self._start = default_timer()
        self._end: float | None = None

    def __enter__(self) -> Self:
        self._start = default_timer()
        return self

    def __exit__(self, *_: object) -> bool:
        self._end = default_timer()
        return False

    def __float__(self) -> float:
        end_use = default_timer() if (end := self._end) is None else end
        return end_use - self._start

    @override
    def __repr__(self) -> str:
        return str(self.timedelta)

    @override
    def __str__(self) -> str:
        return str(self.timedelta)

    @override
    def __eq__(self, other: object) -> bool:
        return self._compare(other, eq)

    def __ge__(self, other: Any) -> bool:
        return self._compare(other, ge)

    def __gt__(self, other: Any) -> bool:
        return self._compare(other, gt)

    def __le__(self, other: Any) -> bool:
        return self._compare(other, le)

    def __lt__(self, other: Any) -> bool:
        return self._compare(other, lt)

    @override
    def __ne__(self, other: object) -> bool:
        return self._compare(other, ne)

    @property
    def timedelta(self) -> dt.timedelta:
        """The elapsed time, as a `timedelta` object."""
        return dt.timedelta(seconds=float(self))

    def _compare(self, other: Any, op: Callable[[Any, Any], bool], /) -> bool:
        try:
            right = ensure_class(other, (Number, Timer, dt.timedelta))
        except EnsureClassError:
            raise TimerError(obj=other) from None
        left = float(self) if isinstance(right, Number | Timer) else self.timedelta
        return op(left, right)


@dataclass(kw_only=True)
class TimerError(Exception):
    obj: Any

    @override
    def __str__(self) -> str:
        return (
            "Timer must be compared to a number, Timer, or timedelta; got "
            f"{get_class_name(self.obj)} instead"
        )


__all__ = ["Timer", "TimerError"]
