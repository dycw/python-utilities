from __future__ import annotations

from operator import add, eq, ge, gt, le, lt, mul, ne, sub, truediv
from typing import TYPE_CHECKING, Any, Self, override

from whenever import TimeDelta

from utilities.constants import SECOND
from utilities.core import get_now_local

if TYPE_CHECKING:
    from collections.abc import Callable

    from whenever import ZonedDateTime


class Timer:
    """Context manager for timing blocks of code."""

    def __init__(self) -> None:
        super().__init__()
        self._start: ZonedDateTime = get_now_local()
        self._end: ZonedDateTime | None = None

    # arithmetic

    def __add__(self, other: Any) -> TimeDelta:
        return self._apply_op(add, other)

    def __float__(self) -> float:
        return self.timedelta.in_seconds()

    def __sub__(self, other: Any) -> TimeDelta:
        return self._apply_op(sub, other)

    def __mul__(self, other: Any) -> TimeDelta:
        return self._apply_op(mul, other)

    def __truediv__(self, other: Any) -> TimeDelta:
        return self._apply_op(truediv, other)

    # context manager

    def __enter__(self) -> Self:
        self._start = get_now_local()
        return self

    def __exit__(self, *_: object) -> bool:
        self._end = get_now_local()
        return False

    # hash

    @override
    def __hash__(self) -> int:
        return hash((id(self), self._start, self._end))

    # repr

    @override
    def __repr__(self) -> str:
        return self.timedelta.format_iso()

    @override
    def __str__(self) -> str:
        return self.timedelta.format_iso()

    # comparison

    @override
    def __eq__(self, other: object) -> bool:
        return self._apply_op(eq, other, cast=True, type_error=False)

    def __ge__(self, other: Any) -> bool:
        return self._apply_op(ge, other, cast=True)

    def __gt__(self, other: Any) -> bool:
        return self._apply_op(gt, other, cast=True)

    def __le__(self, other: Any) -> bool:
        return self._apply_op(le, other, cast=True)

    def __lt__(self, other: Any) -> bool:
        return self._apply_op(lt, other, cast=True)

    @override
    def __ne__(self, other: object) -> bool:
        return self._apply_op(ne, other, cast=True, type_error=True)

    # properties

    @property
    def timedelta(self) -> TimeDelta:
        """The elapsed time, as a `timedelta` object."""
        end_use = get_now_local() if (end := self._end) is None else end
        return end_use - self._start

    # private

    def _apply_op(
        self,
        op: Callable[[Any, Any], Any],
        other: Any,
        /,
        *,
        cast: bool = False,
        type_error: bool | None = None,
    ) -> Any:
        other_use = other.timedelta if isinstance(other, Timer) else other
        if cast:
            match other_use:
                case int() | float():
                    other_use = other_use * SECOND
                case TimeDelta():
                    ...
                case _:
                    if type_error is not None:
                        return type_error
                    raise TypeError  # pragma: no cover
        return op(self.timedelta, other_use)


__all__ = ["Timer"]
