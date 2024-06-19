from __future__ import annotations

from dataclasses import dataclass
from math import isclose, isfinite, isnan
from typing import Annotated

from typing_extensions import override

from utilities.errors import ImpossibleCaseError

# functions


def is_equal(x: float, y: float, /) -> bool:
    """Check if x == y."""
    return (x == y) or (isnan(x) and isnan(y))


def is_equal_or_approx(
    x: int | tuple[int, float], y: int | tuple[int, float], /
) -> bool:
    """Check if x == y, or approximately."""
    if isinstance(x, int) and isinstance(y, int):
        return x == y
    if isinstance(x, int) and isinstance(y, tuple):
        return isclose(x, y[0], rel_tol=y[1])
    if isinstance(x, tuple) and isinstance(y, int):
        return isclose(x[0], y, rel_tol=x[1])
    if isinstance(x, tuple) and isinstance(y, tuple):
        return isclose(x[0], y[0], rel_tol=max(x[1], y[1]))
    raise ImpossibleCaseError(case=[f"{x=}", f"{y=}"])  # pragma: no cover


def is_at_least(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x >= y."""
    return (x >= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_at_least_or_nan(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x >= y or x == nan."""
    return is_at_least(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_at_most(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x <= y."""
    return (x <= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_at_most_or_nan(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x <= y or x == nan."""
    return is_at_most(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_between(
    x: float,
    low: float,
    high: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if low <= x <= high."""
    return is_at_least(x, low, rel_tol=rel_tol, abs_tol=abs_tol) and is_at_most(
        x, high, rel_tol=rel_tol, abs_tol=abs_tol
    )


def is_between_or_nan(
    x: float,
    low: float,
    high: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if low <= x <= high or x == nan."""
    return is_between(x, low, high, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite(x: float, /) -> bool:
    """Check if -inf < x < inf."""
    return isfinite(x)


def is_finite_and_integral(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf and x == int(x)."""
    return isfinite(x) & is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_integral_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf and x == int(x), or x == nan."""
    return is_finite_and_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(x)


def is_finite_and_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < 0."""
    return isfinite(x) and is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < 0 or x == nan."""
    return is_finite_and_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite_and_non_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 <= x < inf."""
    return isfinite(x) and is_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 <= x < inf or x == nan."""
    return is_finite_and_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite_and_non_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x <= 0."""
    return isfinite(x) and is_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x <= 0 or x == nan."""
    return is_finite_and_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite_and_non_zero(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf, x != 0."""
    return isfinite(x) and is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_zero_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x != 0 or x == nan."""
    return is_finite_and_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite_and_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 < x < inf."""
    return isfinite(x) and is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 < x < inf or x == nan."""
    return is_finite_and_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_finite_or_nan(x: float, /) -> bool:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) or isnan(x)


def is_greater_than(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > y."""
    return (x > y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_greater_than_or_nan(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > y or x == nan."""
    return is_greater_than(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_integral(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == int(x)."""
    try:
        rounded = round(x)
    except (OverflowError, ValueError):
        rounded = x
    return _is_close(x, rounded, rel_tol=rel_tol, abs_tol=abs_tol)


def is_integral_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == int(x) or x == nan."""
    return is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(x)


def is_less_than(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x < y."""
    return (x < y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_less_than_or_nan(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x < y or x == nan."""
    return is_less_than(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x < 0."""
    return is_less_than(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x < 0 or x == nan."""
    return is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_non_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x >= 0."""
    return is_at_least(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_non_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x >= 0 or x == nan."""
    return is_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_non_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x <= 0."""
    return is_at_most(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_non_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x <=0 or x == nan."""
    return is_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_non_zero(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x != 0."""
    return not _is_close(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_non_zero_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x != 0 or x == nan."""
    return is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > 0."""
    return is_greater_than(x, 0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > 0 or x == nan."""
    return is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_zero(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0."""
    return _is_close(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_zero_or_finite_and_non_micro(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) or is_finite_and_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_zero_or_finite_and_non_micro_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0), or x == nan."""
    return is_zero_or_finite_and_non_micro(
        x, rel_tol=rel_tol, abs_tol=abs_tol
    ) or isnan(x)


def is_zero_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_zero_or_non_micro(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0 or ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) or is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_zero_or_non_micro_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0 or ~isclose(x, 0) or x == nan."""
    return is_zero_or_non_micro(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def _is_close(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == y."""
    return isclose(
        x,
        y,
        **({} if rel_tol is None else {"rel_tol": rel_tol}),
        **({} if abs_tol is None else {"abs_tol": abs_tol}),
    )


# annotations


# int
IntNeg = Annotated[int, is_negative]
IntNonNeg = Annotated[int, is_non_negative]
IntNonPos = Annotated[int, is_non_positive]
IntNonZr = Annotated[int, is_non_zero]
IntPos = Annotated[int, is_positive]
IntZr = Annotated[int, is_zero]

# float
FloatFin = Annotated[float, is_finite]
FloatFinInt = Annotated[float, is_finite_and_integral]
FloatFinIntNan = Annotated[float, is_finite_and_integral_or_nan]
FloatFinNeg = Annotated[float, is_finite_and_negative]
FloatFinNegNan = Annotated[float, is_finite_and_negative_or_nan]
FloatFinNonNeg = Annotated[float, is_finite_and_non_negative]
FloatFinNonNegNan = Annotated[float, is_finite_and_non_negative_or_nan]
FloatFinNonPos = Annotated[float, is_finite_and_non_positive]
FloatFinNonPosNan = Annotated[float, is_finite_and_non_positive_or_nan]
FloatFinNonZr = Annotated[float, is_finite_and_non_zero]
FloatFinNonZrNan = Annotated[float, is_finite_and_non_zero_or_nan]
FloatFinPos = Annotated[float, is_finite_and_positive]
FloatFinPosNan = Annotated[float, is_finite_and_positive_or_nan]
FloatFinNan = Annotated[float, is_finite_or_nan]
FloatInt = Annotated[float, is_integral]
FloatIntNan = Annotated[float, is_integral_or_nan]
FloatNeg = Annotated[float, is_negative]
FloatNegNan = Annotated[float, is_negative_or_nan]
FloatNonNeg = Annotated[float, is_non_negative]
FloatNonNegNan = Annotated[float, is_non_negative_or_nan]
FloatNonPos = Annotated[float, is_non_positive]
FloatNonPosNan = Annotated[float, is_non_positive_or_nan]
FloatNonZr = Annotated[float, is_non_zero]
FloatNonZrNan = Annotated[float, is_non_zero_or_nan]
FloatPos = Annotated[float, is_positive]
FloatPosNan = Annotated[float, is_positive_or_nan]
FloatZr = Annotated[float, is_zero]
FloatZrFinNonMic = Annotated[float, is_zero_or_finite_and_non_micro]
FloatZrFinNonMicNan = Annotated[float, is_zero_or_finite_and_non_micro_or_nan]
FloatZrNan = Annotated[float, is_zero_or_nan]
FloatZrNonMic = Annotated[float, is_zero_or_non_micro]
FloatZrNonMicNan = Annotated[float, is_zero_or_non_micro_or_nan]


# checks


def check_integer(
    n: int,
    /,
    *,
    equal: int | None = None,
    equal_or_approx: int | tuple[int, float] | None = None,
    min: int | None = None,  # noqa: A002
    max: int | None = None,  # noqa: A002
) -> None:
    """Check the properties of an integer."""
    if (equal is not None) and (n != equal):
        raise _CheckIntegerEqualError(n=n, equal=equal)
    if (equal_or_approx is not None) and not is_equal_or_approx(n, equal_or_approx):
        raise _CheckIntegerEqualOrApproxError(n=n, equal_or_approx=equal_or_approx)
    if (min is not None) and (n < min):
        raise _CheckIntegerMinError(n=n, min_=min)
    if (max is not None) and (n > max):
        raise _CheckIntegerMaxError(n=n, max_=max)


@dataclass(kw_only=True)
class CheckIntegerError(Exception):
    n: int


@dataclass(kw_only=True)
class _CheckIntegerEqualError(CheckIntegerError):
    equal: int

    @override
    def __str__(self) -> str:
        return f"Integer must be equal to {self.equal}; got {self.n}"


@dataclass(kw_only=True)
class _CheckIntegerEqualOrApproxError(CheckIntegerError):
    equal_or_approx: int | tuple[int, float]

    @override
    def __str__(self) -> str:
        match self.equal_or_approx:
            case target, error:
                desc = f"approximately equal to {target} (error {error:%})"
            case target:
                desc = f"equal to {target}"
        return f"Integer must be {desc}; got {self.n}"


@dataclass(kw_only=True)
class _CheckIntegerMinError(CheckIntegerError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Integer must be at least {self.min_}; got {self.n}"


@dataclass(kw_only=True)
class _CheckIntegerMaxError(CheckIntegerError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Integer must be at most {self.max_}; got {self.n}"


__all__ = [
    "CheckIntegerError",
    "FloatFin",
    "FloatFinInt",
    "FloatFinIntNan",
    "FloatFinNan",
    "FloatFinNeg",
    "FloatFinNegNan",
    "FloatFinNonNeg",
    "FloatFinNonNegNan",
    "FloatFinNonPos",
    "FloatFinNonPosNan",
    "FloatFinNonZr",
    "FloatFinNonZrNan",
    "FloatFinPos",
    "FloatFinPosNan",
    "FloatInt",
    "FloatIntNan",
    "FloatNeg",
    "FloatNegNan",
    "FloatNonNeg",
    "FloatNonNegNan",
    "FloatNonPos",
    "FloatNonPosNan",
    "FloatNonZr",
    "FloatNonZrNan",
    "FloatPos",
    "FloatPosNan",
    "FloatZr",
    "FloatZrFinNonMic",
    "FloatZrFinNonMicNan",
    "FloatZrNan",
    "FloatZrNonMic",
    "FloatZrNonMicNan",
    "IntNeg",
    "IntNonNeg",
    "IntNonPos",
    "IntNonZr",
    "IntPos",
    "IntZr",
    "check_integer",
    "is_at_least",
    "is_at_least_or_nan",
    "is_at_most",
    "is_at_most_or_nan",
    "is_between",
    "is_between_or_nan",
    "is_finite",
    "is_finite_and_integral",
    "is_finite_and_integral_or_nan",
    "is_finite_and_negative",
    "is_finite_and_negative_or_nan",
    "is_finite_and_non_negative",
    "is_finite_and_non_negative_or_nan",
    "is_finite_and_non_positive",
    "is_finite_and_non_positive_or_nan",
    "is_finite_and_non_zero",
    "is_finite_and_non_zero_or_nan",
    "is_finite_and_positive",
    "is_finite_and_positive_or_nan",
    "is_finite_or_nan",
    "is_greater_than",
    "is_greater_than_or_nan",
    "is_integral",
    "is_integral_or_nan",
    "is_less_than",
    "is_less_than_or_nan",
    "is_negative",
    "is_negative_or_nan",
    "is_non_negative",
    "is_non_negative_or_nan",
    "is_non_positive",
    "is_non_positive_or_nan",
    "is_non_zero",
    "is_non_zero_or_nan",
    "is_positive",
    "is_positive_or_nan",
    "is_zero",
    "is_zero_or_finite_and_non_micro",
    "is_zero_or_finite_and_non_micro_or_nan",
    "is_zero_or_nan",
    "is_zero_or_non_micro",
    "is_zero_or_non_micro_or_nan",
]
