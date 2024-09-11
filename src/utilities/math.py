from __future__ import annotations

from dataclasses import dataclass
from math import exp, isclose, isfinite, isnan, log, log10
from typing import Literal, overload

from typing_extensions import override

from utilities.errors import ImpossibleCaseError

MIN_INT8, MAX_INT8 = -(2 ** (8 - 1)), 2 ** (8 - 1) - 1
MIN_INT16, MAX_INT16 = -(2 ** (16 - 1)), 2 ** (16 - 1) - 1
MIN_INT32, MAX_INT32 = -(2 ** (32 - 1)), 2 ** (32 - 1) - 1
MIN_INT64, MAX_INT64 = -(2 ** (64 - 1)), 2 ** (64 - 1) - 1
MIN_UINT8, MAX_UINT8 = 0, 2**8 - 1
MIN_UINT16, MAX_UINT16 = 0, 2**16 - 1
MIN_UINT32, MAX_UINT32 = 0, 2**32 - 1
MIN_UINT64, MAX_UINT64 = 0, 2**64 - 1

# functions


@dataclass(frozen=True, kw_only=True)
class _EWMParameters:
    """A set of EWM parameters."""

    com: float
    span: float
    half_life: float
    alpha: float


def ewm_parameters(
    *,
    com: float | None = None,
    span: float | None = None,
    half_life: float | None = None,
    alpha: float | None = None,
) -> _EWMParameters:
    """Compute a set of EWM parameters."""
    if (com is not None) and (span is None) and (half_life is None) and (alpha is None):
        if com <= 0:
            raise _EWMParametersCOMError(com=com)
        alpha = 1 / (1 + com)
        return _EWMParameters(
            com=com,
            span=_ewm_parameters_alpha_to_span(alpha),
            half_life=_ewm_parameters_alpha_to_half_life(alpha),
            alpha=alpha,
        )
    if (com is None) and (span is not None) and (half_life is None) and (alpha is None):
        if span <= 1:
            raise _EWMParametersSpanError(span=span)
        alpha = 2 / (span + 1)
        return _EWMParameters(
            com=_ewm_parameters_alpha_to_com(alpha),
            span=span,
            half_life=_ewm_parameters_alpha_to_half_life(alpha),
            alpha=alpha,
        )
    if (com is None) and (span is None) and (half_life is not None) and (alpha is None):
        if half_life <= 0:
            raise _EWMParametersHalfLifeError(half_life=half_life)
        alpha = 1 - exp(-log(2) / half_life)
        return _EWMParameters(
            com=_ewm_parameters_alpha_to_com(alpha),
            span=_ewm_parameters_alpha_to_span(alpha),
            half_life=half_life,
            alpha=alpha,
        )
    if (com is None) and (span is None) and (half_life is None) and (alpha is not None):
        if not (0 < alpha < 1):
            raise _EWMParametersAlphaError(alpha=alpha)
        return _EWMParameters(
            com=_ewm_parameters_alpha_to_com(alpha),
            span=_ewm_parameters_alpha_to_span(alpha),
            half_life=_ewm_parameters_alpha_to_half_life(alpha),
            alpha=alpha,
        )
    raise _EWMParametersArgumentsError(
        com=com, span=span, half_life=half_life, alpha=alpha
    )


@dataclass(kw_only=True)
class EWMParametersError(Exception):
    com: float | None = None
    span: float | None = None
    half_life: float | None = None
    alpha: float | None = None


@dataclass(kw_only=True)
class _EWMParametersCOMError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Center of mass (γ) must be positive; got {self.com}"  # noqa: RUF001


@dataclass(kw_only=True)
class _EWMParametersSpanError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Span (θ) must be greater than 1; got {self.span}"


class _EWMParametersHalfLifeError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Half-life (λ) must be positive; got {self.half_life}"


class _EWMParametersAlphaError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Smoothing factor (α) must be between 0 and 1 (exclusive); got {self.alpha}"  # noqa: RUF001


class _EWMParametersArgumentsError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Exactly one of center of mass (γ), span (θ), half-life (λ) and smoothing factor (α) must be given; got γ={self.com}, θ={self.span}, λ={self.half_life} and α={self.alpha}"  # noqa: RUF001


def _ewm_parameters_alpha_to_com(alpha: float, /) -> float:
    return 1 / alpha - 1


def _ewm_parameters_alpha_to_span(alpha: float, /) -> float:
    return 2 / alpha - 1


def _ewm_parameters_alpha_to_half_life(alpha: float, /) -> float:
    return -log(2) / log(1 - alpha)


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


def number_of_decimals(x: float, /, *, max_decimals: int = 20) -> int:
    """Get the number of decimals."""
    _, frac = divmod(x, 1)
    results = (
        s for s in range(max_decimals + 1) if _number_of_decimals_check_scale(frac, s)
    )
    try:
        return next(results)
    except StopIteration:
        raise NumberOfDecimalsError(x=x, max_decimals=max_decimals) from None


def _number_of_decimals_check_scale(frac: float, scale: int, /) -> bool:
    scaled = 10**scale * frac
    return isclose(scaled, round(scaled))


@dataclass(kw_only=True)
class NumberOfDecimalsError(Exception):
    x: float
    max_decimals: int

    @override
    def __str__(self) -> str:
        return f"Could not determine number of decimals of {self.x} (up to {self.max_decimals})"


@overload
def order_of_magnitude(x: float, /, *, round_: Literal[True]) -> int: ...
@overload
def order_of_magnitude(x: float, /, *, round_: bool = False) -> float: ...
def order_of_magnitude(x: float, /, *, round_: bool = False) -> float:
    """Get the order of magnitude of a number."""
    result = log10(abs(x))
    return round(result) if round_ else result


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
    "MAX_INT8",
    "MAX_INT16",
    "MAX_INT32",
    "MAX_INT64",
    "MAX_UINT8",
    "MAX_UINT16",
    "MAX_UINT32",
    "MAX_UINT64",
    "MIN_INT8",
    "MIN_INT16",
    "MIN_INT32",
    "MIN_INT64",
    "MIN_UINT8",
    "MIN_UINT16",
    "MIN_UINT32",
    "MIN_UINT64",
    "CheckIntegerError",
    "EWMParametersError",
    "check_integer",
    "ewm_parameters",
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
    "number_of_decimals",
    "order_of_magnitude",
]
