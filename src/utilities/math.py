from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from math import ceil, exp, floor, isclose, isfinite, isinf, isnan, log, log10, modf
from re import Match, search
from typing import TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.errors import ImpossibleCaseError
from utilities.re import ExtractGroupsError, extract_groups

if TYPE_CHECKING:
    from utilities.types import MathRoundMode, Number, Sign


MIN_FLOAT32, MAX_FLOAT32 = -3.4028234663852886e38, 3.4028234663852886e38
MIN_FLOAT64, MAX_FLOAT64 = -1.7976931348623157e308, 1.7976931348623157e308
MIN_INT8, MAX_INT8 = -(2 ** (8 - 1)), 2 ** (8 - 1) - 1
MIN_INT16, MAX_INT16 = -(2 ** (16 - 1)), 2 ** (16 - 1) - 1
MIN_INT32, MAX_INT32 = -(2 ** (32 - 1)), 2 ** (32 - 1) - 1
MIN_INT64, MAX_INT64 = -(2 ** (64 - 1)), 2 ** (64 - 1) - 1
MIN_UINT8, MAX_UINT8 = 0, 2**8 - 1
MIN_UINT16, MAX_UINT16 = 0, 2**16 - 1
MIN_UINT32, MAX_UINT32 = 0, 2**32 - 1
MIN_UINT64, MAX_UINT64 = 0, 2**64 - 1


##


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


@dataclass(kw_only=True, slots=True)
class CheckIntegerError(Exception):
    n: int


@dataclass(kw_only=True, slots=True)
class _CheckIntegerEqualError(CheckIntegerError):
    equal: int

    @override
    def __str__(self) -> str:
        return f"Integer must be equal to {self.equal}; got {self.n}"


@dataclass(kw_only=True, slots=True)
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


@dataclass(kw_only=True, slots=True)
class _CheckIntegerMinError(CheckIntegerError):
    min_: int

    @override
    def __str__(self) -> str:
        return f"Integer must be at least {self.min_}; got {self.n}"


@dataclass(kw_only=True, slots=True)
class _CheckIntegerMaxError(CheckIntegerError):
    max_: int

    @override
    def __str__(self) -> str:
        return f"Integer must be at most {self.max_}; got {self.n}"


##


@dataclass(kw_only=True, slots=True)
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
    match com, span, half_life, alpha:
        case int() | float(), None, None, None:
            if com <= 0:
                raise _EWMParametersCOMError(com=com)
            alpha = 1 / (1 + com)
            return _EWMParameters(
                com=com,
                span=_ewm_parameters_alpha_to_span(alpha),
                half_life=_ewm_parameters_alpha_to_half_life(alpha),
                alpha=alpha,
            )
        case None, int() | float(), None, None:
            if span <= 1:
                raise _EWMParametersSpanError(span=span)
            alpha = 2 / (span + 1)
            return _EWMParameters(
                com=_ewm_parameters_alpha_to_com(alpha),
                span=span,
                half_life=_ewm_parameters_alpha_to_half_life(alpha),
                alpha=alpha,
            )
        case None, None, int() | float(), None:
            if half_life <= 0:
                raise _EWMParametersHalfLifeError(half_life=half_life)
            alpha = 1 - exp(-log(2) / half_life)
            return _EWMParameters(
                com=_ewm_parameters_alpha_to_com(alpha),
                span=_ewm_parameters_alpha_to_span(alpha),
                half_life=half_life,
                alpha=alpha,
            )
        case None, None, None, int() | float():
            if not (0 < alpha < 1):
                raise _EWMParametersAlphaError(alpha=alpha)
            return _EWMParameters(
                com=_ewm_parameters_alpha_to_com(alpha),
                span=_ewm_parameters_alpha_to_span(alpha),
                half_life=_ewm_parameters_alpha_to_half_life(alpha),
                alpha=alpha,
            )
        case _:
            raise _EWMParametersArgumentsError(
                com=com, span=span, half_life=half_life, alpha=alpha
            )


@dataclass(kw_only=True, slots=True)
class EWMParametersError(Exception):
    com: float | None = None
    span: float | None = None
    half_life: float | None = None
    alpha: float | None = None


@dataclass(kw_only=True, slots=True)
class _EWMParametersCOMError(EWMParametersError):
    @override
    def __str__(self) -> str:
        return f"Center of mass (γ) must be positive; got {self.com}"  # noqa: RUF001


@dataclass(kw_only=True, slots=True)
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
        return f"Exactly one of center of mass (γ), span (θ), half-life (λ) or smoothing factor (α) must be given; got γ={self.com}, θ={self.span}, λ={self.half_life} and α={self.alpha}"  # noqa: RUF001


def _ewm_parameters_alpha_to_com(alpha: float, /) -> float:
    return 1 / alpha - 1


def _ewm_parameters_alpha_to_span(alpha: float, /) -> float:
    return 2 / alpha - 1


def _ewm_parameters_alpha_to_half_life(alpha: float, /) -> float:
    return -log(2) / log(1 - alpha)


##


def is_equal(
    x: float, y: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == y."""
    if isinstance(x, int) and isinstance(y, int):
        return x == y
    return _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or (isnan(x) and isnan(y))


##


def is_equal_or_approx(
    x: int | tuple[int, float],
    y: int | tuple[int, float],
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x == y, or approximately."""
    if isinstance(x, int) and isinstance(y, int):
        return is_equal(x, y, rel_tol=rel_tol, abs_tol=abs_tol)
    if isinstance(x, int) and isinstance(y, tuple):
        return isclose(x, y[0], rel_tol=y[1])
    if isinstance(x, tuple) and isinstance(y, int):
        return isclose(x[0], y, rel_tol=x[1])
    if isinstance(x, tuple) and isinstance(y, tuple):
        return isclose(x[0], y[0], rel_tol=max(x[1], y[1]))
    raise ImpossibleCaseError(case=[f"{x=}", f"{y=}"])  # pragma: no cover


##


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


##


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


##


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


##


def is_finite(x: float, /) -> bool:
    """Check if -inf < x < inf."""
    return isfinite(x)


def is_finite_or_nan(x: float, /) -> bool:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) or isnan(x)


##


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


##


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


##


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


##


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


##


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


##


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


##


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


##


def is_integral(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == int(x)."""
    if isinf(x) or isnan(x):
        return False
    frac, _ = modf(x)
    return is_zero(frac, rel_tol=rel_tol, abs_tol=abs_tol)


def is_integral_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == int(x) or x == nan."""
    return is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(x)


##


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


##


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


##


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


##


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


##


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


##


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


##


def is_zero(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x == 0."""
    return _is_close(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


def is_zero_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


##


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


##


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


##


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


##


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


@dataclass(kw_only=True, slots=True)
class NumberOfDecimalsError(Exception):
    x: float
    max_decimals: int

    @override
    def __str__(self) -> str:
        return f"Could not determine number of decimals of {self.x} (up to {self.max_decimals})"


##


@overload
def order_of_magnitude(x: float, /, *, round_: Literal[True]) -> int: ...
@overload
def order_of_magnitude(x: float, /, *, round_: bool = False) -> float: ...
def order_of_magnitude(x: float, /, *, round_: bool = False) -> float:
    """Get the order of magnitude of a number."""
    result = log10(abs(x))
    return round(result) if round_ else result


##


def parse_number(number: str, /) -> Number:
    """Convert text into a number."""
    with suppress(ValueError):
        return int(number)
    with suppress(ValueError):
        return float(number)
    raise ParseNumberError(number=number)


@dataclass(kw_only=True, slots=True)
class ParseNumberError(Exception):
    number: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse number; got {self.number!r}"


##


def round_(
    x: float,
    /,
    *,
    mode: MathRoundMode = "standard",
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> int:
    """Round a float to an integer."""
    match mode:
        case "standard":
            return round(x)
        case "floor":
            return floor(x)
        case "ceil":
            return ceil(x)
        case "toward-zero":
            return int(x)
        case "away-zero":
            match sign(x):
                case 1:
                    return ceil(x)
                case 0:
                    return 0
                case -1:
                    return floor(x)
                case _ as never:
                    assert_never(never)
        case "standard-tie-floor":
            return _round_tie_standard(x, "floor", rel_tol=rel_tol, abs_tol=abs_tol)
        case "standard-tie-ceil":
            return _round_tie_standard(x, "ceil", rel_tol=rel_tol, abs_tol=abs_tol)
        case "standard-tie-toward-zero":
            return _round_tie_standard(
                x, "toward-zero", rel_tol=rel_tol, abs_tol=abs_tol
            )
        case "standard-tie-away-zero":
            return _round_tie_standard(x, "away-zero", rel_tol=rel_tol, abs_tol=abs_tol)
        case _ as never:
            assert_never(never)


def _round_tie_standard(
    x: float,
    mode: MathRoundMode,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> int:
    """Round a float to an integer using the standard method."""
    frac, _ = modf(x)
    if _is_close(abs(frac), 0.5, rel_tol=rel_tol, abs_tol=abs_tol):
        mode_use: MathRoundMode = mode
    else:
        mode_use: MathRoundMode = "standard"
    return round_(x, mode=mode_use)


##


_ROUND_FLOAT_IMPRECISIONS_DECIMALS = 8
_ROUND_FLOAT_IMPRECISIONS_PATTERN = re.compile(r"^(-?\d+)\.(\d+)$")


def round_float_imprecisions(
    x: float, /, *, decimals: int = _ROUND_FLOAT_IMPRECISIONS_DECIMALS
) -> float:
    """Round a float, removing binary representation imprecisions."""
    try:
        head, tail = extract_groups(_ROUND_FLOAT_IMPRECISIONS_PATTERN, str(x))
    except ExtractGroupsError:
        head, tail = extract_groups(_ROUND_FLOAT_IMPRECISIONS_PATTERN, f"{x:.20f}")
    half = ceil(decimals / 2)
    pattern0 = search(rf"^([0-9]+?)(0{{{half},}})([0-9]+?)$", tail)
    pattern9 = search(rf"^(0*)([0-9]+?)(9{{{half},}})([0-9]+?)$", tail)
    match pattern0, pattern9:
        case None, None:
            return x
        case Match() as match, None:
            t0, t1, t2 = match.groups()
            if ((len(t0) + len(t1)) >= decimals) and (len(t1) > len(t2)):
                return float(f"{head}.{t0}")
            return x
        case None, Match() as match:
            return _round_float_imprecisions_pattern9(x, head, match, decimals=decimals)
        case Match() as match0, Match() as match9 if (
            match0.span(3)[0] < match9.span(4)[0]
        ):
            return _round_float_imprecisions_pattern9(
                x, head, match9, decimals=decimals
            )
        case _:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{pattern0=}", f"{pattern9=}"])


def _round_float_imprecisions_pattern9(
    x: float,
    head: str,
    match: Match[str],
    /,
    *,
    decimals: float = _ROUND_FLOAT_IMPRECISIONS_DECIMALS,
) -> float:
    t0, t1, t2, t3 = match.groups()
    if ((len(t0) + len(t1) + len(t2)) >= decimals) and (len(t2) > len(t3)):
        return float(f"{head}.{t0}{int(t1) + 1}")
    return x


##


def round_to_float(
    x: float,
    y: float,
    /,
    *,
    mode: MathRoundMode = "standard",
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> float:
    """Round a float to the nearest multiple of another float."""
    rounded = round_(x / y, mode=mode, rel_tol=rel_tol, abs_tol=abs_tol)
    return round_float_imprecisions(y * rounded)


##


def safe_round(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> int:
    """Safely round a float."""
    if is_finite_and_integral(x, rel_tol=rel_tol, abs_tol=abs_tol):
        return round(x)
    raise SafeRoundError(x=x, rel_tol=rel_tol, abs_tol=abs_tol)


@dataclass(kw_only=True, slots=True)
class SafeRoundError(Exception):
    x: float
    rel_tol: float | None = None
    abs_tol: float | None = None

    @override
    def __str__(self) -> str:
        return f"Unable to safely round {self.x} (rel_tol={self.rel_tol}, abs_tol={self.abs_tol})"


##


def sign(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> Sign:
    """Get the sign of an integer/float."""
    match x:
        case int():
            if x > 0:
                return 1
            if x < 0:
                return -1
            return 0
        case float():
            if is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol):
                return 1
            if is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol):
                return -1
            return 0
        case _ as never:
            assert_never(never)


##


def significant_figures(x: float, /, *, n: int = 2) -> str:
    """Format an integer/float to a given number of significant figures."""
    return "{:g}".format(float("{:.{p}g}".format(x, p=n)))


__all__ = [
    "MAX_FLOAT32",
    "MAX_FLOAT64",
    "MAX_INT8",
    "MAX_INT16",
    "MAX_INT32",
    "MAX_INT64",
    "MAX_UINT8",
    "MAX_UINT16",
    "MAX_UINT32",
    "MAX_UINT64",
    "MIN_FLOAT32",
    "MIN_FLOAT64",
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
    "ParseNumberError",
    "SafeRoundError",
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
    "parse_number",
    "round_",
    "round_float_imprecisions",
    "round_to_float",
    "safe_round",
    "significant_figures",
]
