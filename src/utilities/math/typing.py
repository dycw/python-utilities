from math import isclose, isfinite, isnan
from typing import Annotated, Optional, Union

from beartype import beartype

# checks


@beartype
def is_at_least(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < inf and x == int(x)."""
    return (x >= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_at_least_or_nan(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x >= y or x == nan."""
    return is_at_least(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_at_most(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x <= y."""
    return (x <= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_at_most_or_nan(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x <= y or x == nan."""
    return is_at_most(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_between(
    x: Union[int, float],
    low: Union[int, float],
    high: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if low <= x <= high."""
    return is_at_least(x, low, rel_tol=rel_tol, abs_tol=abs_tol) and is_at_most(
        x, high, rel_tol=rel_tol, abs_tol=abs_tol
    )


@beartype
def is_between_or_nan(
    x: Union[int, float],
    low: Union[int, float],
    high: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if low <= x <= high or x == nan."""
    return is_between(x, low, high, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_and_integral(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < inf and x == int(x)."""
    return isfinite(x) & is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_integral_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < inf and x == int(x), or x == nan."""
    return is_finite_and_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(x)


@beartype
def is_finite_and_negative(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < 0."""
    return isfinite(x) and is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_negative_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < 0 or x == nan."""
    return is_finite_and_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_and_non_negative(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if 0 <= x < inf."""
    return isfinite(x) and is_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_non_negative_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if 0 <= x < inf or x == nan."""
    return is_finite_and_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_and_non_positive(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x <= 0."""
    return isfinite(x) and is_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_non_positive_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x <= 0 or x == nan."""
    return is_finite_and_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_and_non_zero(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if -inf < x < inf, x != 0."""
    return isfinite(x) and is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_non_zero_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x != 0 or x == nan."""
    return is_finite_and_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_and_positive(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if 0 < x < inf."""
    return isfinite(x) and is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_finite_and_positive_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if 0 < x < inf or x == nan."""
    return is_finite_and_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_finite_or_nan(x: Union[int, float], /) -> bool:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) or isnan(x)


@beartype
def is_greater_than(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x > y."""
    return (x > y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_greater_than_or_nan(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x > y or x == nan."""
    return is_greater_than(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_integral(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == int(x)."""
    try:
        rounded = round(x)
    except (OverflowError, ValueError):
        rounded = x
    return _is_close(x, rounded, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_integral_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == int(x) or x == nan."""
    return is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(x)


@beartype
def is_less_than(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x < y."""
    return (x < y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_less_than_or_nan(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x < y or x == nan."""
    return is_less_than(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_negative(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x < 0."""
    return is_less_than(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_negative_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x < 0 or x == nan."""
    return is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_non_negative(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x >= 0."""
    return is_at_least(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_non_negative_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x >= 0 or x == nan."""
    return is_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_non_positive(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x <= 0."""
    return is_at_most(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_non_positive_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x <=0 or x == nan."""
    return is_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_non_zero(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x != 0."""
    return not _is_close(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_non_zero_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x != 0 or x == nan."""
    return is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_positive(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x > 0."""
    return is_greater_than(x, 0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_positive_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x > 0 or x == nan."""
    return is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_zero(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == 0."""
    return _is_close(x, 0.0, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_zero_or_finite_and_non_micro(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) or is_finite_and_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_zero_or_finite_and_non_micro_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == 0, or -inf < x < inf and ~isclose(x, 0), or x == nan."""
    return is_zero_or_finite_and_non_micro(
        x, rel_tol=rel_tol, abs_tol=abs_tol
    ) or isnan(x)


@beartype
def is_zero_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x > 0 or x == nan."""
    return is_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def is_zero_or_non_micro(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == 0 or ~isclose(x, 0)."""
    zero = 0.0
    return (x == zero) or is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


@beartype
def is_zero_or_non_micro_or_nan(
    x: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == 0 or ~isclose(x, 0) or x == nan."""
    return is_zero_or_non_micro(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


@beartype
def _is_close(
    x: Union[int, float],
    y: Union[int, float],
    /,
    *,
    rel_tol: Optional[float] = None,
    abs_tol: Optional[float] = None,
) -> bool:
    """Check if x == y."""
    return isclose(
        x,
        y,
        **({} if rel_tol is None else {"rel_tol": rel_tol}),
        **({} if abs_tol is None else {"abs_tol": abs_tol}),
    )


# annotated; int & checks
IntNeg = Annotated[int, is_negative]
IntNonNeg = Annotated[int, is_non_negative]
IntNonPos = Annotated[int, is_non_positive]
IntNonZr = Annotated[int, is_non_zero]
IntPos = Annotated[int, is_positive]
IntZr = Annotated[int, is_zero]

# annotated; float & checks
FloatFin = Annotated[float, isfinite]
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
