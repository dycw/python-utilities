from __future__ import annotations

from math import isclose, isfinite, isnan


def is_at_least(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if -inf < x < inf and x == int(x)."""
    return (x >= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_at_least_or_nan(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x >= y or x == nan."""
    return is_at_least(x, y, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(x)


def is_at_most(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x <= y."""
    return (x <= y) or _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_at_most_or_nan(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
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
    return is_between(x, low, high, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(
        x
    )


def is_finite_and_integral(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf and x == int(x)."""
    return isfinite(x) & is_integral(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_integral_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf and x == int(x), or x == nan."""
    return is_finite_and_integral(x, rel_tol=rel_tol, abs_tol=abs_tol) | isnan(
        x
    )


def is_finite_and_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < 0."""
    return isfinite(x) and is_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < 0 or x == nan."""
    return is_finite_and_negative(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(
        x
    )


def is_finite_and_non_negative(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 <= x < inf."""
    return isfinite(x) and is_non_negative(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_negative_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 <= x < inf or x == nan."""
    return is_finite_and_non_negative(
        x, rel_tol=rel_tol, abs_tol=abs_tol
    ) or isnan(x)


def is_finite_and_non_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x <= 0."""
    return isfinite(x) and is_non_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x <= 0 or x == nan."""
    return is_finite_and_non_positive(
        x, rel_tol=rel_tol, abs_tol=abs_tol
    ) or isnan(x)


def is_finite_and_non_zero(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if -inf < x < inf, x != 0."""
    return isfinite(x) and is_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_non_zero_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if x != 0 or x == nan."""
    return is_finite_and_non_zero(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(
        x
    )


def is_finite_and_positive(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 < x < inf."""
    return isfinite(x) and is_positive(x, rel_tol=rel_tol, abs_tol=abs_tol)


def is_finite_and_positive_or_nan(
    x: float, /, *, rel_tol: float | None = None, abs_tol: float | None = None
) -> bool:
    """Check if 0 < x < inf or x == nan."""
    return is_finite_and_positive(x, rel_tol=rel_tol, abs_tol=abs_tol) or isnan(
        x
    )


def is_finite_or_nan(x: float, /) -> bool:
    """Check if -inf < x < inf or x == nan."""
    return isfinite(x) or isnan(x)


def is_greater_than(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x > y."""
    return (x > y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_greater_than_or_nan(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
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
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x < y."""
    return (x < y) and not _is_close(x, y, rel_tol=rel_tol, abs_tol=abs_tol)


def is_less_than_or_nan(
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
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
    return (x == zero) or is_finite_and_non_zero(
        x, rel_tol=rel_tol, abs_tol=abs_tol
    )


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
    x: float,
    y: float,
    /,
    *,
    rel_tol: float | None = None,
    abs_tol: float | None = None,
) -> bool:
    """Check if x == y."""
    return isclose(
        x,
        y,
        **({} if rel_tol is None else {"rel_tol": rel_tol}),
        **({} if abs_tol is None else {"abs_tol": abs_tol}),
    )
