from __future__ import annotations

from typing import TYPE_CHECKING

from polars import struct
from polars_ols import RollingKwargs, compute_rolling_least_squares

from utilities.polars import ensure_expr_or_series

if TYPE_CHECKING:
    from polars import Expr
    from polars_ols import NullPolicy

    from utilities.polars import ExprLike


def compute_rolling_ols(
    target: ExprLike,
    *features: ExprLike,
    sample_weights: ExprLike | None = None,
    add_intercept: bool = False,
    null_policy: NullPolicy = "drop_window",
    window_size: int = 1000000,
    min_periods: int | None = None,
    use_woodbury: bool | None = None,
    alpha: float | None = None,
) -> Expr:
    """Compute a rolling OLS."""
    target = ensure_expr_or_series(target)
    rolling_kwargs = RollingKwargs(
        null_policy=null_policy,
        window_size=window_size,
        min_periods=min_periods,
        use_woodbury=use_woodbury,
        alpha=alpha,
    )
    coefficients = compute_rolling_least_squares(
        target,
        *features,
        sample_weights=sample_weights,
        add_intercept=add_intercept,
        mode="coefficients",
        rolling_kwargs=rolling_kwargs,
    ).alias("coefficients")
    predictions = compute_rolling_least_squares(
        target,
        *features,
        sample_weights=sample_weights,
        add_intercept=add_intercept,
        mode="predictions",
        rolling_kwargs=rolling_kwargs,
    ).alias("predictions")
    residuals = compute_rolling_least_squares(
        target,
        *features,
        sample_weights=sample_weights,
        add_intercept=add_intercept,
        mode="residuals",
        rolling_kwargs=rolling_kwargs,
    ).alias("residuals")
    ssr = (residuals**2).rolling_sum(window_size, min_samples=min_periods).alias("SSR")
    sst = (
        ((target - target.rolling_mean(window_size, min_samples=min_periods)) ** 2)
        .rolling_sum(window_size, min_samples=min_periods)
        .alias("SST")
    )
    r2 = (1 - ssr / sst).alias("R2")
    return struct(coefficients, predictions, residuals, r2).alias("ols")


__all__ = ["compute_rolling_ols"]
