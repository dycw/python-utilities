from __future__ import annotations

from typing import TYPE_CHECKING, overload

from polars import Expr, Series, struct
from polars_ols import RollingKwargs, compute_rolling_least_squares

from utilities.errors import ImpossibleCaseError
from utilities.functions import is_sequence_of
from utilities.polars import concat_series, ensure_expr_or_series

if TYPE_CHECKING:
    from polars._typing import IntoExprColumn
    from polars_ols import NullPolicy

    from utilities.polars import ExprLike


@overload
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
) -> Expr: ...
@overload
def compute_rolling_ols(
    target: Series,
    *features: Series,
    sample_weights: Series | None = None,
    add_intercept: bool = False,
    null_policy: NullPolicy = "drop_window",
    window_size: int = 1000000,
    min_periods: int | None = None,
    use_woodbury: bool | None = None,
    alpha: float | None = None,
) -> Series: ...
@overload
def compute_rolling_ols(
    target: IntoExprColumn,
    *features: IntoExprColumn,
    sample_weights: IntoExprColumn | None = None,
    add_intercept: bool = False,
    null_policy: NullPolicy = "drop_window",
    window_size: int = 1000000,
    min_periods: int | None = None,
    use_woodbury: bool | None = None,
    alpha: float | None = None,
) -> Expr | Series: ...
def compute_rolling_ols(
    target: IntoExprColumn,
    *features: IntoExprColumn,
    sample_weights: IntoExprColumn | None = None,
    add_intercept: bool = False,
    null_policy: NullPolicy = "drop_window",
    window_size: int = 1000000,
    min_periods: int | None = None,
    use_woodbury: bool | None = None,
    alpha: float | None = None,
) -> Expr | Series:
    """Compute a rolling OLS."""
    target = ensure_expr_or_series(target)
    features2 = tuple(map(ensure_expr_or_series, features))
    sample_weights = (
        None if sample_weights is None else ensure_expr_or_series(sample_weights)
    )
    if (
        isinstance(target, Expr)
        and is_sequence_of(features2, Expr)
        and ((sample_weights is None) or isinstance(sample_weights, Expr))
    ):
        return _compute_rolling_ols_expr(
            target,
            *features2,
            sample_weights=sample_weights,
            add_intercept=add_intercept,
            null_policy=null_policy,
            window_size=window_size,
            min_periods=min_periods,
            use_woodbury=use_woodbury,
            alpha=alpha,
        )
    if (
        isinstance(target, Series)
        and is_sequence_of(features2, Series)
        and ((sample_weights is None) or isinstance(sample_weights, Series))
    ):
        return concat_series(
            target, *features2, *([] if sample_weights is None else [sample_weights])
        ).with_columns(
            _compute_rolling_ols_expr(
                target.name,
                *(f.name for f in features2),
                sample_weights=None if sample_weights is None else sample_weights.name,
                add_intercept=add_intercept,
                null_policy=null_policy,
                window_size=window_size,
                min_periods=min_periods,
                use_woodbury=use_woodbury,
                alpha=alpha,
            )
        )["ols"]
    raise ImpossibleCaseError(  # pragma: no cover
        case=[f"{target=}", f"{features2=}", f"{sample_weights=}"]
    )


def _compute_rolling_ols_expr(
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
    features2 = tuple(map(ensure_expr_or_series, features))
    sample_weights = (
        None if sample_weights is None else ensure_expr_or_series(sample_weights)
    )
    rolling_kwargs = RollingKwargs(
        null_policy=null_policy,
        window_size=window_size,
        min_periods=min_periods,
        use_woodbury=use_woodbury,
        alpha=alpha,
    )
    coefficients = compute_rolling_least_squares(
        target,
        *features2,
        sample_weights=sample_weights,
        add_intercept=add_intercept,
        mode="coefficients",
        rolling_kwargs=rolling_kwargs,
    ).alias("coefficients")
    predictions = compute_rolling_least_squares(
        target,
        *features2,
        sample_weights=sample_weights,
        add_intercept=add_intercept,
        mode="predictions",
        rolling_kwargs=rolling_kwargs,
    ).alias("predictions")
    residuals = compute_rolling_least_squares(
        target,
        *features2,
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
