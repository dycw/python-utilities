from __future__ import annotations

from collections.abc import Hashable, Mapping
from typing import Any, cast

from numbagg import move_exp_nanmean, move_exp_nansum

from utilities._xarray.common import DataArrayF, DataArrayI


def ewma(
    array: DataArrayI | DataArrayF,
    halflife: Mapping[Hashable, int] | None = None,
    /,
    *,
    keep_attrs: bool | None = None,
    **halflife_kwargs: int,
) -> DataArrayF:
    """Compute the EWMA of an array."""
    rolling_exp = array.rolling_exp(halflife, window_type="halflife", **halflife_kwargs)
    return array.reduce(
        _move_exp_nanmean,
        dim=rolling_exp.dim,
        alpha=rolling_exp.alpha,
        keep_attrs=keep_attrs,
    )


def _move_exp_nanmean(array: Any, /, *, axis: Any, alpha: Any) -> Any:
    if axis == ():  # pragma: no cover
        return array.astype(float)
    return cast(Any, move_exp_nanmean)(array, axis=axis, alpha=alpha)


def exp_moving_sum(
    array: DataArrayI | DataArrayF,
    halflife: Mapping[Hashable, int] | None = None,
    /,
    *,
    keep_attrs: bool | None = None,
    **halflife_kwargs: int,
) -> DataArrayF:
    """Compute the exponentially-weighted moving sum of an array."""
    rolling_exp = array.rolling_exp(halflife, window_type="halflife", **halflife_kwargs)
    return array.reduce(
        _move_exp_nansum,
        dim=rolling_exp.dim,
        alpha=rolling_exp.alpha,
        keep_attrs=keep_attrs,
    )


def _move_exp_nansum(array: Any, /, *, axis: Any, alpha: Any) -> Any:
    if axis == ():  # pragma: no cover
        return array.astype(float)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


__all__ = ["ewma", "exp_moving_sum"]
