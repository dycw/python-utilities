from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Annotated, Any, cast

from numpy import bool_, float64, int64, object_
from xarray import DataArray

from utilities.numpy import datetime64ns

if TYPE_CHECKING:
    from collections.abc import Hashable, Mapping

    from utilities.dataclasses import Dataclass

# annotations - dtype
DataArrayB = Annotated[DataArray, bool_]
DataArrayDns = Annotated[DataArray, datetime64ns]
DataArrayF = Annotated[DataArray, float64]
DataArrayI = Annotated[DataArray, int64]
DataArrayO = Annotated[DataArray, object_]

# annotations - ndim
DataArray0 = Annotated[DataArray, 0]
DataArray1 = Annotated[DataArray, 1]
DataArray2 = Annotated[DataArray, 2]
DataArray3 = Annotated[DataArray, 3]

# annotated; dtype & ndim
DataArrayB0 = Annotated[DataArrayB, 0]
DataArrayDns0 = Annotated[DataArrayDns, 0]
DataArrayF0 = Annotated[DataArrayF, 0]
DataArrayI0 = Annotated[DataArrayI, 0]
DataArrayO0 = Annotated[DataArrayO, 0]

DataArrayB1 = Annotated[DataArrayB, 1]
DataArrayDns1 = Annotated[DataArrayDns, 1]
DataArrayF1 = Annotated[DataArrayF, 1]
DataArrayI1 = Annotated[DataArrayI, 1]
DataArrayO1 = Annotated[DataArrayO, 1]

DataArrayB2 = Annotated[DataArrayB, 2]
DataArrayDns2 = Annotated[DataArrayDns, 2]
DataArrayF2 = Annotated[DataArrayF, 2]
DataArrayI2 = Annotated[DataArrayI, 2]
DataArrayO2 = Annotated[DataArrayO, 2]

DataArrayB3 = Annotated[DataArrayB, 3]
DataArrayDns3 = Annotated[DataArrayDns, 3]
DataArrayF3 = Annotated[DataArrayF, 3]
DataArrayI3 = Annotated[DataArrayI, 3]
DataArrayO3 = Annotated[DataArrayO, 3]


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
        _ewma_move_exp_helper,
        dim=rolling_exp.dim,
        alpha=rolling_exp.alpha,
        keep_attrs=keep_attrs,
    )


def _ewma_move_exp_helper(array: Any, /, *, axis: Any, alpha: Any) -> Any:
    from numbagg import move_exp_nanmean

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
        _exp_moving_sum_helper,
        dim=rolling_exp.dim,
        alpha=rolling_exp.alpha,
        keep_attrs=keep_attrs,
    )


def _exp_moving_sum_helper(array: Any, /, *, axis: Any, alpha: Any) -> Any:
    from numbagg import move_exp_nansum

    if axis == ():  # pragma: no cover
        return array.astype(float)
    return cast(Any, move_exp_nansum)(array, axis=axis, alpha=alpha)


def rename_data_arrays(obj: Dataclass, /) -> None:
    """Rename the arrays on a field."""
    for key, value in asdict(obj).items():
        if isinstance(value, DataArray) and (value.name != key):
            setattr(obj, key, value.rename(key))


__all__ = [
    "DataArray0",
    "DataArray1",
    "DataArray2",
    "DataArray3",
    "DataArrayB",
    "DataArrayB0",
    "DataArrayB1",
    "DataArrayB2",
    "DataArrayB3",
    "DataArrayDns",
    "DataArrayDns0",
    "DataArrayDns1",
    "DataArrayDns2",
    "DataArrayDns3",
    "DataArrayF",
    "DataArrayF0",
    "DataArrayF1",
    "DataArrayF2",
    "DataArrayF3",
    "DataArrayI",
    "DataArrayI0",
    "DataArrayI1",
    "DataArrayI2",
    "DataArrayI3",
    "DataArrayO",
    "DataArrayO0",
    "DataArrayO1",
    "DataArrayO2",
    "DataArrayO3",
    "ewma",
    "exp_moving_sum",
    "rename_data_arrays",
]
