from __future__ import annotations

from zarr import Array

from utilities._zarr.common import (
    GetIndexByNameError,
    GetSelIndexerError,
    IselIndexer,
    NDArrayWithIndexes,
    yield_array_with_indexes,
    yield_group_and_array,
)
from utilities.numpy import _ffill_non_nan_slices_helper, array_indexer


def ffill_non_nan_slices(
    array: Array, /, *, limit: int | None = None, axis: int = -1
) -> None:
    """Forward fill the slices in an array which contain non-nan values."""
    ndim = array.ndim
    arrays = (
        array.oindex[array_indexer(i, ndim, axis=axis)]
        for i in range(array.shape[axis])
    )
    for i, repl_i in _ffill_non_nan_slices_helper(arrays, limit=limit):
        array.oindex[array_indexer(i, ndim, axis=axis)] = repl_i


__all__ = [
    "GetIndexByNameError",
    "GetSelIndexerError",
    "IselIndexer",
    "NDArrayWithIndexes",
    "ffill_non_nan_slices",
    "yield_array_with_indexes",
    "yield_group_and_array",
]


try:
    from utilities._zarr.xarray import (
        DataArrayOnDisk,
        ToNDArray1Error,
        save_data_array_to_disk,
        to_ndarray1,
        yield_data_array_on_disk,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "DataArrayOnDisk",
        "ToNDArray1Error",
        "save_data_array_to_disk",
        "to_ndarray1",
        "yield_data_array_on_disk",
    ]
