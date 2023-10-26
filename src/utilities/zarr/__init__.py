from __future__ import annotations

from utilities.zarr.zarr import (
    InvalidDimensionError,
    InvalidIndexValueError,
    IselIndexer,
    NDArrayWithIndexes,
    NoIndexesError,
    ffill_non_nan_slices,
    yield_array_with_indexes,
    yield_group_and_array,
)

__all__ = [
    "ffill_non_nan_slices",
    "InvalidDimensionError",
    "IselIndexer",
    "InvalidIndexValueError",
    "NDArrayWithIndexes",
    "NoIndexesError",
    "yield_array_with_indexes",
    "yield_group_and_array",
]


try:
    from utilities.zarr.xarray import (
        DataArrayOnDisk,
        NotOneDimensionalArrayError,
        save_data_array_to_disk,
        yield_data_array_on_disk,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "DataArrayOnDisk",
        "NotOneDimensionalArrayError",
        "save_data_array_to_disk",
        "yield_data_array_on_disk",
    ]
