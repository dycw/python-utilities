from __future__ import annotations

from .zarr import InvalidDimensionError
from .zarr import InvalidIndexValueError
from .zarr import IselIndexer
from .zarr import NDArrayWithIndexes
from .zarr import NoIndexesError
from .zarr import ffill_non_nan_slices
from .zarr import yield_array_with_indexes
from .zarr import yield_group_and_array

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
    from .xarray import DataArrayOnDisk
    from .xarray import NotOneDimensionalArrayError
    from .xarray import save_data_array_to_disk
    from .xarray import yield_data_array_on_disk
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "DataArrayOnDisk",
        "NotOneDimensionalArrayError",
        "save_data_array_to_disk",
        "yield_data_array_on_disk",
    ]
