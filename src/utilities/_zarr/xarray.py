from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from numpy import empty, ndarray
from pandas import Index
from typing_extensions import override
from xarray import DataArray
from xarray.core.types import ErrorOptionsWithWarn
from zarr import Array, suppress

from utilities._zarr.common import (
    GetIndexByNameError,
    IselIndexer,
    NDArrayWithIndexes,
    yield_group_and_array,
)
from utilities.numpy import NDArray1
from utilities.sentinel import sentinel
from utilities.text import ensure_str
from utilities.types import PathLike

if TYPE_CHECKING:  # pragma: no cover
    from utilities.pandas import IndexA


def save_data_array_to_disk(
    array: DataArray,
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    chunks: bool | int | tuple[int | None, ...] = True,
) -> None:
    """Save a `DataArray` to disk."""
    name_use = None if (name := array.name) is None else ensure_str(name)
    with yield_data_array_on_disk(
        cast(Mapping[str, Any], array.coords),
        path,
        overwrite=overwrite,
        dtype=array.dtype,
        chunks=chunks,
        name=name_use,
    ) as z_array:
        if (values := array.to_numpy()).shape == ():
            z_array[:] = values.item()
        else:
            z_array[:] = values


@contextmanager
def yield_data_array_on_disk(
    coords: Mapping[str, Any],
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    dtype: Any = float,
    fill_value: Any = sentinel,
    chunks: bool | int | tuple[int | None, ...] = True,
    name: str | None = None,
) -> Iterator[Array]:
    """Save a `DataArray`, yielding a view into its values."""
    indexes: dict[str, NDArray1] = {}
    for coord, value in coords.items():
        with suppress(ToNDArray1Error):
            indexes[coord] = to_ndarray1(value)
    with yield_group_and_array(
        indexes,
        path,
        overwrite=overwrite,
        dtype=dtype,
        fill_value=fill_value,
        chunks=chunks,
    ) as (root, array):
        root.attrs["coords"] = tuple(coords)
        for coord, value in coords.items():
            if coord not in indexes:
                root.attrs[f"coord_{coord}"] = value.item()
        root.attrs["name"] = name
        yield array


def to_ndarray1(x: Any, /) -> NDArray1:
    """Convert a coordinate into a 1-dimensional array."""
    if isinstance(x, ndarray):
        if x.ndim == 1:
            return x
        msg = f"{x=}"
        raise ToNDArray1Error(msg)
    if isinstance(x, DataArray | Index):
        if x.ndim == 1:
            return x.to_numpy()
        msg = f"{x=}"
        raise ToNDArray1Error(msg)
    msg = f"{x=}"
    raise ToNDArray1Error(msg)


class ToNDArray1Error(Exception):
    ...


class DataArrayOnDisk(NDArrayWithIndexes):
    """A `DataArray` stored on disk."""

    @property
    def coords(self) -> dict[str, Any]:
        """The coordinates of the underlying array."""
        return {coord: self._get_coord(coord) for coord in self.attrs["coords"]}

    @property
    def da(self) -> DataArray:
        """Alias for `data_array`."""
        return self.data_array

    @property
    def data_array(self) -> DataArray:
        """The underlying `DataArray`."""
        return DataArray(self.ndarray, self.coords, self.dims, self.name)

    @property
    @override
    def indexes(self) -> dict[str, IndexA]:  # type: ignore
        """The indexes of the underlying array."""
        return {ensure_str(dim): Index(index) for dim, index in super().indexes.items()}

    @override
    def isel(
        self,
        indexers: Mapping[str, IselIndexer] | None = None,
        /,
        *,
        drop: bool = False,
        missing_dims: ErrorOptionsWithWarn = "raise",
        **indexer_kwargs: IselIndexer,
    ) -> DataArray:
        """Select orthogonally using integer indexes."""
        empty = self._empty.isel(
            indexers, drop=drop, missing_dims=missing_dims, **indexer_kwargs
        )
        return DataArray(
            super().isel(indexers, **indexer_kwargs),
            empty.coords,
            empty.dims,
            self.name,
        )

    @property
    def name(self) -> str | None:
        """The name of the underlying array."""
        return self.attrs["name"]

    @override
    def sel(
        self,
        indexers: Mapping[str, Any] | None = None,
        /,
        *,
        method: str | None = None,
        tolerance: Any = None,
        drop: bool = False,
        **indexer_kwargs: Any,
    ) -> DataArray:
        """Select orthogonally using index values."""
        empty = self._empty.sel(
            indexers, method=method, tolerance=tolerance, drop=drop, **indexer_kwargs
        )
        return DataArray(
            super().sel(indexers, **indexer_kwargs), empty.coords, empty.dims, self.name
        )

    @property
    def _empty(self, /) -> DataArray:
        """An empty DataArray, for slicing."""
        return DataArray(
            empty(self.shape, dtype=bool), self.coords, self.dims, self.name
        )

    def _get_coord(self, coord: str, /) -> Any:
        """Get a coordinate by name."""
        try:
            return self._get_index_by_name(coord)
        except GetIndexByNameError:
            return self.attrs[f"coord_{coord}"]


__all__ = [
    "DataArrayOnDisk",
    "ToNDArray1Error",
    "save_data_array_to_disk",
    "to_ndarray1",
    "yield_data_array_on_disk",
]
