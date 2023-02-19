from collections.abc import Hashable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Optional, Union, cast

from beartype import beartype
from numpy import empty, ndarray
from pandas import Index
from xarray import DataArray
from zarr import Array, suppress

from utilities.numpy.typing import NDArray1
from utilities.pathlib import PathLike
from utilities.sentinel import sentinel
from utilities.text import ensure_str
from utilities.zarr import (
    InvalidDimensionError,
    IselIndexer,
    NDArrayWithIndexes,
    yield_group_and_array,
)


@beartype
def save_data_array_to_disk(
    array: DataArray,
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    chunks: Union[bool, int, tuple[Optional[int], ...]] = True,
) -> None:
    """Save a `DataArray` to disk."""
    with yield_data_array_on_disk(
        cast(Mapping[str, Any], array.coords),
        path,
        overwrite=overwrite,
        dtype=array.dtype,
        chunks=chunks,
        name=array.name,
    ) as z_array:
        if (values := array.to_numpy()).shape == ():
            z_array[:] = values.item()
        else:
            z_array[:] = values


@contextmanager
@beartype
def yield_data_array_on_disk(
    coords: Mapping[str, Any],
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    dtype: Any = float,
    fill_value: Any = sentinel,
    chunks: Union[bool, int, tuple[Optional[int], ...]] = True,
    name: Hashable = None,
) -> Iterator[Array]:
    """Save a `DataArray`, yielding a view into its values."""
    indexes: dict[Hashable, NDArray1] = {}
    for coord, value in coords.items():
        with suppress(NotOneDimensionalArrayError):
            indexes[coord] = _to_ndarray1(value)
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


@beartype
def _to_ndarray1(x: Any, /) -> NDArray1:
    """Convert a coordinate into a 1-dimensional array."""
    if isinstance(x, ndarray):
        if x.ndim == 1:
            return x
        msg = f"{x=}"
        raise NotOneDimensionalArrayError(msg)
    if isinstance(x, (DataArray, Index)):
        if x.ndim == 1:
            return x.to_numpy()
        msg = f"{x=}"
        raise NotOneDimensionalArrayError(msg)
    msg = f"{x=}"
    raise NotOneDimensionalArrayError(msg)


class NotOneDimensionalArrayError(ValueError):
    """Raised when an object is not a 1-dimensional array."""


class DataArrayOnDisk(NDArrayWithIndexes):
    """A `DataArray` stored on disk."""

    @property
    @beartype
    def coords(self) -> dict[Hashable, Any]:
        """The coordinates of the underlying array."""
        return {coord: self._get_coord(coord) for coord in self.attrs["coords"]}

    @property
    @beartype
    def da(self) -> DataArray:
        """Alias for `data_array`."""
        return self.data_array

    @property
    @beartype
    def data_array(self) -> DataArray:
        """The underlying `DataArray`."""
        return DataArray(self.ndarray, self.coords, self.dims, self.name)

    @property
    @beartype
    def indexes(self) -> dict[str, Index]:
        """The indexes of the underlying array."""
        return {ensure_str(dim): Index(index) for dim, index in super().indexes.items()}

    @beartype
    def isel(
        self,
        indexers: Optional[Mapping[Hashable, IselIndexer]] = None,
        /,
        **indexer_kwargs: IselIndexer,
    ) -> DataArray:
        """Select orthogonally using integer indexes."""
        empty = self._empty.isel(indexers, **indexer_kwargs)
        return DataArray(
            super().isel(indexers, **indexer_kwargs),
            empty.coords,
            empty.dims,
            self.name,
        )

    @property
    @beartype
    def name(self) -> Hashable:
        """The name of the underlying array."""
        return self.attrs["name"]

    @beartype
    def sel(
        self,
        indexers: Optional[Mapping[Hashable, Any]] = None,
        /,
        **indexer_kwargs: Any,
    ) -> DataArray:
        """Select orthogonally using index values."""
        empty = self._empty.sel(indexers, **indexer_kwargs)
        return DataArray(
            super().sel(indexers, **indexer_kwargs), empty.coords, empty.dims, self.name
        )

    @property
    @beartype
    def _empty(self, /) -> DataArray:
        """An empty DataArray, for slicing."""
        return DataArray(
            empty(self.shape, dtype=bool), self.coords, self.dims, self.name
        )

    @beartype
    def _get_coord(self, coord: str, /) -> Any:
        """Get a coordinate by name."""
        try:
            return self._get_index_by_name(coord)
        except InvalidDimensionError:
            return self.attrs[f"coord_{coord}"]
