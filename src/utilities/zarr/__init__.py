from collections.abc import Hashable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Optional, Union, cast

from beartype import beartype
from numpy import array, prod
from numpy.typing import NDArray
from zarr import JSON, Array, Group, group
from zarr.convenience import open_group
from zarr.core import Attributes

from utilities.atomicwrites import writer
from utilities.numpy import get_fill_value
from utilities.numpy.typing import NDArray1
from utilities.pathlib import PathLike
from utilities.sentinel import Sentinel, sentinel


@contextmanager
@beartype
def yield_array_with_indexes(
    indexes: Mapping[Hashable, NDArray1],
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    dtype: Any = float,
    fill_value: Any = sentinel,
    chunks: Union[bool, int, tuple[Optional[int], ...]] = True,
) -> Iterator[Array]:
    """Save an `ndarray` with indexes, yielding a view into its values."""
    with yield_group_and_array(
        indexes,
        path,
        overwrite=overwrite,
        dtype=dtype,
        fill_value=fill_value,
        chunks=chunks,
    ) as (_, array):
        yield array


@contextmanager
@beartype
def yield_group_and_array(
    indexes: Mapping[Hashable, NDArray1],
    path: PathLike,
    /,
    *,
    overwrite: bool = False,
    dtype: Any = float,
    fill_value: Any = sentinel,
    chunks: Union[bool, int, tuple[Optional[int], ...]] = True,
) -> Iterator[tuple[Group, Array]]:
    """Core context manager for the group and array.

    The dimensions must be JSON-serializable.
    """
    with writer(path, overwrite=overwrite) as temp:
        root = group(store=temp)
        root.attrs["dims"] = tuple(indexes)
        if isinstance(fill_value, Sentinel):
            fill_value_use = get_fill_value(dtype)
        else:
            fill_value_use = fill_value
        for i, index in enumerate(indexes.values()):
            _ = root.array(f"index_{i}", index, **_codec(index.dtype))
        shape = tuple(map(len, indexes.values()))
        shape_use = (1,) if shape == () else shape
        root.attrs["shape"] = shape
        array = root.full(
            "values",
            fill_value=fill_value_use,
            shape=shape_use,
            chunks=chunks,
            **_codec(dtype),
        )
        yield root, array


class NoIndexesError(ValueError):
    """Raised when there are no indexes."""


@beartype
def _codec(dtype: Any, /) -> dict[str, Any]:
    """Generate the object codec if necesary."""
    return {"object_codec": JSON()} if dtype == object else {}


class NDArrayWithIndexes:
    """An `ndarray` with indexes stored on disk."""

    @beartype
    def __init__(
        self, path: PathLike, /, *, mode: Literal["r", "r+", "a", "w", "w-"] = "a"
    ) -> None:
        super().__init__()
        self._path = Path(path)
        self._mode = mode

    @property
    @beartype
    def array(self) -> Array:
        """The underlying `zarr.Array`."""
        return cast(Array, self.group["values"])

    @property
    @beartype
    def attrs(self) -> Attributes:
        """The underlying attributes."""
        return self.group.attrs

    @property
    @beartype
    def dims(self) -> tuple[Hashable, ...]:
        """The dimensions of the underlying array."""
        return tuple(self.attrs["dims"])

    @property
    @beartype
    def dtype(self) -> Any:
        """The type of the underlying array."""
        return self.array.dtype

    @property
    @beartype
    def group(self) -> Group:
        """The dimensions of the underlying array."""
        return open_group(self._path, mode=self._mode)

    @property
    @beartype
    def indexes(self) -> dict[Hashable, NDArray1]:
        """The indexes of the underlying array."""
        return {dim: self._get_index_by_int(i) for i, dim in enumerate(self.dims)}

    @property
    @beartype
    def is_scalar(self) -> bool:
        """Whether the underlying array is scalar or not."""
        return self.shape == ()

    @property
    @beartype
    def is_non_scalar(self) -> bool:
        """Whether the underlying array is empty or not."""
        return self.shape != ()

    @property
    @beartype
    def ndarray(self) -> NDArray[Any]:
        """The underlying `numpy.ndarray`."""
        arr = self.array[:]
        if self.is_scalar:
            return array(arr.item(), dtype=arr.dtype)
        return arr

    @property
    @beartype
    def ndim(self) -> int:
        """The number of dimensions of the underlying array."""
        return len(self.shape)

    @property
    @beartype
    def shape(self) -> tuple[int, ...]:
        """The shape of the underlying array."""
        return tuple(self.attrs["shape"])

    @property
    @beartype
    def size(self) -> int:
        """The size of the underlying array."""
        return 0 if self.is_scalar else int(prod(self.shape).item())

    @property
    @beartype
    def sizes(self) -> dict[Hashable, int]:
        """The sizes of the underlying array."""
        return {dim: len(index) for dim, index in self.indexes.items()}

    @beartype
    def _get_index_by_int(self, i: int, /) -> NDArray1:
        """Get the index of a given dimension, by its integer index."""
        return cast(NDArray1, self.group[f"index_{i}"][:])

    @beartype
    def _get_index_by_name(self, dim: Hashable, /) -> NDArray1:
        """Get the index of a given dimension, by its dimension name."""
        try:
            i = self.dims.index(dim)
        except ValueError:
            msg = f"{dim=}"
            raise InvalidDimensionError(msg) from None
        return self._get_index_by_int(i)


class InvalidDimensionError(ValueError):
    """Raised when an dimension is invalid."""
