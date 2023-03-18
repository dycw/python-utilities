from collections.abc import Hashable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, Optional, Union, cast

from beartype import beartype
from numpy import full
from numpy.typing import NDArray
from zarr import JSON, Array, Group, group
from zarr.convenience import open_group

from utilities.atomicwrites import writer
from utilities.numpy import get_fill_value, is_empty, is_non_empty
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
    """Save an `ndarray` with indexes, yielding a view into its values.

    The dimensions must be JSON-serializable.
    """
    with writer(path, overwrite=overwrite) as temp:
        root = group(store=temp)
        root.attrs["dims"] = tuple(indexes)
        for i, index in enumerate(indexes.values()):
            _ = root.array(f"index_{i}", index, **_codec(index.dtype))
        shape = tuple(map(len, indexes.values()))
        if isinstance(fill_value, Sentinel):
            fill_value_use = get_fill_value(dtype)
        else:
            fill_value_use = fill_value
        yield root.full(
            "values",
            fill_value=fill_value_use,
            shape=shape,
            chunks=chunks,
            **_codec(dtype),
        )


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
    def dims(self) -> tuple[Hashable, ...]:
        """The dimensions of the underlying array."""
        return tuple(self.group.attrs["dims"])

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
        return {
            dim: cast(NDArray1, self.group[f"index_{i}"][:])
            for i, dim in enumerate(self.dims)
        }

    @property
    @beartype
    def is_empty(self) -> bool:
        """Whether the underlying array is empty or not."""
        return is_empty(self.shape)

    @property
    @beartype
    def is_non_empty(self) -> bool:
        """Whether the underlying array is empty or not."""
        return is_non_empty(self.shape)

    @property
    @beartype
    def ndarray(self) -> NDArray[Any]:
        """The underlying `numpy.ndarray`."""
        if self.is_empty:
            return full((), get_fill_value(self.dtype), dtype=self.dtype)
        return self.array[:]

    @property
    @beartype
    def ndim(self) -> int:
        """The number of dimensions of the underlying array."""
        return self.array.ndim

    @property
    @beartype
    def shape(self) -> tuple[int, ...]:
        """The shape of the underlying array."""
        return self.array.shape

    @property
    @beartype
    def size(self) -> int:
        """The size of the underlying array."""
        return self.array.size

    @property
    @beartype
    def sizes(self) -> dict[Hashable, int]:
        """The sizes of the underlying array."""
        return {dim: len(index) for dim, index in self.indexes.items()}
