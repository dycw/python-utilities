from collections.abc import Hashable
from pathlib import Path

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, integers
from numpy import nan, sort
from numpy.testing import assert_equal
from zarr import Array, full

from utilities.hypothesis import hashables, temp_paths
from utilities.hypothesis.numpy import float_arrays, int_arrays
from utilities.numpy.typing import NDArrayI1, is_zero
from utilities.zarr import NDArrayWithIndexes, yield_array_with_indexes

indexes1d = int_arrays(shape=integers(0, 10), unique=True).map(sort)


class TestNDArrayWithIndexes:
    @given(data=data(), dim=hashables(), index=indexes1d, root=temp_paths())
    def test_main(
        self, data: DataObject, dim: Hashable, index: NDArrayI1, root: Path
    ) -> None:
        array = data.draw(float_arrays(shape=len(index), allow_nan=True))
        path = root.joinpath("array")
        with yield_array_with_indexes({dim: index}, path) as z_array:
            z_array[:] = array
        view = NDArrayWithIndexes(path)
        assert isinstance(view.array, Array)
        assert view.dims == (dim,)
        assert view.dtype == float
        assert set(view.indexes) == {dim}
        assert_equal(view.indexes[dim], index)
        assert_equal(view.ndarray, array)
        assert view.ndim == 1
        assert view.shape == (len(index),)
        assert view.size == len(index)
        assert view.sizes == {dim: len(index)}

    @given(indexes=dictionaries(hashables(), indexes1d, max_size=3), root=temp_paths())
    def test_fill_value(self, indexes: dict[Hashable, NDArrayI1], root: Path) -> None:
        path = root.joinpath("array")
        with yield_array_with_indexes(indexes, path, fill_value=0.0):
            pass
        view = NDArrayWithIndexes(path)
        if view.is_non_empty:
            assert is_zero(view.ndarray).all()
        assert view.sizes == {dim: len(index) for dim, index in indexes.items()}

    @given(root=temp_paths())
    def test_empty(self, root: Path) -> None:
        path = root.joinpath("array")
        with yield_array_with_indexes({}, path, fill_value=0.0):
            pass
        view = NDArrayWithIndexes(path)
        assert view.dims == ()
        assert view.dtype == float
        assert set(view.indexes) == set()
        assert_equal(view.ndarray, full((), nan, dtype=float))
        assert view.ndim == 0
        assert view.shape == ()
        assert view.size == 1
        assert view.sizes == {}
