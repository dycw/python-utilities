from collections.abc import Hashable
from pathlib import Path

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, floats, integers
from numpy import array, isclose, sort
from numpy.testing import assert_equal
from zarr import full

from utilities.hypothesis import hashables, temp_paths
from utilities.hypothesis.numpy import float_arrays, int_arrays
from utilities.numpy.typing import NDArrayI1
from utilities.zarr import NDArrayWithIndexes, yield_array_with_indexes

indexes1d = int_arrays(shape=integers(0, 10), unique=True).map(sort)


class TestNDArrayWithIndexes:
    @given(
        data=data(),
        indexes=dictionaries(hashables(), indexes1d, max_size=3),
        root=temp_paths(),
    )
    def test_main(
        self, data: DataObject, indexes: dict[Hashable, NDArrayI1], root: Path
    ) -> None:
        shape = tuple(map(len, indexes.values()))
        arrays = float_arrays(shape=shape, allow_nan=True, allow_inf=True)
        if shape == ():
            arrays |= floats(allow_nan=True, allow_infinity=True).map(array)
        arr = data.draw(arrays)
        path = root.joinpath("array")
        with yield_array_with_indexes(indexes, path) as z_array:
            if shape == ():
                z_array[:] = arr.item()
                exp_size = 0
                is_scalar = True
            else:
                z_array[:] = arr
                exp_size = arr.size
                is_scalar = False
        view = NDArrayWithIndexes(path)
        assert view.dims == tuple(indexes)
        assert view.dtype == float
        assert set(view.indexes) == set(indexes)
        for dim, index in view.indexes.items():
            assert_equal(index, indexes[dim])
        assert view.is_scalar is is_scalar
        assert view.is_non_scalar is (not is_scalar)
        assert_equal(view.ndarray, arr)
        assert view.ndim == len(indexes)
        assert view.shape == shape
        assert view.size == exp_size
        assert view.sizes == dict(zip(indexes, shape))

    @given(
        indexes=dictionaries(hashables(), indexes1d, max_size=3),
        root=temp_paths(),
        fill_value=floats(allow_infinity=True, allow_nan=True),
    )
    def test_fill_value(
        self, indexes: dict[Hashable, NDArrayI1], root: Path, fill_value: float
    ) -> None:
        path = root.joinpath("array")
        with yield_array_with_indexes(indexes, path, fill_value=fill_value):
            pass
        view = NDArrayWithIndexes(path)
        assert isclose(view.ndarray, fill_value, equal_nan=True).all()

    @given(root=temp_paths(), fill_value=floats(allow_infinity=True, allow_nan=True))
    def test_empty(self, root: Path, fill_value: float) -> None:
        path = root.joinpath("array")
        with yield_array_with_indexes({}, path, fill_value=fill_value):
            pass
        view = NDArrayWithIndexes(path)
        assert view.dims == ()
        assert view.dtype == float
        assert set(view.indexes) == set()
        assert_equal(view.ndarray, full((), fill_value, dtype=float))
        assert view.ndim == 0
        assert view.shape == ()
        assert view.size == 0
        assert view.sizes == {}
