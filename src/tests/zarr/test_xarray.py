from collections.abc import Hashable
from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, integers
from numpy import nan
from numpy.testing import assert_equal
from pandas import Index
from pandas.testing import assert_index_equal
from pytest import mark, param, raises
from xarray import DataArray
from xarray.testing import assert_identical

from utilities.hypothesis import hashables, temp_paths, text_ascii
from utilities.hypothesis.numpy import float_arrays
from utilities.hypothesis.pandas import int_indexes
from utilities.hypothesis.xarray import int_data_arrays
from utilities.xarray.typing import DataArray1
from utilities.zarr.xarray import (
    DataArrayOnDisk,
    NotOneDimensionalDataArrayError,
    _to_ndarray1,
    save_data_array_to_disk,
)


class TestDataArrayOnDisk:
    @given(
        data=data(),
        coords=dictionaries(text_ascii(), integers() | int_indexes(), max_size=3),
        name=hashables(),
        root=temp_paths(),
    )
    def test_main(
        self, data: DataObject, coords: dict[str, Any], name: Hashable, root: Path
    ) -> None:
        indexes = {k: v for k, v in coords.items() if isinstance(v, Index)}
        shape = tuple(map(len, indexes.values()))
        values = data.draw(float_arrays(shape=shape, allow_nan=True, allow_inf=True))
        dims = list(indexes)
        array = DataArray(values, coords, dims, name)
        save_data_array_to_disk(array, path := root.joinpath("array"))
        view = DataArrayOnDisk(path)
        assert_identical(view.data_array, array)
        assert_identical(view.da, view.data_array)
        assert set(view.indexes) == set(indexes)
        for dim, index in view.indexes.items():
            assert_index_equal(index, indexes[dim])


class TestToNDArray1:
    @given(array=int_data_arrays(x=int_indexes()))
    def test_main(self, array: DataArray1) -> None:
        assert_equal(_to_ndarray1(array), array.to_numpy())

    @mark.parametrize(
        "array",
        [
            param(None),
            param(DataArray()),
            param(DataArray(nan, {"x": 0, "y": 0}, [])),
            param(DataArray(nan, {"x": [0], "y": [0]}, ["x", "y"])),
        ],
    )
    def test_error(self, array: DataArray1) -> None:
        with raises(NotOneDimensionalDataArrayError):
            _ = _to_ndarray1(array)
