from collections.abc import Callable, Hashable
from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, integers
from numpy import arange, array, zeros
from numpy.testing import assert_equal
from pandas import Index, RangeIndex
from pandas.testing import assert_index_equal
from pytest import mark, param, raises
from xarray import DataArray
from xarray.testing import assert_identical

from utilities.class_name import get_class_name
from utilities.hypothesis import hashables, temp_paths, text_ascii
from utilities.hypothesis.numpy import float_arrays, int_arrays
from utilities.hypothesis.pandas import int_indexes
from utilities.numpy.typing import NDArrayI1
from utilities.warnings import suppress_warnings
from utilities.xarray.typing import DataArray1
from utilities.zarr.xarray import (
    DataArrayOnDisk,
    NotOneDimensionalArrayError,
    _to_ndarray1,
    save_data_array_to_disk,
    yield_data_array_on_disk,
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

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param({"x": 0}, DataArray([0, 1, 2], {"x": 0, "y": arange(3)}, ["y"])),
            param({"x": -1}, DataArray([3, 4, 5], {"x": 1, "y": arange(3)}, ["y"])),
            param(
                {"x": slice(None, 1)},
                DataArray([[0, 1, 2]], {"x": [0], "y": arange(3)}, ["x", "y"]),
            ),
            param(
                {"x": []},
                DataArray(
                    zeros((0, 3), dtype=int), {"x": [], "y": arange(3)}, ["x", "y"]
                ),
            ),
            param(
                {"x": array([True, False])},
                DataArray([[0, 1, 2]], {"x": [0], "y": arange(3)}, ["x", "y"]),
            ),
            param(
                {"x": array([0])},
                DataArray([[0, 1, 2]], {"x": [0], "y": arange(3)}, ["x", "y"]),
            ),
            param({"x": 0, "y": 0}, DataArray(0, {"x": 0, "y": 0}, [])),
            param({"x": 0, "y": -1}, DataArray(2, {"x": 0, "y": 2}, [])),
            param(
                {"x": 0, "y": slice(None, 1)}, DataArray([0], {"x": 0, "y": [0]}, ["y"])
            ),
            param({"x": 0, "y": []}, DataArray([], {"x": 0, "y": []}, ["y"])),
            param(
                {"x": 0, "y": array([True, False, False])},
                DataArray([0], {"x": 0, "y": [0]}, ["y"]),
            ),
            param({"x": 0, "y": array([0])}, DataArray([0], {"x": 0, "y": [0]}, ["y"])),
        ],
    )
    def test_isel(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        array = DataArray(
            arange(6, dtype=int).reshape(2, 3),
            {"x": arange(2), "y": arange(3)},
            ["x", "y"],
        )
        path = tmp_path.joinpath("array")
        save_data_array_to_disk(array, path := tmp_path.joinpath("array"))
        view = DataArrayOnDisk(path)
        assert_identical(view.isel(indexer), expected)

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param(
                {"x": "x0"},
                DataArray([0, 1, 2], {"x": "x0", "y": ["y0", "y1", "y2"]}, ["y"]),
            ),
            param(
                {"x": []},
                DataArray(
                    zeros((0, 3), dtype=int),
                    {"x": [], "y": ["y0", "y1", "y2"]},
                    ["x", "y"],
                ),
            ),
            param(
                {"x": ["x0"]},
                DataArray(
                    [[0, 1, 2]], {"x": ["x0"], "y": ["y0", "y1", "y2"]}, ["x", "y"]
                ),
            ),
            param(
                {"x": ["x0", "x1"]},
                DataArray(
                    [[0, 1, 2], [3, 4, 5]],
                    {"x": ["x0", "x1"], "y": ["y0", "y1", "y2"]},
                    ["x", "y"],
                ),
            ),
            param({"x": "x0", "y": "y0"}, DataArray(0, {"x": "x0", "y": "y0"}, [])),
            param(
                {"x": "x0", "y": []},
                DataArray(zeros(0, dtype=int), {"x": "x0", "y": []}, ["y"]),
            ),
            param(
                {"x": "x0", "y": ["y0"]},
                DataArray([0], {"x": "x0", "y": ["y0"]}, ["y"]),
            ),
            param(
                {"x": "x0", "y": ["y0", "y1"]},
                DataArray([0, 1], {"x": "x0", "y": ["y0", "y1"]}, ["y"]),
            ),
        ],
    )
    def test_sel(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        array = DataArray(
            arange(6, dtype=int).reshape(2, 3),
            {"x": ["x0", "x1"], "y": ["y0", "y1", "y2"]},
            ["x", "y"],
        )
        path = tmp_path.joinpath("array")
        save_data_array_to_disk(array, path := tmp_path.joinpath("array"))
        view = DataArrayOnDisk(path)
        with suppress_warnings(category=FutureWarning):  # empty arrays trigger
            assert_identical(view.sel(indexer), expected)

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr_and_str(self, func: Callable[[Any], str], tmp_path: Path) -> None:
        view = DataArrayOnDisk(tmp_path)
        cls = get_class_name(DataArrayOnDisk)
        path = func(tmp_path.as_posix())
        assert func(view) == f"{cls}({path})"


class TestYieldDataArrayOnDisk:
    @mark.parametrize("length", [param(0), param(1), param(2)])
    def test_main(self, tmp_path: Path, length: int) -> None:
        coords = {"x": RangeIndex(length)}
        with yield_data_array_on_disk(coords, tmp_path.joinpath("array")):
            pass


class TestToNDArray1:
    @given(array=int_arrays(shape=integers(0, 10)))
    def test_array(self, array: NDArrayI1) -> None:
        assert_equal(_to_ndarray1(array), array)

    @given(array=int_arrays(shape=integers(0, 10)))
    def test_data_array(self, array: NDArrayI1) -> None:
        assert_equal(_to_ndarray1(DataArray(array)), array)

    @given(array=int_arrays(shape=integers(0, 10)))
    def test_index(self, array: NDArrayI1) -> None:
        assert_equal(_to_ndarray1(Index(array)), array)

    @mark.parametrize("array", [param(None), param(zeros(())), param(zeros((1, 1)))])
    def test_error(self, array: DataArray1) -> None:
        with raises(NotOneDimensionalArrayError):
            _ = _to_ndarray1(array)
