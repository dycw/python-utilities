import datetime as dt
from collections.abc import Hashable
from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject, data, dictionaries, floats, integers
from numpy import arange, array, isclose, sort, zeros
from numpy.testing import assert_equal
from pytest import mark, param, raises
from zarr.errors import BoundsCheckError

from utilities.hypothesis import hashables, temp_paths
from utilities.hypothesis.numpy import float_arrays, int_arrays
from utilities.numpy import datetime64D, datetime64ns
from utilities.numpy.typing import NDArray1, NDArrayI1, NDArrayO1
from utilities.zarr import (
    InvalidIndexValueError,
    NDArrayWithIndexes,
    yield_array_with_indexes,
)

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

    @given(root=temp_paths())
    def test_dtype(self, root: Path) -> None:
        path = root.joinpath("array")
        with yield_array_with_indexes({}, path, dtype=int):
            pass
        view = NDArrayWithIndexes(path)
        assert view.dtype == int

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

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param({"x": 0}, array([0, 1, 2])),
            param({"x": -1}, array([3, 4, 5])),
            param({"x": slice(None, 1)}, array([[0, 1, 2]])),
            param({"x": []}, zeros((0, 3), dtype=int)),
            param({"x": array([True, False])}, array([[0, 1, 2]])),
            param({"x": array([0])}, array([[0, 1, 2]])),
            param({"x": 0, "y": 0}, 0),
            param({"x": 0, "y": -1}, 2),
            param({"x": 0, "y": slice(None, 1)}, array([0])),
            param({"x": 0, "y": []}, array([])),
            param({"x": 0, "y": array([True, False, False])}, array([0])),
            param({"x": 0, "y": array([0])}, array([0])),
        ],
    )
    def test_isel(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        indexes: dict[Hashable, NDArray1] = {"x": arange(2), "y": arange(3)}
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(6, dtype=int).reshape(2, 3)
        view = NDArrayWithIndexes(path)
        assert_equal(view.isel(indexer), expected)

    @mark.parametrize("indexer", [param({"x": 2}), param({"x": [2]})])
    def test_isel_error(self, tmp_path: Path, indexer: dict[Hashable, Any]) -> None:
        indexes: dict[Hashable, NDArray1] = {"x": arange(2), "y": arange(3)}
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(6, dtype=int).reshape(2, 3)
        view = NDArrayWithIndexes(path)
        with raises(BoundsCheckError):
            _ = view.isel(indexer)

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param({"x": "x0"}, array([0, 1, 2])),
            param({"x": []}, zeros((0, 3), dtype=int)),
            param({"x": ["x0"]}, array([[0, 1, 2]])),
            param({"x": ["x0", "x1"]}, array([[0, 1, 2], [3, 4, 5]])),
            param({"x": "x0", "y": "y0"}, 0),
            param({"x": "x0", "y": []}, zeros(0, dtype=int)),
            param({"x": "x0", "y": ["y0"]}, array([0])),
            param({"x": "x0", "y": ["y0", "y1"]}, array([0, 1])),
        ],
    )
    def test_sel(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        indexes: dict[Hashable, NDArray1] = {
            "x": array(["x0", "x1"]),
            "y": array(["y0", "y1", "y2"]),
        }
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(6, dtype=int).reshape(2, 3)
        view = NDArrayWithIndexes(path)
        assert_equal(view.sel(indexer), expected)

    @mark.parametrize(
        ("index", "indexer"),
        [
            param(array(["x0", "x1"]), {"x": "x2"}),
            param(array(["x0", "x1"]), {"x": "x2"}),
            param(array(["x0", "x0"]), {"x": "x0"}),
            param(array(["x0", "x0"]), {"x": ["x0"]}),
        ],
    )
    def test_sel_error(
        self, tmp_path: Path, index: NDArrayO1, indexer: dict[Hashable, Any]
    ) -> None:
        indexes: dict[Hashable, NDArrayO1] = {"x": index}
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(2, dtype=int)
        view = NDArrayWithIndexes(path)
        with raises(InvalidIndexValueError):
            _ = view.sel(indexer)

    def test_missing(self, tmp_path: Path) -> None:
        with raises(FileNotFoundError):
            _ = NDArrayWithIndexes(tmp_path.joinpath("array"))

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param({"x": dt.date(2000, 1, 1)}, 0),
            param({"x": "2000-01-01"}, 0),
            param({"x": [dt.date(2000, 1, 1)]}, array([0])),
            param({"x": ["2000-01-01"]}, array([0])),
            param({"x": [dt.date(2000, 1, 1), dt.date(2000, 1, 2)]}, array([0, 1])),
            param({"x": [dt.date(2000, 1, 1), "2000-01-02"]}, array([0, 1])),
            param({"x": ["2000-01-01", dt.date(2000, 1, 2)]}, array([0, 1])),
            param({"x": ["2000-01-01", "2000-01-02"]}, array([0, 1])),
        ],
    )
    def test_sel_with_date(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        indexes: dict[Hashable, NDArray1] = {
            "x": array([dt.date(2000, 1, i) for i in range(1, 4)], dtype=datetime64D)
        }
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(3)
        view = NDArrayWithIndexes(path)
        assert_equal(view.sel(indexer), expected)

    @mark.parametrize(
        ("indexer", "expected"),
        [
            param({"x": dt.datetime(2000, 1, 1)}, 0),  # noqa: DTZ001
            param({"x": "2000-01-01"}, 0),
            param({"x": [dt.datetime(2000, 1, 1)]}, array([0])),  # noqa: DTZ001
            param({"x": ["2000-01-01"]}, array([0])),
            param(
                {
                    "x": [
                        dt.datetime(2000, 1, 1),  # noqa: DTZ001
                        dt.datetime(2000, 1, 2),  # noqa: DTZ001
                    ]
                },
                array([0, 1]),
            ),
            param(
                {"x": [dt.datetime(2000, 1, 1), "2000-01-02"]},  # noqa: DTZ001
                array([0, 1]),
            ),
            param(
                {"x": ["2000-01-01", dt.datetime(2000, 1, 2)]},  # noqa: DTZ001
                array([0, 1]),
            ),
            param({"x": ["2000-01-01", "2000-01-02"]}, array([0, 1])),
        ],
    )
    def test_sel_with_datetime(
        self, tmp_path: Path, indexer: dict[Hashable, Any], expected: Any
    ) -> None:
        indexes: dict[Hashable, NDArray1] = {
            "x": array([dt.date(2000, 1, i) for i in range(1, 4)], dtype=datetime64ns)
        }
        path = tmp_path.joinpath("array")
        with yield_array_with_indexes(indexes, path, dtype=int) as z_array:
            z_array[:] = arange(3, dtype=int)
        view = NDArrayWithIndexes(path)
        assert_equal(view.sel(indexer), expected)
