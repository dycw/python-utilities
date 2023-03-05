import datetime as dt
from typing import Any, Optional

from hypothesis import assume, given
from hypothesis.strategies import DataObject, data, dates, integers
from numpy import (
    arange,
    array,
    full,
    inf,
    isclose,
    median,
    nan,
    ndarray,
    ones,
    zeros,
    zeros_like,
)
from numpy.testing import assert_allclose
from pandas import DatetimeTZDtype, Series
from pytest import mark, param, raises

from utilities.hypothesis.numpy import float_arrays
from utilities.numpy import (
    InfElementsError,
    MultipleTrueElementsError,
    NanElementsError,
    NonIntegralElementsError,
    NoTrueElementsError,
    ZeroPercentageChangeSpanError,
    ZeroShiftError,
    as_int,
    datetime64D,
    discretize,
    ffill,
    fillna,
    flatn0,
    has_dtype,
    maximum,
    minimum,
    pct_change,
    shift,
    shift_bool,
    year,
)
from utilities.numpy.typing import NDArrayF1


class TestAsInt:
    @given(n=integers(-10, 10))
    def test_main(self, n: int) -> None:
        arr = array([n], dtype=float)
        result = as_int(arr)
        expected = array([n], dtype=int)
        assert_allclose(result, expected)

    def test_nan_elements_error(self) -> None:
        arr = array([nan], dtype=float)
        with raises(NanElementsError):
            _ = as_int(arr)

    @given(n=integers(-10, 10))
    def test_nan_elements_fill(self, n: int) -> None:
        arr = array([nan], dtype=float)
        result = as_int(arr, nan=n)
        expected = array([n], dtype=int)
        assert_allclose(result, expected)

    def test_inf_elements_error(self) -> None:
        arr = array([inf], dtype=float)
        with raises(InfElementsError):
            _ = as_int(arr)

    @given(n=integers(-10, 10))
    def test_inf_elements_fill(self, n: int) -> None:
        arr = array([inf], dtype=float)
        result = as_int(arr, inf=n)
        expected = array([n], dtype=int)
        assert_allclose(result, expected)

    @given(n=integers(-10, 10))
    def test_non_integral_elements(self, n: int) -> None:
        arr = array([n + 0.5], dtype=float)
        with raises(NonIntegralElementsError):
            _ = as_int(arr)


class TestDiscretize:
    @given(arr=float_arrays(shape=integers(0, 10), min_value=-1.0, max_value=1.0))
    def test_1_bin(self, arr: NDArrayF1) -> None:
        result = discretize(arr, 1)
        expected = zeros_like(arr, dtype=float)
        assert_allclose(result, expected, equal_nan=True)

    @given(
        arr=float_arrays(
            shape=integers(1, 10), min_value=-1.0, max_value=1.0, unique=True
        )
    )
    def test_2_bins(self, arr: NDArrayF1) -> None:
        _ = assume(len(arr) % 2 == 0)
        result = discretize(arr, 2)
        med = median(arr)
        is_below = (arr < med) & ~isclose(arr, med)
        assert isclose(result[is_below], 0.0).all()
        is_above = (arr > med) & ~isclose(arr, med)
        assert isclose(result[is_above], 1.0).all()

    @given(bins=integers(1, 10))
    def test_empty(self, bins: int) -> None:
        arr = array([], dtype=float)
        result = discretize(arr, bins)
        assert_allclose(result, arr, equal_nan=True)

    @given(n=integers(0, 10), bins=integers(1, 10))
    def test_all_nan(self, n: int, bins: int) -> None:
        arr = full(n, nan, dtype=float)
        result = discretize(arr, bins)
        assert_allclose(result, arr, equal_nan=True)

    @mark.parametrize(
        ("arr_v", "bins", "expected_v"),
        [
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.25, 0.5, 0.75, 1.0],
                [0.0, 1.0, 2.0, 3.0],
                id="equally spaced",
            ),
            param(
                [1.0, 2.0, 3.0, 4.0],
                [0.0, 0.1, 0.9, 1.0],
                [0.0, 1.0, 1.0, 2.0],
                id="unequally spaced",
            ),
            param(
                [1.0, 2.0, 3.0],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0],
                id="equally spaced 1 to 2",
            ),
            param(
                [1.0, 2.0, 3.0, nan],
                [0.0, 0.33, 1.0],
                [0.0, 1.0, 1.0, nan],
                id="with nan",
            ),
        ],
    )
    def test_bins_of_floats(
        self, arr_v: list[float], bins: list[float], expected_v: list[float]
    ) -> None:
        arr = array(arr_v, dtype=float)
        result = discretize(arr, bins)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, equal_nan=True)


class TestHasDtype:
    @mark.parametrize(
        ("x", "dtype", "expected"),
        [
            param(array([]), float, True),
            param(array([]), (float,), True),
            param(array([]), int, False),
            param(array([]), (int,), False),
            param(array([]), "Int64", False),
            param(array([]), ("Int64",), False),
            param(Series([], dtype="Int64"), "Int64", True),
            param(Series([], dtype="Int64"), int, False),
            param(
                Series([], dtype=DatetimeTZDtype(tz="UTC")),
                DatetimeTZDtype(tz="UTC"),
                True,
            ),
            param(
                Series([], dtype=DatetimeTZDtype(tz="UTC")),
                DatetimeTZDtype(tz="Asia/Hong_Kong"),
                False,
            ),
        ],
    )
    def test_main(self, x: Any, dtype: Any, expected: bool) -> None:
        assert has_dtype(x, dtype) is expected


class TestFFill:
    @mark.parametrize(("limit", "expected_v"), [param(None, 0.2), param(1, nan)])
    def test_main(self, limit: Optional[int], expected_v: float) -> None:
        arr = array([0.1, nan, 0.2, nan, nan, 0.3])
        result = ffill(arr, limit=limit)
        expected = array([0.1, 0.1, 0.2, 0.2, expected_v, 0.3])
        assert_allclose(result, expected, equal_nan=True)


class TestFillNa:
    @mark.parametrize(
        ("init", "value", "expected_v"),
        [
            param(0.0, 0.0, 0.0),
            param(0.0, nan, 0.0),
            param(0.0, inf, 0.0),
            param(nan, 0.0, 0.0),
            param(nan, nan, nan),
            param(nan, inf, inf),
            param(inf, 0.0, inf),
            param(inf, nan, inf),
            param(inf, inf, inf),
        ],
    )
    def test_main(self, init: float, value: float, expected_v: float) -> None:
        arr = array([init], dtype=float)
        result = fillna(arr, value=value)
        expected = array([expected_v], dtype=float)
        assert_allclose(result, expected, equal_nan=True)


class TestFlatN0:
    @given(data=data(), n=integers(1, 10))
    def test_main(self, data: DataObject, n: int) -> None:
        i = data.draw(integers(0, n - 1))
        arr = arange(n) == i
        result = flatn0(arr)
        assert result == i

    def test_no_true_elements(self) -> None:
        arr = zeros(0, dtype=bool)
        with raises(NoTrueElementsError):
            _ = flatn0(arr)

    @given(n=integers(2, 10))
    def test_all_true_elements(self, n: int) -> None:
        arr = ones(n, dtype=bool)
        with raises(MultipleTrueElementsError):
            _ = flatn0(arr)


class TestMaximumMinimum:
    def test_maximum_floats(self) -> None:
        result = maximum(1.0, 2.0)
        assert isinstance(result, float)

    def test_maximum_arrays(self) -> None:
        result = maximum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)

    def test_minimum_floats(self) -> None:
        result = minimum(1.0, 2.0)
        assert isinstance(result, float)

    def test_minimum_arrays(self) -> None:
        result = minimum(array([1.0], dtype=float), array([2.0], dtype=float))
        assert isinstance(result, ndarray)


class TestPctChange:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [nan, 0.1, 0.090909]),
            param(2, [nan, nan, 0.2]),
            param(-1, [-0.090909, -0.083333, nan]),
            param(-2, [-0.166667, nan, nan]),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_1d(self, n: int, expected_v: list[float], dtype: type[Any]) -> None:
        arr = arange(10, 13, dtype=dtype)
        result = pct_change(arr, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, atol=1e-4, equal_nan=True)

    @mark.parametrize(
        ("axis", "n", "expected_v"),
        [
            param(
                0,
                1,
                [
                    4 * [nan],
                    [0.4, 0.363636, 0.333333, 0.307692],
                    [0.285714, 0.266667, 0.25, 0.235294],
                ],
                id="axis=0, n=1",
            ),
            param(
                0,
                2,
                [4 * [nan], 4 * [nan], [0.8, 0.727272, 0.666667, 0.615385]],
                id="axis=0, n=2",
            ),
            param(
                0,
                -1,
                [
                    [-0.285714, -0.266667, -0.25, -0.235294],
                    [-0.222222, -0.210526, -0.2, -0.190476],
                    4 * [nan],
                ],
                id="axis=0, n=-1",
            ),
            param(
                0,
                -2,
                [[-0.444444, -0.421053, -0.4, -0.380952], 4 * [nan], 4 * [nan]],
                id="axis=0, n=-2",
            ),
            param(
                1,
                1,
                [
                    [nan, 0.1, 0.090909, 0.083333],
                    [nan, 0.071429, 0.066667, 0.0625],
                    [nan, 0.055556, 0.052632, 0.05],
                ],
                id="axis=1, n=1",
            ),
            param(
                1,
                2,
                [
                    [nan, nan, 0.2, 0.181818],
                    [nan, nan, 0.1428527, 0.133333],
                    [nan, nan, 0.111111, 0.105263],
                ],
                id="axis=1, n=1",
            ),
            param(
                1,
                -1,
                [
                    [-0.090909, -0.083333, -0.076923, nan],
                    [-0.066667, -0.0625, -0.058824, nan],
                    [-0.052632, -0.05, -0.047619, nan],
                ],
                id="axis=1, n=-1",
            ),
            param(
                1,
                -2,
                [
                    [-0.166667, -0.153846, nan, nan],
                    [-0.125, -0.117647, nan, nan],
                    [-0.1, -0.095238, nan, nan],
                ],
                id="axis=1, n=-2",
            ),
        ],
    )
    def test_2d(self, axis: int, n: int, expected_v: list[list[float]]) -> None:
        arr = arange(10, 22, dtype=float).reshape((3, 4))
        result = pct_change(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, atol=1e-4, equal_nan=True)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(ZeroPercentageChangeSpanError):
            _ = pct_change(arr, n=0)


class TestShift:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [nan, 0.0, 1.0]),
            param(2, [nan, nan, 0.0]),
            param(-1, [1.0, 2.0, nan]),
            param(-2, [2.0, nan, nan]),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_1d(self, n: int, expected_v: list[float], dtype: type[Any]) -> None:
        arr = arange(3, dtype=dtype)
        result = shift(arr, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, equal_nan=True)

    @mark.parametrize(
        ("axis", "n", "expected_v"),
        [
            param(
                0,
                1,
                [4 * [nan], [0.0, 1.0, 2.0, 3.0], [4.0, 5.0, 6.0, 7.0]],
                id="axis=0, n=1",
            ),
            param(0, 2, [4 * [nan], 4 * [nan], [0.0, 1.0, 2.0, 3.0]], id="axis=0, n=2"),
            param(
                0,
                -1,
                [[4.0, 5.0, 6.0, 7.0], [8.0, 9.0, 10.0, 11.0], 4 * [nan]],
                id="axis=0, n=-1",
            ),
            param(
                0, -2, [[8.0, 9.0, 10.0, 11.0], 4 * [nan], 4 * [nan]], id="axis=0, n=-2"
            ),
            param(
                1,
                1,
                [[nan, 0.0, 1.0, 2.0], [nan, 4.0, 5.0, 6.0], [nan, 8.0, 9.0, 10.0]],
                id="axis=1, n=1",
            ),
            param(
                1,
                2,
                [[nan, nan, 0.0, 1.0], [nan, nan, 4.0, 5.0], [nan, nan, 8.0, 9.0]],
                id="axis=1, n=1",
            ),
            param(
                1,
                -1,
                [[1.0, 2.0, 3.0, nan], [5.0, 6.0, 7.0, nan], [9.0, 10.0, 11.0, nan]],
                id="axis=1, n=-1",
            ),
            param(
                1,
                -2,
                [[2.0, 3.0, nan, nan], [6.0, 7.0, nan, nan], [10.0, 11.0, nan, nan]],
                id="axis=1, n=-2",
            ),
        ],
    )
    def test_2d(self, axis: int, n: int, expected_v: list[list[float]]) -> None:
        arr = arange(12, dtype=float).reshape((3, 4))
        result = shift(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, equal_nan=True)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(ZeroShiftError):
            _ = shift(arr, n=0)


class TestShiftBool:
    @mark.parametrize(
        ("n", "expected_v"),
        [
            param(1, [None, True, False], id="n=1"),
            param(2, [None, None, True], id="n=2"),
            param(-1, [False, True, None], id="n=-1"),
            param(-2, [True, None, None], id="n=-2"),
        ],
    )
    @mark.parametrize("fill_value", [param(True), param(False)])
    def test_main(
        self, n: int, expected_v: list[Optional[bool]], fill_value: bool
    ) -> None:
        arr = array([True, False, True], dtype=bool)
        result = shift_bool(arr, n=n, fill_value=fill_value)
        expected = array(
            [fill_value if e is None else e for e in expected_v], dtype=bool
        )
        assert (result == expected).all()


class TestYear:
    @given(date=dates())
    def test_main(self, date: dt.date) -> None:
        dates = array([date], dtype=datetime64D)
        years = year(dates)
        assert years.item() == date.year
