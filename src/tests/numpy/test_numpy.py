from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any, Literal

from hypothesis import assume, given
from hypothesis.strategies import DataObject, data, dates, floats, integers
from numpy import (
    arange,
    array,
    concatenate,
    datetime64,
    eye,
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
from numpy.testing import assert_allclose, assert_equal
from pandas import DatetimeTZDtype, Series
from pytest import mark, param, raises

from utilities.datetime import UTC
from utilities.hypothesis import assume_does_not_raise, datetimes_utc
from utilities.hypothesis.numpy import (
    datetime64_dtypes,
    datetime64_units,
    datetime64s,
    float_arrays,
)
from utilities.numpy import (
    DateOverflowError,
    Datetime64Kind,
    Datetime64Unit,
    EmptyNumpyConcatenateError,
    InfElementsError,
    InvalidDTypeError,
    LossOfNanosecondsError,
    MultipleTrueElementsError,
    NanElementsError,
    NonIntegralElementsError,
    NoTrueElementsError,
    ZeroPercentageChangeSpanError,
    ZeroShiftError,
    array_indexer,
    as_int,
    date_to_datetime64,
    datetime64_dtype_to_unit,
    datetime64_to_date,
    datetime64_to_datetime,
    datetime64_to_int,
    datetime64_unit_to_dtype,
    datetime64_unit_to_kind,
    datetime64D,
    datetime64ns,
    datetime64us,
    datetime64Y,
    datetime_to_datetime64,
    discretize,
    ewma,
    exp_moving_sum,
    ffill,
    ffill_non_nan_slices,
    fillna,
    flatn0,
    get_fill_value,
    has_dtype,
    is_empty,
    is_non_empty,
    is_non_singular,
    is_positive_semidefinite,
    is_symmetric,
    maximum,
    minimum,
    pct_change,
    redirect_to_empty_numpy_concatenate_error,
    shift,
    shift_bool,
    year,
)
from utilities.numpy.typing import NDArrayF, NDArrayF1, NDArrayF2, NDArrayI2


class TestArrayIndexer:
    @mark.parametrize(
        ("i", "ndim", "expected"),
        [
            param(0, 1, (0,)),
            param(0, 2, (slice(None), 0)),
            param(1, 2, (slice(None), 1)),
            param(0, 3, (slice(None), slice(None), 0)),
            param(1, 3, (slice(None), slice(None), 1)),
            param(2, 3, (slice(None), slice(None), 2)),
        ],
    )
    def test_main(
        self, i: int, ndim: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim) == expected

    @mark.parametrize(
        ("i", "ndim", "axis", "expected"),
        [
            param(0, 1, 0, (0,)),
            param(0, 2, 0, (0, slice(None))),
            param(0, 2, 1, (slice(None), 0)),
            param(1, 2, 0, (1, slice(None))),
            param(1, 2, 1, (slice(None), 1)),
            param(0, 3, 0, (0, slice(None), slice(None))),
            param(0, 3, 1, (slice(None), 0, slice(None))),
            param(0, 3, 2, (slice(None), slice(None), 0)),
            param(1, 3, 0, (1, slice(None), slice(None))),
            param(1, 3, 1, (slice(None), 1, slice(None))),
            param(1, 3, 2, (slice(None), slice(None), 1)),
            param(2, 3, 0, (2, slice(None), slice(None))),
            param(2, 3, 1, (slice(None), 2, slice(None))),
            param(2, 3, 2, (slice(None), slice(None), 2)),
        ],
    )
    def test_axis(
        self, i: int, ndim: int, axis: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim, axis=axis) == expected


class TestAsInt:
    @given(n=integers(-10, 10))
    def test_main(self, n: int) -> None:
        arr = array([n], dtype=float)
        result = as_int(arr)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    def test_nan_elements_error(self) -> None:
        arr = array([nan], dtype=float)
        with raises(NanElementsError):
            _ = as_int(arr)

    @given(n=integers(-10, 10))
    def test_nan_elements_fill(self, n: int) -> None:
        arr = array([nan], dtype=float)
        result = as_int(arr, nan=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    def test_inf_elements_error(self) -> None:
        arr = array([inf], dtype=float)
        with raises(InfElementsError):
            _ = as_int(arr)

    @given(n=integers(-10, 10))
    def test_inf_elements_fill(self, n: int) -> None:
        arr = array([inf], dtype=float)
        result = as_int(arr, inf=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_non_integral_elements(self, n: int) -> None:
        arr = array([n + 0.5], dtype=float)
        with raises(NonIntegralElementsError):
            _ = as_int(arr)


class TestDateToDatetime64ns:
    def test_example(self) -> None:
        result = date_to_datetime64(dt.date(2000, 1, 1))
        assert result == datetime64("2000-01-01", "D")
        assert result.dtype == datetime64D

    @given(date=dates())
    def test_main(self, date: dt.date) -> None:
        result = date_to_datetime64(date)
        assert result.dtype == datetime64D


class TestDatetimeToDatetime64ns:
    def test_example(self) -> None:
        result = datetime_to_datetime64(
            dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
        )
        assert result == datetime64("2000-01-01 00:00:00.123456", "us")
        assert result.dtype == datetime64us

    @given(datetime=datetimes_utc())
    def test_main(self, datetime: dt.datetime) -> None:
        result = datetime_to_datetime64(datetime)
        assert result.dtype == datetime64us


class TestDatetime64ToDate:
    def test_example(self) -> None:
        assert datetime64_to_date(datetime64("2000-01-01", "D")) == dt.date(
            2000, 1, 1
        )

    @given(date=dates())
    def test_round_trip(self, date: dt.date) -> None:
        assert datetime64_to_date(date_to_datetime64(date)) == date

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("10000-01-01", "D", DateOverflowError),
            param("2000-01-01", "ns", NotImplementedError),
        ],
    )
    def test_error(
        self, datetime: str, dtype: str, error: type[Exception]
    ) -> None:
        with raises(error):
            _ = datetime64_to_date(datetime64(datetime, dtype))


class TestDatetime64ToInt:
    def test_example(self) -> None:
        expected = 10957
        assert datetime64_to_int(datetime64("2000-01-01", "D")) == expected

    @given(datetime=datetime64s())
    def test_main(self, datetime: datetime64) -> None:
        _ = datetime64_to_int(datetime)

    @given(data=data(), unit=datetime64_units())
    def test_round_trip(self, data: DataObject, unit: Datetime64Unit) -> None:
        datetime = data.draw(datetime64s(unit=unit))
        result = datetime64(datetime64_to_int(datetime), unit)
        assert result == datetime


class TestDatetime64ToDatetime:
    def test_example_ms(self) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123", "ms")
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)

    @mark.parametrize("dtype", [param("us"), param("ns")])
    def test_examples_us_ns(self, dtype: str) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123456", dtype)
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)

    @given(datetime=datetimes_utc())
    def test_round_trip(self, datetime: dt.datetime) -> None:
        assert (
            datetime64_to_datetime(datetime_to_datetime64(datetime)) == datetime
        )

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("0000-12-31", "ms", DateOverflowError),
            param("10000-01-01", "ms", DateOverflowError),
            param(
                "1970-01-01 00:00:00.000000001", "ns", LossOfNanosecondsError
            ),
            param("2000-01-01", "D", NotImplementedError),
        ],
    )
    def test_error(
        self, datetime: str, dtype: str, error: type[Exception]
    ) -> None:
        with raises(error):
            _ = datetime64_to_datetime(datetime64(datetime, dtype))


class TestDatetime64DTypeToUnit:
    @mark.parametrize(
        ("dtype", "expected"),
        [
            param(datetime64D, "D"),
            param(datetime64Y, "Y"),
            param(datetime64ns, "ns"),
        ],
    )
    def test_example(self, dtype: Any, expected: Datetime64Unit) -> None:
        assert datetime64_dtype_to_unit(dtype) == expected

    @given(dtype=datetime64_dtypes())
    def test_round_trip(self, dtype: Any) -> None:
        assert (
            datetime64_unit_to_dtype(datetime64_dtype_to_unit(dtype)) == dtype
        )


class TestDatetime64DUnitToDType:
    @mark.parametrize(
        ("unit", "expected"),
        [
            param("D", datetime64D),
            param("Y", datetime64Y),
            param("ns", datetime64ns),
        ],
    )
    def test_example(self, unit: Datetime64Unit, expected: Any) -> None:
        assert datetime64_unit_to_dtype(unit) == expected

    @given(unit=datetime64_units())
    def test_round_trip(self, unit: Datetime64Unit) -> None:
        assert datetime64_dtype_to_unit(datetime64_unit_to_dtype(unit)) == unit


class TestDatetime64DUnitToKind:
    @mark.parametrize(
        ("unit", "expected"),
        [param("D", "date"), param("Y", "date"), param("ns", "time")],
    )
    def test_example(
        self, unit: Datetime64Unit, expected: Datetime64Kind
    ) -> None:
        assert datetime64_unit_to_kind(unit) == expected


class TestDiscretize:
    @given(
        arr=float_arrays(shape=integers(0, 10), min_value=-1.0, max_value=1.0)
    )
    def test_1_bin(self, arr: NDArrayF1) -> None:
        result = discretize(arr, 1)
        expected = zeros_like(arr, dtype=float)
        assert_equal(result, expected)

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
        assert_equal(result, arr)

    @given(n=integers(0, 10), bins=integers(1, 10))
    def test_all_nan(self, n: int, bins: int) -> None:
        arr = full(n, nan, dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

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
        self,
        arr_v: Sequence[float],
        bins: Sequence[float],
        expected_v: Sequence[float],
    ) -> None:
        arr = array(arr_v, dtype=float)
        result = discretize(arr, bins)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)


class TestEwma:
    @given(data=data(), array=float_arrays(), halflife=floats(0.1, 10.0))
    def test_main(
        self, data: DataObject, array: NDArrayF, halflife: float
    ) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = ewma(array, halflife, axis=axis)


class TestExpMovingSum:
    @given(data=data(), array=float_arrays(), halflife=floats(0.1, 10.0))
    def test_main(
        self, data: DataObject, array: NDArrayF, halflife: float
    ) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, halflife, axis=axis)


class TestFFill:
    @mark.parametrize(
        ("limit", "expected_v"), [param(None, 0.2), param(1, nan)]
    )
    def test_main(self, limit: int | None, expected_v: float) -> None:
        arr = array([0.1, nan, 0.2, nan, nan, 0.3], dtype=float)
        result = ffill(arr, limit=limit)
        expected = array([0.1, 0.1, 0.2, 0.2, expected_v, 0.3], dtype=float)
        assert_equal(result, expected)


class TestFFillNonNanSlices:
    @mark.parametrize(
        ("limit", "axis", "expected_v"),
        [
            param(
                None,
                0,
                [
                    [0.1, nan, nan, 0.2],
                    [0.1, nan, nan, 0.2],
                    [0.3, nan, nan, nan],
                ],
            ),
            param(
                None, 1, [[0.1, 0.1, 0.1, 0.2], 4 * [nan], [0.3, 0.3, 0.3, nan]]
            ),
            param(
                1,
                0,
                [
                    [0.1, nan, nan, 0.2],
                    [0.1, nan, nan, 0.2],
                    [0.3, nan, nan, nan],
                ],
            ),
            param(
                1, 1, [[0.1, 0.1, nan, 0.2], 4 * [nan], [0.3, 0.3, nan, nan]]
            ),
        ],
    )
    def test_main(
        self,
        limit: int | None,
        axis: int,
        expected_v: Sequence[Sequence[float]],
    ) -> None:
        arr = array(
            [[0.1, nan, nan, 0.2], 4 * [nan], [0.3, nan, nan, nan]], dtype=float
        )
        result = ffill_non_nan_slices(arr, limit=limit, axis=axis)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    @mark.parametrize(
        ("axis", "expected_v"),
        [
            param(0, [4 * [nan], [nan, 0.1, nan, nan], [nan, 0.1, nan, nan]]),
            param(1, [4 * [nan], [nan, 0.1, 0.1, 0.1], 4 * [nan]]),
        ],
    )
    def test_initial_all_nan(
        self, axis: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = array([4 * [nan], [nan, 0.1, nan, nan], 4 * [nan]], dtype=float)
        result = ffill_non_nan_slices(arr, axis=axis)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)


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
        assert_equal(result, expected)


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


class TestGetFillValue:
    @mark.parametrize(
        "dtype",
        [
            param(bool),
            param(datetime64D),
            param(datetime64Y),
            param(datetime64ns),
            param(float),
            param(int),
            param(object),
        ],
    )
    def test_main(self, dtype: Any) -> None:
        fill_value = get_fill_value(dtype)
        array = full(0, fill_value, dtype=dtype)
        assert has_dtype(array, dtype)

    def test_error(self) -> None:
        with raises(InvalidDTypeError):
            _ = get_fill_value(None)


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
    def test_main(self, *, x: Any, dtype: Any, expected: bool) -> None:
        assert has_dtype(x, dtype) is expected


class TestIsEmptyAndIsNotEmpty:
    @mark.parametrize(
        ("shape", "expected"),
        [
            param(0, "empty"),
            param(1, "non-empty"),
            param(2, "non-empty"),
            param((), "empty"),
            param((0,), "empty"),
            param((1,), "non-empty"),
            param((2,), "non-empty"),
            param((0, 0), "empty"),
            param((0, 1), "empty"),
            param((0, 2), "empty"),
            param((1, 0), "empty"),
            param((1, 1), "non-empty"),
            param((1, 2), "non-empty"),
            param((2, 0), "empty"),
            param((2, 1), "non-empty"),
            param((2, 2), "non-empty"),
        ],
    )
    @mark.parametrize("kind", [param("shape"), param("array")])
    def test_main(
        self,
        shape: int | tuple[int, ...],
        kind: Literal["shape", "array"],
        expected: Literal["empty", "non-empty"],
    ) -> None:
        shape_or_array = shape if kind == "shape" else zeros(shape, dtype=float)
        assert is_empty(shape_or_array) is (expected == "empty")
        assert is_non_empty(shape_or_array) is (expected == "non-empty")


class TestIsNonSingular:
    @mark.parametrize(
        ("array", "expected"), [param(eye(2), True), param(ones((2, 2)), False)]
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF2, dtype: Any, expected: bool
    ) -> None:
        assert is_non_singular(array.astype(dtype)) is expected

    def test_overflow(self) -> None:
        arr = array([[0.0, 0.0], [5e-323, 0.0]], dtype=float)
        assert not is_non_singular(arr)


class TestIsPositiveSemiDefinite:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF2 | NDArrayI2, dtype: Any, expected: bool
    ) -> None:
        assert is_positive_semidefinite(array.astype(dtype)) is expected

    @given(array=float_arrays(shape=(2, 2), min_value=-1.0, max_value=1.0))
    def test_overflow(self, array: NDArrayF2) -> None:
        _ = is_positive_semidefinite(array)


class TestIsSymmetric:
    @mark.parametrize(
        ("array", "expected"),
        [
            param(eye(2), True),
            param(zeros((1, 2), dtype=float), False),
            param(arange(4).reshape((2, 2)), False),
        ],
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(
        self, *, array: NDArrayF2 | NDArrayI2, dtype: Any, expected: bool
    ) -> None:
        assert is_symmetric(array.astype(dtype)) is expected


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
    def test_1d(
        self, n: int, expected_v: Sequence[float], dtype: type[Any]
    ) -> None:
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
    def test_2d(
        self, axis: int, n: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = arange(10, 22, dtype=float).reshape((3, 4))
        result = pct_change(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, atol=1e-4, equal_nan=True)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(ZeroPercentageChangeSpanError):
            _ = pct_change(arr, n=0)


class TestRedirectToEmptyNumpyConcatenateError:
    def test_main(self) -> None:
        with raises(EmptyNumpyConcatenateError):
            try:
                _ = concatenate([])
            except ValueError as error:
                redirect_to_empty_numpy_concatenate_error(error)


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
    def test_1d(
        self, n: int, expected_v: Sequence[float], dtype: type[Any]
    ) -> None:
        arr = arange(3, dtype=dtype)
        result = shift(arr, n=n)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    @mark.parametrize(
        ("axis", "n", "expected_v"),
        [
            param(
                0,
                1,
                [4 * [nan], [0.0, 1.0, 2.0, 3.0], [4.0, 5.0, 6.0, 7.0]],
                id="axis=0, n=1",
            ),
            param(
                0,
                2,
                [4 * [nan], 4 * [nan], [0.0, 1.0, 2.0, 3.0]],
                id="axis=0, n=2",
            ),
            param(
                0,
                -1,
                [[4.0, 5.0, 6.0, 7.0], [8.0, 9.0, 10.0, 11.0], 4 * [nan]],
                id="axis=0, n=-1",
            ),
            param(
                0,
                -2,
                [[8.0, 9.0, 10.0, 11.0], 4 * [nan], 4 * [nan]],
                id="axis=0, n=-2",
            ),
            param(
                1,
                1,
                [
                    [nan, 0.0, 1.0, 2.0],
                    [nan, 4.0, 5.0, 6.0],
                    [nan, 8.0, 9.0, 10.0],
                ],
                id="axis=1, n=1",
            ),
            param(
                1,
                2,
                [
                    [nan, nan, 0.0, 1.0],
                    [nan, nan, 4.0, 5.0],
                    [nan, nan, 8.0, 9.0],
                ],
                id="axis=1, n=1",
            ),
            param(
                1,
                -1,
                [
                    [1.0, 2.0, 3.0, nan],
                    [5.0, 6.0, 7.0, nan],
                    [9.0, 10.0, 11.0, nan],
                ],
                id="axis=1, n=-1",
            ),
            param(
                1,
                -2,
                [
                    [2.0, 3.0, nan, nan],
                    [6.0, 7.0, nan, nan],
                    [10.0, 11.0, nan, nan],
                ],
                id="axis=1, n=-2",
            ),
        ],
    )
    def test_2d(
        self, axis: int, n: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = arange(12, dtype=float).reshape((3, 4))
        result = shift(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

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
        self, *, n: int, expected_v: Sequence[bool | None], fill_value: bool
    ) -> None:
        arr = array([True, False, True], dtype=bool)
        result = shift_bool(arr, n=n, fill_value=fill_value)
        expected = array(
            [fill_value if e is None else e for e in expected_v], dtype=bool
        )
        assert_equal(result, expected)


class TestYear:
    @given(date=dates())
    def test_scalar(self, date: dt.date) -> None:
        date64 = datetime64(date, "D")
        yr = year(date64)
        assert yr == date.year

    @given(date=dates())
    def test_array(self, date: dt.date) -> None:
        dates = array([date], dtype=datetime64D)
        years = year(dates)
        assert years.item() == date.year
