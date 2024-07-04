from __future__ import annotations

import datetime as dt
from re import escape
from typing import TYPE_CHECKING, Any, Literal

from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    none,
)
from numpy import (
    arange,
    array,
    bool_,
    concatenate,
    datetime64,
    eye,
    float64,
    full,
    inf,
    int64,
    isclose,
    median,
    nan,
    ndarray,
    object_,
    ones,
    zeros,
    zeros_like,
)
from numpy.random import Generator
from numpy.testing import assert_allclose, assert_equal
from pandas import DatetimeTZDtype, Series
from pytest import mark, param, raises

from utilities.hypothesis import assume_does_not_raise, datetimes_utc, float_arrays
from utilities.numpy import (
    DEFAULT_RNG,
    AsIntError,
    DateTime64ToDateError,
    DateTime64ToDateTimeError,
    DatetimeToDatetime64Error,
    EmptyNumpyConcatenateError,
    FlatN0EmptyError,
    FlatN0MultipleError,
    GetFillValueError,
    NDArrayF,
    NDArrayF1,
    NDArrayF2,
    NDArrayI2,
    PctChangeError,
    ShiftError,
    array_indexer,
    as_int,
    date_to_datetime64,
    datetime64_to_date,
    datetime64_to_datetime,
    datetime64_to_int,
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
    is_at_least,
    is_at_least_or_nan,
    is_at_most,
    is_at_most_or_nan,
    is_between,
    is_between_or_nan,
    is_empty,
    is_finite_and_integral,
    is_finite_and_integral_or_nan,
    is_finite_and_negative,
    is_finite_and_negative_or_nan,
    is_finite_and_non_negative,
    is_finite_and_non_negative_or_nan,
    is_finite_and_non_positive,
    is_finite_and_non_positive_or_nan,
    is_finite_and_non_zero,
    is_finite_and_non_zero_or_nan,
    is_finite_and_positive,
    is_finite_and_positive_or_nan,
    is_finite_or_nan,
    is_greater_than,
    is_greater_than_or_nan,
    is_integral,
    is_integral_or_nan,
    is_less_than,
    is_less_than_or_nan,
    is_negative,
    is_negative_or_nan,
    is_non_empty,
    is_non_negative,
    is_non_negative_or_nan,
    is_non_positive,
    is_non_positive_or_nan,
    is_non_singular,
    is_non_zero,
    is_non_zero_or_nan,
    is_positive,
    is_positive_or_nan,
    is_positive_semidefinite,
    is_symmetric,
    is_zero,
    is_zero_or_finite_and_non_micro,
    is_zero_or_finite_and_non_micro_or_nan,
    is_zero_or_nan,
    is_zero_or_non_micro,
    is_zero_or_non_micro_or_nan,
    maximum,
    minimum,
    pct_change,
    redirect_empty_numpy_concatenate,
    shift,
    shift_bool,
    year,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    from collections.abc import Sequence
    from zoneinfo import ZoneInfo


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
        self, *, i: int, ndim: int, expected: tuple[int | slice, ...]
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
        self, *, i: int, ndim: int, axis: int, expected: tuple[int | slice, ...]
    ) -> None:
        assert array_indexer(i, ndim, axis=axis) == expected


class TestAsInt:
    @given(n=integers(-10, 10), fuzz=floats(-1e-8, 1e-8) | none())
    def test_main(self, *, n: int, fuzz: float | None) -> None:
        n_use = n if fuzz is None else (n + fuzz)
        arr = array([n_use], dtype=float)
        result = as_int(arr)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_nan_elements_filled(self, *, n: int) -> None:
        arr = array([nan], dtype=float)
        result = as_int(arr, nan=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @given(n=integers(-10, 10))
    def test_inf_elements_filled(self, *, n: int) -> None:
        arr = array([inf], dtype=float)
        result = as_int(arr, inf=n)
        expected = array([n], dtype=int)
        assert_equal(result, expected)

    @mark.parametrize("value", [param(inf), param(nan), param(0.5)])
    def test_errors(self, *, value: float) -> None:
        arr = array([value], dtype=float)
        with raises(AsIntError):
            _ = as_int(arr)


class TestDateToDatetime64ns:
    def test_example(self) -> None:
        result = date_to_datetime64(dt.date(2000, 1, 1))
        assert result == datetime64("2000-01-01", "D")
        assert result.dtype == datetime64D

    @given(date=dates())
    def test_main(self, *, date: dt.date) -> None:
        result = date_to_datetime64(date)
        assert result.dtype == datetime64D


class TestDatetimeToDatetime64:
    @mark.parametrize("time_zone", [param(UTC), param(None)])
    def test_example(self, *, time_zone: ZoneInfo | None) -> None:
        result = datetime_to_datetime64(
            dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=time_zone)
        )
        assert result == datetime64("2000-01-01 00:00:00.123456", "us")
        assert result.dtype == datetime64us

    @given(datetime=datetimes() | datetimes_utc())
    def test_main(self, *, datetime: dt.datetime) -> None:
        result = datetime_to_datetime64(datetime)
        assert result.dtype == datetime64us

    @given(datetime=datetimes(timezones=just(HONG_KONG)))
    def test_error(self, *, datetime: dt.datetime) -> None:
        with raises(
            DatetimeToDatetime64Error, match=r"Timezone must be None or UTC; got .*\."
        ):
            _ = datetime_to_datetime64(datetime)


class TestDatetime64ToDate:
    def test_example(self) -> None:
        assert datetime64_to_date(datetime64("2000-01-01", "D")) == dt.date(2000, 1, 1)

    @given(date=dates())
    def test_round_trip(self, *, date: dt.date) -> None:
        assert datetime64_to_date(date_to_datetime64(date)) == date

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("10000-01-01", "D", DateTime64ToDateError),
            param("2000-01-01", "ns", NotImplementedError),
        ],
    )
    def test_error(self, *, datetime: str, dtype: str, error: type[Exception]) -> None:
        with raises(error):
            _ = datetime64_to_date(datetime64(datetime, dtype))


class TestDatetime64ToInt:
    def test_example(self) -> None:
        expected = 10957
        assert datetime64_to_int(datetime64("2000-01-01", "D")) == expected


class TestDatetime64ToDatetime:
    def test_example_ms(self) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123", "ms")
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)

    @mark.parametrize("dtype", [param("us"), param("ns")])
    def test_examples_us_ns(self, *, dtype: str) -> None:
        assert datetime64_to_datetime(
            datetime64("2000-01-01 00:00:00.123456", dtype)
        ) == dt.datetime(2000, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)

    @given(datetime=datetimes_utc())
    def test_round_trip(self, *, datetime: dt.datetime) -> None:
        assert datetime64_to_datetime(datetime_to_datetime64(datetime)) == datetime

    @mark.parametrize(
        ("datetime", "dtype", "error"),
        [
            param("0000-12-31", "ms", DateTime64ToDateTimeError),
            param("10000-01-01", "ms", DateTime64ToDateTimeError),
            param("1970-01-01 00:00:00.000000001", "ns", DateTime64ToDateTimeError),
            param("2000-01-01", "D", NotImplementedError),
        ],
    )
    def test_error(self, *, datetime: str, dtype: str, error: type[Exception]) -> None:
        with raises(error):
            _ = datetime64_to_datetime(datetime64(datetime, dtype))


class TestDefaultRng:
    def test_main(self) -> None:
        assert isinstance(DEFAULT_RNG, Generator)


class TestDiscretize:
    @given(arr=float_arrays(shape=integers(0, 10), min_value=-1.0, max_value=1.0))
    def test_1_bin(self, *, arr: NDArrayF1) -> None:
        result = discretize(arr, 1)
        expected = zeros_like(arr, dtype=float)
        assert_equal(result, expected)

    @given(
        arr=float_arrays(
            shape=integers(1, 10), min_value=-1.0, max_value=1.0, unique=True
        )
    )
    def test_2_bins(self, *, arr: NDArrayF1) -> None:
        _ = assume(len(arr) % 2 == 0)
        result = discretize(arr, 2)
        med = median(arr)
        is_below = (arr < med) & ~isclose(arr, med)
        assert isclose(result[is_below], 0.0).all()
        is_above = (arr > med) & ~isclose(arr, med)
        assert isclose(result[is_above], 1.0).all()

    @given(bins=integers(1, 10))
    def test_empty(self, *, bins: int) -> None:
        arr = array([], dtype=float)
        result = discretize(arr, bins)
        assert_equal(result, arr)

    @given(n=integers(0, 10), bins=integers(1, 10))
    def test_all_nan(self, *, n: int, bins: int) -> None:
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
        *,
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
    def test_main(self, data: DataObject, array: NDArrayF, halflife: float) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = ewma(array, halflife, axis=axis)


class TestExpMovingSum:
    @given(data=data(), array=float_arrays(), halflife=floats(0.1, 10.0))
    def test_main(self, data: DataObject, array: NDArrayF, halflife: float) -> None:
        axis = data.draw(integers(0, array.ndim - 1)) if array.ndim >= 1 else -1
        with assume_does_not_raise(RuntimeWarning):
            _ = exp_moving_sum(array, halflife, axis=axis)


class TestFFill:
    @mark.parametrize(("limit", "expected_v"), [param(None, 0.2), param(1, nan)])
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
                [[0.1, nan, nan, 0.2], [0.1, nan, nan, 0.2], [0.3, nan, nan, nan]],
            ),
            param(None, 1, [[0.1, 0.1, 0.1, 0.2], 4 * [nan], [0.3, 0.3, 0.3, nan]]),
            param(
                1, 0, [[0.1, nan, nan, 0.2], [0.1, nan, nan, 0.2], [0.3, nan, nan, nan]]
            ),
            param(1, 1, [[0.1, 0.1, nan, 0.2], 4 * [nan], [0.3, 0.3, nan, nan]]),
        ],
    )
    def test_main(
        self, *, limit: int | None, axis: int, expected_v: Sequence[Sequence[float]]
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
        self, *, axis: int, expected_v: Sequence[Sequence[float]]
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
    def test_main(self, *, init: float, value: float, expected_v: float) -> None:
        arr = array([init], dtype=float)
        result = fillna(arr, value=value)
        expected = array([expected_v], dtype=float)
        assert_equal(result, expected)


class TestFlatN0:
    @given(data=data(), n=integers(1, 10))
    def test_main(self, *, data: DataObject, n: int) -> None:
        i = data.draw(integers(0, n - 1))
        arr = arange(n) == i
        result = flatn0(arr)
        assert result == i

    def test_empty_error(self) -> None:
        with raises(FlatN0EmptyError, match=escape(r"Array [] must contain a True.")):
            _ = flatn0(zeros(0, dtype=bool))

    def test_multiple_error(self) -> None:
        with raises(
            FlatN0MultipleError,
            match=escape("Array [ True  True] must contain at most one True."),
        ):
            _ = flatn0(ones(2, dtype=bool))


class TestGetFillValue:
    @mark.parametrize(
        "dtype",
        [
            param(bool),
            param(bool_),
            param(datetime64D),
            param(datetime64Y),
            param(datetime64ns),
            param(float),
            param(float64),
            param(int),
            param(int64),
            param(object),
            param(object_),
        ],
    )
    def test_main(self, *, dtype: Any) -> None:
        fill_value = get_fill_value(dtype)
        array = full(0, fill_value, dtype=dtype)
        assert has_dtype(array, dtype)

    def test_error(self) -> None:
        with raises(GetFillValueError, match="Invalid data type; got 'invalid'"):
            _ = get_fill_value("invalid")


class TestHasDtype:
    @mark.parametrize(("dtype", "expected"), [param(float, True), param(int, False)])
    @mark.parametrize("is_tuple", [param(True), param(False)])
    def test_main(self, *, dtype: Any, is_tuple: bool, expected: bool) -> None:
        against = (dtype,) if is_tuple else dtype
        result = has_dtype(array([], dtype=float), against)
        assert result is expected

    @mark.parametrize(
        ("dtype", "against", "expected"),
        [
            param("Int64", "Int64", True),
            param("Int64", ("Int64",), True),
            param("Int64", int, False),
            param(DatetimeTZDtype(tz="UTC"), DatetimeTZDtype(tz="UTC"), True),
            param(
                DatetimeTZDtype(tz="UTC"), DatetimeTZDtype(tz="Asia/Hong_Kong"), False
            ),
        ],
    )
    def test_pandas(self, *, dtype: Any, against: Any, expected: bool) -> None:
        result = has_dtype(Series([], dtype=dtype), against)
        assert result is expected


class TestIsAtLeast:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_least(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_least_or_nan(nan, y)


class TestIsAtMost:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, True),
            param(0.0, 0.0, False, True),
            param(0.0, 1e-8, False, True),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_at_most(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_at_most_or_nan(nan, y)


class TestIsBetween:
    @mark.parametrize(
        ("x", "low", "high", "equal_nan", "expected"),
        [
            param(0.0, -1.0, -1.0, False, False),
            param(0.0, -1.0, 0.0, False, True),
            param(0.0, -1.0, 1.0, False, True),
            param(0.0, 0.0, -1.0, False, False),
            param(0.0, 0.0, 0.0, False, True),
            param(0.0, 0.0, 1.0, False, True),
            param(0.0, 1.0, -1.0, False, False),
            param(0.0, 1.0, 0.0, False, False),
            param(0.0, 1.0, 1.0, False, False),
            param(nan, -1.0, 1.0, False, False),
        ],
    )
    def test_main(
        self, *, x: float, low: float, high: float, equal_nan: bool, expected: bool
    ) -> None:
        assert is_between(x, low, high, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "low",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    @mark.parametrize(
        "high",
        [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)],
    )
    def test_nan(self, *, low: float, high: float) -> None:
        assert is_between_or_nan(nan, low, high)


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
        *,
        shape: int | tuple[int, ...],
        kind: Literal["shape", "array"],
        expected: Literal["empty", "non-empty"],
    ) -> None:
        shape_or_array = shape if kind == "shape" else zeros(shape, dtype=float)
        assert is_empty(shape_or_array) is (expected == "empty")
        assert is_non_empty(shape_or_array) is (expected == "non-empty")


class TestIsFiniteAndIntegral:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_integral_or_nan(nan)


class TestIsFiniteOrNan:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(0.0, True),
            param(1.0, True),
            param(inf, False),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_or_nan(x).item() is expected


class TestIsFiniteAndNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_negative_or_nan(nan)


class TestIsFiniteAndNonNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_negative_or_nan(nan)


class TestIsFiniteAndNonPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_positive_or_nan(nan)


class TestIsFiniteAndNonZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_non_zero_or_nan(nan)


class TestIsFiniteAndPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_finite_and_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_finite_and_positive_or_nan(nan)


class TestIsGreaterThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, True),
            param(0.0, -1.0, False, True),
            param(0.0, -1e-6, False, True),
            param(0.0, -1e-7, False, True),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, False),
            param(0.0, 1e-6, False, False),
            param(0.0, 1.0, False, False),
            param(0.0, inf, False, False),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_greater_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_greater_than_or_nan(nan, y)


class TestIsIntegral:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-2.0, True),
            param(-1.5, False),
            param(-1.0, True),
            param(-0.5, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(0.5, False),
            param(1.0, True),
            param(1.5, False),
            param(2.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_integral(x).item() is expected

    def test_nan(self) -> None:
        assert is_integral_or_nan(nan)


class TestIsLessThan:
    @mark.parametrize(
        ("x", "y", "equal_nan", "expected"),
        [
            param(0.0, -inf, False, False),
            param(0.0, -1.0, False, False),
            param(0.0, -1e-6, False, False),
            param(0.0, -1e-7, False, False),
            param(0.0, -1e-8, False, False),
            param(0.0, 0.0, False, False),
            param(0.0, 1e-8, False, False),
            param(0.0, 1e-7, False, True),
            param(0.0, 1e-6, False, True),
            param(0.0, 1.0, False, True),
            param(0.0, inf, False, True),
            param(0.0, nan, False, False),
            param(nan, nan, True, True),
        ],
    )
    def test_main(self, *, x: float, y: float, equal_nan: bool, expected: bool) -> None:
        assert is_less_than(x, y, equal_nan=equal_nan).item() is expected

    @mark.parametrize(
        "y", [param(-inf), param(-1.0), param(0.0), param(1.0), param(inf), param(nan)]
    )
    def test_nan(self, *, y: float) -> None:
        assert is_less_than_or_nan(nan, y)


class TestIsNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_negative_or_nan(nan)


class TestIsNonNegative:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_negative(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_negative_or_nan(nan)


class TestIsNonPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_positive_or_nan(nan)


class TestIsNonSingular:
    @mark.parametrize(
        ("array", "expected"), [param(eye(2), True), param(ones((2, 2)), False)]
    )
    @mark.parametrize("dtype", [param(float), param(int)])
    def test_main(self, *, array: NDArrayF2, dtype: Any, expected: bool) -> None:
        assert is_non_singular(array.astype(dtype)) is expected

    def test_overflow(self) -> None:
        arr = array([[0.0, 0.0], [5e-323, 0.0]], dtype=float)
        assert not is_non_singular(arr)


class TestIsNonZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_non_zero(x).item() is expected

    def test_nan(self) -> None:
        assert is_non_zero_or_nan(nan)


class TestIsPositive:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, False),
            param(0.0, False),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_positive(x).item() is expected

    def test_nan(self) -> None:
        assert is_positive_or_nan(nan)


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
    def test_overflow(self, *, array: NDArrayF2) -> None:
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


class TestIsZero:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, False),
            param(-1e-6, False),
            param(-1e-7, False),
            param(-1e-8, True),
            param(0.0, True),
            param(1e-8, True),
            param(1e-7, False),
            param(1e-6, False),
            param(1.0, False),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero(x).item() is expected

    def test_is_zero_or_nan(self) -> None:
        assert is_zero_or_nan(nan)


class TestIsZeroOrFiniteAndMicro:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, False),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, False),
            param(nan, False),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_finite_and_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_finite_and_non_micro_or_nan(nan)


class TestIsZeroOrNonMicro:
    @mark.parametrize(
        ("x", "expected"),
        [
            param(-inf, True),
            param(-1.0, True),
            param(-1e-6, True),
            param(-1e-7, True),
            param(-1e-8, False),
            param(0.0, True),
            param(1e-8, False),
            param(1e-7, True),
            param(1e-6, True),
            param(1.0, True),
            param(inf, True),
            param(nan, True),
        ],
    )
    def test_main(self, *, x: float, expected: bool) -> None:
        assert is_zero_or_non_micro(x).item() is expected

    def test_nan(self) -> None:
        assert is_zero_or_non_micro_or_nan(nan)


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
    def test_1d(self, n: int, expected_v: Sequence[float], dtype: type[Any]) -> None:
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
    def test_2d(self, axis: int, n: int, expected_v: Sequence[Sequence[float]]) -> None:
        arr = arange(10, 22, dtype=float).reshape((3, 4))
        result = pct_change(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_allclose(result, expected, atol=1e-4, equal_nan=True)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(PctChangeError, match="Shift must be non-zero"):
            _ = pct_change(arr, n=0)


class TestRedirectEmptyNumpyConcatenate:
    def test_main(self) -> None:
        with raises(EmptyNumpyConcatenateError), redirect_empty_numpy_concatenate():
            _ = concatenate([])


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
    def test_1d(self, *, n: int, expected_v: Sequence[float], dtype: type[Any]) -> None:
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
    def test_2d(
        self, *, axis: int, n: int, expected_v: Sequence[Sequence[float]]
    ) -> None:
        arr = arange(12, dtype=float).reshape((3, 4))
        result = shift(arr, axis=axis, n=n)
        expected = array(expected_v, dtype=float)
        assert_equal(result, expected)

    def test_error(self) -> None:
        arr = array([], dtype=float)
        with raises(ShiftError, match="Shift must be non-zero"):
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
    def test_scalar(self, *, date: dt.date) -> None:
        date64 = datetime64(date, "D")
        yr = year(date64)
        assert yr == date.year

    @given(date=dates())
    def test_array(self, *, date: dt.date) -> None:
        dates = array([date], dtype=datetime64D)
        years = year(dates)
        assert years.item() == date.year
