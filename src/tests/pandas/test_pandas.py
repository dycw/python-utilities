import datetime as dt
from typing import Any, Callable, cast

from beartype import beartype
from hypothesis import assume, given
from hypothesis.extra.pandas import range_indexes
from hypothesis.strategies import integers
from numpy import array, nan
from numpy.testing import assert_equal
from pandas import (
    NA,
    DataFrame,
    Index,
    NaT,
    RangeIndex,
    Series,
    Timestamp,
    concat,
    to_datetime,
)
from pandas.testing import assert_series_equal
from pytest import mark, param, raises

from utilities.datetime import TODAY, UTC
from utilities.hypothesis import text_ascii
from utilities.hypothesis.pandas import timestamps
from utilities.numpy import datetime64ns
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    DataFrameColumnsError,
    DataFrameColumnsNameError,
    DataFrameDTypesError,
    DataFrameRangeIndexError,
    DifferentDTypeError,
    EmptyPandasConcatError,
    Int64,
    RangeIndexNameError,
    RangeIndexStartError,
    RangeIndexStepError,
    SeriesRangeIndexError,
    TimestampIsNaTError,
    boolean,
    check_dataframe,
    check_range_index,
    redirect_to_empty_pandas_concat_error,
    series_max,
    series_min,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
    to_numpy,
)


class TestCheckDataFrame:
    @beartype
    def test_main(self) -> None:
        df = DataFrame(index=RangeIndex(0))
        check_dataframe(df)

    @beartype
    def test_columns_name_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=Index(["value"], name="name"))
        with raises(DataFrameColumnsNameError):
            check_dataframe(df)

    @beartype
    def test_columns_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_dataframe(df, columns=["value"])

    @beartype
    def test_columns_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameColumnsError):
            check_dataframe(df, columns=["other"])

    @beartype
    def test_dtypes_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_dataframe(df, dtypes={"value": float})

    @beartype
    def test_dtypes_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameDTypesError):
            check_dataframe(df, dtypes={"value": int})


class TestCheckRangeIndex:
    @given(index=range_indexes())
    @beartype
    def test_main(self, index: RangeIndex) -> None:
        check_range_index(index)

    @beartype
    def test_type(self) -> None:
        index = Index([], dtype=float)
        with raises(TypeError):
            check_range_index(index)

    @given(start=integers(-10, 10), stop=integers(-10, 10))
    @beartype
    def test_start(self, start: int, stop: int) -> None:
        _ = assume(start != 0)
        index = RangeIndex(start=start, stop=stop)
        with raises(RangeIndexStartError):
            check_range_index(index)

    @given(step=integers(-10, 10))
    @beartype
    def test_step(self, step: int) -> None:
        _ = assume(step not in {0, 1})
        index = RangeIndex(step=step)
        with raises(RangeIndexStepError):
            check_range_index(index)

    @given(index=range_indexes(name=text_ascii()))
    @beartype
    def test_name(self, index: RangeIndex) -> None:
        with raises(RangeIndexNameError):
            check_range_index(index)

    @beartype
    def test_series_pass(self) -> None:
        series = Series(index=RangeIndex(0), dtype=float)
        check_range_index(series)

    @beartype
    def test_series_fail(self) -> None:
        series = Series(dtype=float)
        with raises(SeriesRangeIndexError):
            check_range_index(series)

    @beartype
    def test_dataframe_pass(self) -> None:
        df = DataFrame(index=RangeIndex(0))
        check_range_index(df)

    @beartype
    def test_dataframe_fail(self) -> None:
        df = DataFrame()
        with raises(DataFrameRangeIndexError):
            check_range_index(df)


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
    @beartype
    def test_main(self, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)


class TestRedirectToEmptyPandasConcatError:
    @beartype
    def test_main(self) -> None:
        with raises(EmptyPandasConcatError):
            try:
                _ = concat([])
            except ValueError as error:
                redirect_to_empty_pandas_concat_error(error)


class TestSeriesMinMax:
    @mark.parametrize(
        ("x_v", "y_v", "dtype", "expected_min_v", "expected_max_v"),
        [
            param(0.0, 1.0, float, 0.0, 1.0),
            param(0.0, nan, float, 0.0, 0.0),
            param(nan, 1.0, float, 1.0, 1.0),
            param(nan, nan, float, nan, nan),
            param(0, 1, Int64, 0, 1),
            param(0, NA, Int64, 0, 0),
            param(NA, 1, Int64, 1, 1),
            param(NA, NA, Int64, NA, NA),
            param(
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            param(
                TIMESTAMP_MIN_AS_DATE,
                NaT,
                datetime64ns,
                TIMESTAMP_MIN_AS_DATE,
                TIMESTAMP_MIN_AS_DATE,
            ),
            param(
                NaT,
                TIMESTAMP_MAX_AS_DATE,
                datetime64ns,
                TIMESTAMP_MAX_AS_DATE,
                TIMESTAMP_MAX_AS_DATE,
            ),
            param(NaT, NaT, datetime64ns, NaT, NaT),
        ],
    )
    @beartype
    def test_main(
        self, x_v: Any, y_v: Any, dtype: Any, expected_min_v: Any, expected_max_v: Any
    ) -> None:
        x = Series(data=[x_v], dtype=dtype)
        y = Series(data=[y_v], dtype=dtype)
        result_min = series_min(x, y)
        expected_min = Series(data=[expected_min_v], dtype=dtype)
        assert_series_equal(result_min, expected_min)
        result_max = series_max(x, y)
        expected_max = Series(data=[expected_max_v], dtype=dtype)
        assert_series_equal(result_max, expected_max)

    @mark.parametrize("func", [param(series_min), param(series_max)])
    @beartype
    def test_different_index(self, func: Callable[[Series, Series], Series]) -> None:
        x = Series(data=nan, index=Index([0], dtype=int))
        y = Series(data=nan, index=Index([1], dtype=int))
        with raises(AssertionError):
            _ = func(x, y)

    @mark.parametrize("func", [param(series_min), param(series_max)])
    @beartype
    def test_different_dtype(self, func: Callable[[Series, Series], Series]) -> None:
        x = Series(data=nan, dtype=float)
        y = Series(data=NA, dtype=Int64)
        with raises(DifferentDTypeError):
            _ = func(x, y)


class TestTimestampMinMaxAsDate:
    @beartype
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date - dt.timedelta(days=1)))

    @beartype
    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date + dt.timedelta(days=1)))


class TestTimestampMinMaxAsDateTime:
    @beartype
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date - dt.timedelta(microseconds=1))

    @beartype
    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date + dt.timedelta(microseconds=1))


class TestTimestampToDate:
    @mark.parametrize(
        ("timestamp", "expected"),
        [
            param(to_datetime("2000-01-01"), dt.date(2000, 1, 1)),
            param(to_datetime("2000-01-01 12:00:00"), dt.date(2000, 1, 1)),
        ],
    )
    @beartype
    def test_main(self, timestamp: Any, expected: dt.date) -> None:
        assert timestamp_to_date(timestamp) == expected

    @beartype
    def test_error(self) -> None:
        with raises(TimestampIsNaTError):
            _ = timestamp_to_date(NaT)


class TestTimestampToDateTime:
    @mark.parametrize(
        ("timestamp", "expected"),
        [
            param(to_datetime("2000-01-01"), dt.datetime(2000, 1, 1, tzinfo=UTC)),
            param(
                to_datetime("2000-01-01 12:00:00"),
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC),
            ),
            param(
                to_datetime("2000-01-01 12:00:00+00:00"),
                dt.datetime(2000, 1, 1, 12, tzinfo=UTC),
            ),
        ],
    )
    @beartype
    def test_main(self, timestamp: Any, expected: dt.datetime) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    @given(timestamp=timestamps(allow_nanoseconds=True))
    @beartype
    def test_warn(self, timestamp: Timestamp) -> None:
        _ = assume(cast(Any, timestamp).nanosecond != 0)
        with raises(UserWarning, match="Discarding nonzero nanoseconds in conversion"):
            _ = timestamp_to_datetime(timestamp)

    @beartype
    def test_error(self) -> None:
        with raises(TimestampIsNaTError):
            _ = timestamp_to_datetime(NaT)


class TestToNumpy:
    @mark.parametrize(
        ("series_v", "series_d", "array_v", "array_d"),
        [
            param(True, bool, True, bool),
            param(False, bool, False, bool),
            param(True, boolean, True, object),
            param(False, boolean, False, object),
            param(NA, boolean, None, object),
            param(TODAY, datetime64ns, TODAY, datetime64ns),
            param(0, int, 0, int),
            param(0, Int64, 0, object),
            param(NA, Int64, None, object),
            param(nan, float, nan, float),
            param("", string, "", object),
            param(NA, string, None, object),
        ],
    )
    @beartype
    def test_main(
        self, series_v: Any, series_d: Any, array_v: Any, array_d: Any
    ) -> None:
        series = Series([series_v], dtype=series_d)
        result = to_numpy(series)
        expected = array([array_v], dtype=array_d)
        assert_equal(result, expected)
