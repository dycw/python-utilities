from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

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
from pandas.testing import assert_index_equal, assert_series_equal
from pytest import mark, param, raises

from utilities.datetime import TODAY_UTC, UTC
from utilities.hypothesis import int_indexes, text_ascii, timestamps
from utilities.numpy import datetime64ns
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    DataFrameColumnsDuplicatedError,
    DataFrameColumnsError,
    DataFrameColumnsNameError,
    DataFrameDTypesError,
    DataFrameLengthError,
    DataFrameMaxLengthError,
    DataFrameMinLengthError,
    DataFrameRangeIndexError,
    DataFrameSortedError,
    DataFrameUniqueError,
    DifferentDTypeError,
    EmptyPandasConcatError,
    Int64,
    RangeIndexNameError,
    RangeIndexStartError,
    RangeIndexStepError,
    SeriesRangeIndexError,
    TimestampIsNaTError,
    _check_dataframe_length,
    astype,
    boolean,
    check_dataframe,
    check_range_index,
    redirect_to_empty_pandas_concat_error,
    rename_index,
    series_max,
    series_min,
    sort_index,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
    to_numpy,
)

if TYPE_CHECKING:  # pragma: no cover
    from utilities.pandas import IndexI, SeriesA


class TestAsType:
    def test_main(self) -> None:
        df = DataFrame(0, index=RangeIndex(1), columns=["value"], dtype=int)
        check_dataframe(df, dtypes={"value": int})
        result = astype(df, float)
        check_dataframe(result, dtypes={"value": float})


class TestCheckDataFrame:
    def test_main(self) -> None:
        df = DataFrame(index=RangeIndex(0))
        check_dataframe(df)

    def test_columns_name_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=Index(["value"], name="name"))
        with raises(DataFrameColumnsNameError):
            check_dataframe(df)

    def test_columns_duplicated_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=Index(["value", "value"]))
        with raises(DataFrameColumnsDuplicatedError):
            check_dataframe(df)

    def test_columns_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_dataframe(df, columns=["value"])

    def test_columns_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameColumnsError):
            check_dataframe(df, columns=["other"])

    def test_dtypes_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_dataframe(df, dtypes={"value": float})

    def test_dtypes_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameDTypesError):
            check_dataframe(df, dtypes={"value": int})

    def test_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        check_dataframe(df, length=1)

    def test_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameLengthError):
            check_dataframe(df, length=2)

    def test_min_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        check_dataframe(df, min_length=1)

    def test_min_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["value"])
        with raises(DataFrameMinLengthError):
            check_dataframe(df, min_length=1)

    def test_max_length_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(0), columns=["value"])
        check_dataframe(df, max_length=1)

    def test_max_length_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        with raises(DataFrameMaxLengthError):
            check_dataframe(df, max_length=1)

    def test_sorted_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=["value"])
        check_dataframe(df, sorted="value")

    def test_sorted_error(self) -> None:
        df = DataFrame([[1.0], [0.0]], index=RangeIndex(2), columns=["value"])
        with raises(DataFrameSortedError):
            check_dataframe(df, sorted="value")

    def test_unique_pass(self) -> None:
        df = DataFrame([[0.0], [1.0]], index=RangeIndex(2), columns=["value"])
        check_dataframe(df, unique="value")

    def test_unique_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(2), columns=["value"])
        with raises(DataFrameUniqueError):
            check_dataframe(df, unique="value")


class TestCheckDataFrameLength:
    def test_int_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        _check_dataframe_length(df, 1)

    def test_int_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(1), columns=["value"])
        with raises(DataFrameLengthError):
            _check_dataframe_length(df, 2)

    def test_tuple_pass(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(11), columns=["value"])
        _check_dataframe_length(df, (10, 0.1))

    def test_tuple_error(self) -> None:
        df = DataFrame(0.0, index=RangeIndex(12), columns=["value"])
        with raises(DataFrameLengthError):
            _check_dataframe_length(df, (10, 0.1))


class TestCheckRangeIndex:
    @given(index=range_indexes())
    def test_main(self, *, index: RangeIndex) -> None:
        check_range_index(index)

    def test_type(self) -> None:
        index = Index([], dtype=float)
        with raises(TypeError):
            check_range_index(index)

    @given(start=integers(-10, 10), stop=integers(-10, 10))
    def test_start(self, *, start: int, stop: int) -> None:
        _ = assume(start != 0)
        index = RangeIndex(start=start, stop=stop)
        with raises(RangeIndexStartError):
            check_range_index(index)

    @given(step=integers(-10, 10))
    def test_step(self, *, step: int) -> None:
        _ = assume(step not in {0, 1})
        index = RangeIndex(step=step)
        with raises(RangeIndexStepError):
            check_range_index(index)

    @given(index=range_indexes(name=text_ascii()))
    def test_name(self, *, index: RangeIndex) -> None:
        with raises(RangeIndexNameError):
            check_range_index(index)

    def test_series_pass(self) -> None:
        series = Series(index=RangeIndex(0), dtype=float)
        check_range_index(series)

    def test_series_fail(self) -> None:
        series = Series(index=Index([], dtype=int), dtype=float)
        with raises(SeriesRangeIndexError):
            check_range_index(series)

    def test_dataframe_pass(self) -> None:
        df = DataFrame(index=RangeIndex(0))
        check_range_index(df)

    def test_dataframe_fail(self) -> None:
        df = DataFrame(index=Index([], dtype=int))
        with raises(DataFrameRangeIndexError):
            check_range_index(df)


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
    def test_main(self, *, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)


class TestRedirectToEmptyPandasConcatError:
    def test_main(self) -> None:
        with raises(EmptyPandasConcatError):
            try:
                _ = concat([])
            except ValueError as error:
                redirect_to_empty_pandas_concat_error(error)


class TestRenameIndex:
    @given(index=int_indexes(), name=text_ascii())
    def test_main(self, *, index: IndexI, name: str) -> None:
        renamed = rename_index(index, name)
        assert renamed.name == name


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
    def test_main(
        self,
        *,
        x_v: Any,
        y_v: Any,
        dtype: Any,
        expected_min_v: Any,
        expected_max_v: Any,
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
    def test_different_index(
        self, *, func: Callable[[SeriesA, SeriesA], SeriesA]
    ) -> None:
        x = Series(data=nan, index=Index([0], dtype=int))
        y = Series(data=nan, index=Index([1], dtype=int))
        with raises(AssertionError):
            _ = func(x, y)

    @mark.parametrize("func", [param(series_min), param(series_max)])
    def test_different_dtype(
        self, *, func: Callable[[SeriesA, SeriesA], SeriesA]
    ) -> None:
        x = Series(data=nan, dtype=float)
        y = Series(data=NA, dtype=Int64)  # type: ignore
        with raises(DifferentDTypeError):
            _ = func(x, y)


class TestSortIndex:
    @given(index=int_indexes())
    def test_main(self, *, index: IndexI) -> None:
        sorted_ = sort_index(index)
        assert_index_equal(sorted_, cast(Any, index.sort_values()))


class TestTimestampMinMaxAsDate:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date - dt.timedelta(days=1)))

    def test_max(self) -> None:
        date = TIMESTAMP_MAX_AS_DATE
        assert isinstance(to_datetime(cast(Timestamp, date)), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(cast(Timestamp, date + dt.timedelta(days=1)))


class TestTimestampMinMaxAsDateTime:
    def test_min(self) -> None:
        date = TIMESTAMP_MIN_AS_DATETIME
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            _ = to_datetime(date - dt.timedelta(microseconds=1))

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
    def test_main(self, *, timestamp: Any, expected: dt.date) -> None:
        assert timestamp_to_date(timestamp) == expected

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
    def test_main(self, *, timestamp: Any, expected: dt.datetime) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    @given(timestamp=timestamps(allow_nanoseconds=True))
    def test_warn(self, *, timestamp: Timestamp) -> None:
        _ = assume(cast(Any, timestamp).nanosecond != 0)
        with raises(UserWarning, match="Discarding nonzero nanoseconds in conversion"):
            _ = timestamp_to_datetime(timestamp)

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
            param(TODAY_UTC, datetime64ns, TODAY_UTC, datetime64ns),
            param(0, int, 0, int),
            param(0, Int64, 0, object),
            param(NA, Int64, None, object),
            param(nan, float, nan, float),
            param("", string, "", object),
            param(NA, string, None, object),
        ],
    )
    def test_main(
        self, *, series_v: Any, series_d: Any, array_v: Any, array_d: Any
    ) -> None:
        series = Series([series_v], dtype=series_d)
        result = to_numpy(series)
        expected = array([array_v], dtype=array_d)
        assert_equal(result, expected)
