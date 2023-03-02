import datetime as dt
from typing import Any, cast

from hypothesis import assume, given
from hypothesis.extra.pandas import range_indexes
from hypothesis.strategies import integers
from pandas import DataFrame, Index, NaT, RangeIndex, Series, Timestamp, to_datetime
from pytest import mark, param, raises

from utilities.datetime import UTC
from utilities.hypothesis import text_ascii
from utilities.hypothesis.pandas import timestamps
from utilities.pandas import (
    TIMESTAMP_MAX_AS_DATE,
    TIMESTAMP_MAX_AS_DATETIME,
    TIMESTAMP_MIN_AS_DATE,
    TIMESTAMP_MIN_AS_DATETIME,
    DataFrameRangeIndexError,
    Int64,
    RangeIndexNameError,
    RangeIndexStartError,
    RangeIndexStepError,
    SeriesRangeIndexError,
    TimestampIsNaTError,
    boolean,
    check_range_index,
    string,
    timestamp_to_date,
    timestamp_to_datetime,
)


class TestCheckRangeIndex:
    @given(index=range_indexes())
    def test_main(self, index: RangeIndex) -> None:
        check_range_index(index)

    def test_type(self) -> None:
        index = Index([], dtype=float)
        with raises(TypeError):
            check_range_index(index)

    @given(start=integers(-10, 10), stop=integers(-10, 10))
    def test_start(self, start: int, stop: int) -> None:
        _ = assume(start != 0)
        index = RangeIndex(start=start, stop=stop)
        with raises(RangeIndexStartError):
            check_range_index(index)

    @given(step=integers(-10, 10))
    def test_step(self, step: int) -> None:
        _ = assume(step not in {0, 1})
        index = RangeIndex(step=step)
        with raises(RangeIndexStepError):
            check_range_index(index)

    @given(index=range_indexes(name=text_ascii()))
    def test_name(self, index: RangeIndex) -> None:
        with raises(RangeIndexNameError):
            check_range_index(index)

    def test_series_pass(self) -> None:
        series = Series(index=RangeIndex(0), dtype=float)
        check_range_index(series)

    def test_series_fail(self) -> None:
        series = Series(dtype=float)
        with raises(SeriesRangeIndexError):
            check_range_index(series)

    def test_dataframe_pass(self) -> None:
        df = DataFrame(index=RangeIndex(0))
        check_range_index(df)

    def test_dataframe_fail(self) -> None:
        df = DataFrame()
        with raises(DataFrameRangeIndexError):
            check_range_index(df)


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
    def test_main(self, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)


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
    def test_main(self, timestamp: Any, expected: dt.date) -> None:
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
    def test_main(self, timestamp: Any, expected: dt.datetime) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    @given(timestamp=timestamps(allow_nanoseconds=True))
    def test_warn(self, timestamp: Timestamp) -> None:
        _ = assume(cast(Any, timestamp).nanosecond != 0)
        with raises(UserWarning, match="Discarding nonzero nanoseconds in conversion"):
            _ = timestamp_to_datetime(timestamp)

    def test_error(self) -> None:
        with raises(TimestampIsNaTError):
            _ = timestamp_to_datetime(NaT)
