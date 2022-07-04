import datetime as dt
from typing import Any
from typing import cast

from pandas import NaT
from pandas import Series
from pandas import Timestamp
from pandas import to_datetime
from pytest import mark
from pytest import param
from pytest import raises

from utilities.pandas import TIMESTAMP_MAX_AS_DATE
from utilities.pandas import TIMESTAMP_MAX_AS_DATETIME
from utilities.pandas import TIMESTAMP_MIN_AS_DATE
from utilities.pandas import TIMESTAMP_MIN_AS_DATETIME
from utilities.pandas import Int64
from utilities.pandas import boolean
from utilities.pandas import string
from utilities.pandas import timestamp_to_date
from utilities.pandas import timestamp_to_datetime


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
        "timestamp, expected",
        [
            param(to_datetime("2000-01-01"), dt.date(2000, 1, 1)),
            param(to_datetime("2000-01-01 12:00:00"), dt.date(2000, 1, 1)),
        ],
    )
    def test_main(self, timestamp: Any, expected: dt.date) -> None:
        assert timestamp_to_date(timestamp) == expected

    def test_error(self) -> None:
        with raises(ValueError, match="Invalid value"):
            _ = timestamp_to_date(NaT)


class TestTimestampToDateTime:
    @mark.parametrize(
        "timestamp, expected",
        [
            param(to_datetime("2000-01-01"), dt.datetime(2000, 1, 1)),
            param(
                to_datetime("2000-01-01 12:00:00"), dt.datetime(2000, 1, 1, 12)
            ),
        ],
    )
    def test_main(self, timestamp: Any, expected: dt.datetime) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    def test_error(self) -> None:
        with raises(ValueError, match="Invalid value"):
            _ = timestamp_to_datetime(NaT)
