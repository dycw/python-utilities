import datetime as dt
from typing import Any
from typing import Optional

from pandas import NaT
from pandas import Series
from pandas import Timestamp
from pandas import to_datetime
from pytest import mark
from pytest import param
from pytest import raises

from dycw_utilities.pandas import Int64
from dycw_utilities.pandas import boolean
from dycw_utilities.pandas import string
from dycw_utilities.pandas import timestamp_max_as_date
from dycw_utilities.pandas import timestamp_max_as_datetime
from dycw_utilities.pandas import timestamp_min_as_date
from dycw_utilities.pandas import timestamp_min_as_datetime
from dycw_utilities.pandas import timestamp_to_date
from dycw_utilities.pandas import timestamp_to_datetime


class TestDTypes:
    @mark.parametrize("dtype", [param(Int64), param(boolean), param(string)])
    def test_main(self, dtype: Any) -> None:
        assert isinstance(Series([], dtype=dtype), Series)


class TestGetMinMaxTimestampAsDate:
    def test_min(self) -> None:
        date = timestamp_min_as_date()
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            to_datetime(date - dt.timedelta(days=1))

    def test_max(self) -> None:
        date = timestamp_max_as_date()
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            to_datetime(date + dt.timedelta(days=1))


class TestGetMinMaxTimestampAsDateTime:
    def test_min(self) -> None:
        date = timestamp_min_as_datetime()
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            to_datetime(date - dt.timedelta(microseconds=1))

    def test_max(self) -> None:
        date = timestamp_max_as_datetime()
        assert isinstance(to_datetime(date), Timestamp)
        with raises(ValueError, match="Out of bounds nanosecond timestamp"):
            to_datetime(date + dt.timedelta(microseconds=1))


class TestTimestampToDate:
    @mark.parametrize(
        "timestamp, expected",
        [
            param(to_datetime("2000-01-01"), dt.date(2000, 1, 1)),
            param(to_datetime("2000-01-01 12:00:00"), dt.date(2000, 1, 1)),
            param(NaT, None),
        ],
    )
    def test_main(self, timestamp: Any, expected: Optional[dt.date]) -> None:
        assert timestamp_to_date(timestamp) == expected


class TestTimestampToDateTime:
    @mark.parametrize(
        "timestamp, expected",
        [
            param(to_datetime("2000-01-01"), dt.datetime(2000, 1, 1)),
            param(
                to_datetime("2000-01-01 12:00:00"), dt.datetime(2000, 1, 1, 12)
            ),
            param(NaT, None),
        ],
    )
    def test_main(self, timestamp: Any, expected: Optional[dt.date]) -> None:
        assert timestamp_to_datetime(timestamp) == expected

    def test_error(self) -> None:
        with raises(TypeError, match="Invalid type: 'str'"):
            _ = timestamp_to_datetime("error")
