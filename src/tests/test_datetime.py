import datetime as dt
from collections.abc import Callable
from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import SearchStrategy
from hypothesis.strategies import data
from hypothesis.strategies import dates
from hypothesis.strategies import datetimes
from hypothesis.strategies import sampled_from
from hypothesis.strategies import timedeltas
from hypothesis.strategies import times
from pytest import mark
from pytest import param
from pytest import raises

from utilities.datetime import InvalidDate
from utilities.datetime import InvalidDateTime
from utilities.datetime import InvalidTime
from utilities.datetime import InvalidTimedelta
from utilities.datetime import ensure_date
from utilities.datetime import ensure_datetime
from utilities.datetime import ensure_time
from utilities.datetime import ensure_timedelta
from utilities.datetime import parse_date
from utilities.datetime import parse_datetime
from utilities.datetime import parse_time
from utilities.datetime import parse_timedelta
from utilities.datetime import serialize_date
from utilities.datetime import serialize_datetime
from utilities.datetime import serialize_time
from utilities.datetime import serialize_timedelta


class TestEnsure:
    @given(data=data())
    @mark.parametrize(
        ["strategy", "func"],
        [
            param(dates(), ensure_date),
            param(datetimes(), ensure_datetime),
            param(times(), ensure_time),
            param(timedeltas(), ensure_timedelta),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        strategy: SearchStrategy[Any],
        func: Callable[[Any], Any],
    ) -> None:
        value = data.draw(strategy)
        input = data.draw(sampled_from([value, str(value)]))
        result = func(input)
        assert result == value


class TestParseDate:
    @given(date=dates())
    def test_str(self, date: dt.date) -> None:
        result = parse_date(str(date))
        assert result == date

    @given(date=dates())
    def test_isoformat(self, date: dt.date) -> None:
        result = parse_date(date.isoformat())
        assert result == date

    @given(date=dates())
    def test_yyyymmdd(self, date: dt.date) -> None:
        result = parse_date(date.strftime("%4Y%m%d"))
        assert result == date

    def test_error(self) -> None:
        with raises(InvalidDate):
            _ = parse_date("error")


class TestParseDateTime:
    @given(datetime=datetimes())
    def test_str(self, datetime: dt.datetime) -> None:
        result = parse_datetime(str(datetime))
        assert result == datetime

    @given(datetime=datetimes())
    def test_isoformat(self, datetime: dt.datetime) -> None:
        result = parse_datetime(datetime.isoformat())
        assert result == datetime

    @given(
        datetime=datetimes(),
        format=sampled_from(["%4Y%m%dT%H%M%S.%f", "%4Y-%m-%d %H:%M:%S.%f"]),
    )
    def test_yyyymmdd_hhmmss_fff(
        self, datetime: dt.datetime, format: str
    ) -> None:
        result = parse_datetime(datetime.strftime(format))
        assert result == datetime

    @given(
        datetime=datetimes(),
        format=sampled_from(
            ["%4Y%m%dT%H%M%S", "%4Y-%m-%d %H:%M:%S", "%4Y-%m-%dT%H:%M:%S"]
        ),
    )
    def test_yyyymmdd_hhmmss(self, datetime: dt.datetime, format: str) -> None:
        datetime = datetime.replace(microsecond=0)
        result = parse_datetime(datetime.strftime(format))
        assert result == datetime

    @given(
        datetime=datetimes(),
        format=sampled_from(
            ["%4Y%m%dT%H%M", "%4Y-%m-%d %H:%M", "%4Y-%m-%dT%H:%M"]
        ),
    )
    def test_yyyymmdd_hhmm(self, datetime: dt.datetime, format: str) -> None:
        datetime = datetime.replace(second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(format))
        assert result == datetime

    @given(
        datetime=datetimes(),
        format=sampled_from(["%4Y%m%dT%H", "%4Y-%m-%d %H", "%4Y-%m-%dT%H"]),
    )
    def test_yyyymmdd_hh(self, datetime: dt.datetime, format: str) -> None:
        datetime = datetime.replace(minute=0, second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(format))
        assert result == datetime

    @given(datetime=datetimes(), format=sampled_from(["%4Y%m%d", "%4Y-%m-%d"]))
    def test_yyyymmdd(self, datetime: dt.datetime, format: str) -> None:
        datetime = datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        result = parse_datetime(datetime.strftime(format))
        assert result == datetime

    def test_error(self) -> None:
        with raises(InvalidDateTime):
            _ = parse_datetime("error")


class TestParseTime:
    @given(time=times())
    def test_str(self, time: dt.time) -> None:
        result = parse_time(str(time))
        assert result == time

    @given(time=times())
    def test_isoformat(self, time: dt.time) -> None:
        result = parse_time(time.isoformat())
        assert result == time

    @given(time=times(), format=sampled_from(["%H%M%S.%f", "%H:%M:%S.%f"]))
    def test_hhmmss_fff(self, time: dt.time, format: str) -> None:
        result = parse_time(time.strftime(format))
        assert result == time

    @given(time=times(), format=sampled_from(["%H%M%S", "%H:%M:%S"]))
    def test_hhmmss(self, time: dt.time, format: str) -> None:
        time = time.replace(microsecond=0)
        result = parse_time(time.strftime(format))
        assert result == time

    @given(time=times(), format=sampled_from(["%H%M", "%H:%M"]))
    def test_hhmm(self, time: dt.time, format: str) -> None:
        time = time.replace(second=0, microsecond=0)
        result = parse_time(time.strftime(format))
        assert result == time

    @given(time=times(), format=sampled_from(["%H", "%H"]))
    def test_hh(self, time: dt.time, format: str) -> None:
        time = time.replace(minute=0, second=0, microsecond=0)
        result = parse_time(time.strftime(format))
        assert result == time

    def test_error(self) -> None:
        with raises(InvalidTime):
            _ = parse_time("error")


class TestParseTimedelta:
    @given(timedelta=timedeltas())
    def test_main(self, timedelta: dt.timedelta) -> None:
        result = parse_timedelta(str(timedelta))
        assert result == timedelta

    def test_error(self) -> None:
        with raises(InvalidTimedelta):
            _ = parse_timedelta("error")


class TestSerialize:
    @given(data=data())
    @mark.parametrize(
        ["strategy", "serialize", "parse"],
        [
            param(dates(), serialize_date, parse_date),
            param(datetimes(), serialize_datetime, parse_datetime),
            param(times(), serialize_time, parse_time),
            param(timedeltas(), str, parse_timedelta),
            param(timedeltas(), serialize_timedelta, parse_timedelta),
        ],
    )
    def test_main(
        self,
        data: DataObject,
        strategy: SearchStrategy[Any],
        serialize: Callable[[Any], Any],
        parse: Callable[[Any], Any],
    ) -> None:
        value = data.draw(strategy)
        result = parse(serialize(value))
        assert result == value
