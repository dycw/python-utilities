import datetime as dt
from typing import Any
from typing import cast

from hypothesis import assume
from hypothesis import given
from hypothesis.strategies import dates
from hypothesis.strategies import timedeltas
from hypothesis.strategies import times
from pytest import mark
from pytest import raises
from typed_settings import settings
from typed_settings.exceptions import InvalidValueError

from utilities.typed_settings import load_settings


class TestTypedSettings:
    @given(date=dates())
    def test_date(self, date: dt.date) -> None:
        @settings(frozen=True)
        class Config:
            as_date: dt.date = date
            as_str: dt.date = cast(dt.date, date.isoformat())

        config = load_settings(Config, "appname")
        assert isinstance(config, Config)
        assert isinstance(config.as_date, dt.date)
        assert isinstance(config.as_str, dt.date)

    @given(time=times())
    def test_time(self, time: dt.time) -> None:
        @settings(frozen=True)
        class Config:
            as_time: dt.time = time
            as_str: dt.time = cast(dt.time, time.isoformat())

        config = load_settings(Config, "appname")
        assert isinstance(config, Config)
        assert isinstance(config.as_time, dt.time)
        assert isinstance(config.as_str, dt.time)

    @given(
        timedelta=timedeltas(
            min_value=dt.timedelta(0), max_value=dt.timedelta(days=1)
        )
    )
    def test_timedelta(self, timedelta: dt.timedelta) -> None:
        _ = assume(
            (timedelta < dt.timedelta(days=1)) and (timedelta.microseconds == 0)
        )

        @settings(frozen=True)
        class Config:
            as_timedelta: dt.timedelta = timedelta
            as_str: dt.timedelta = cast(dt.timedelta, str(timedelta))

        config = load_settings(Config, "appname")
        assert isinstance(config, Config)
        assert isinstance(config.as_timedelta, dt.timedelta)
        assert isinstance(config.as_str, dt.timedelta)

    @mark.parametrize("cls", [dt.date, dt.time, dt.timedelta])
    def test_errors(self, cls: Any) -> None:
        @settings(frozen=True)
        class Config:
            value: cls = cast(cls, None)

        with raises(InvalidValueError):
            _ = load_settings(Config, "appname")
