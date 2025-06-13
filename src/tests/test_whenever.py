from __future__ import annotations

import datetime as dt
from re import escape

from hypothesis import given
from hypothesis.strategies import sampled_from
from pytest import raises

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from utilities.datetime import MICROSECOND
from utilities.tzdata import HongKong
from utilities.whenever import (
    _CheckValidZonedDateTimeUnequalError,
    check_valid_zoned_datetime,
)

_TIMEDELTA_MICROSECONDS = int(1e18) * MICROSECOND
_TIMEDELTA_OVERFLOW = dt.timedelta(days=106751991, seconds=14454, microseconds=775808)


@SKIPIF_CI_AND_WINDOWS
class TestCheckValidZonedDateTime:
    @given(
        datetime=sampled_from([
            dt.datetime(1951, 4, 1, 3, tzinfo=HongKong),
            dt.datetime(1951, 4, 1, 5, tzinfo=HongKong),
        ])
    )
    def test_main(self, *, datetime: dt.datetime) -> None:
        check_valid_zoned_datetime(datetime)

    def test_error(self) -> None:
        datetime = dt.datetime(1951, 4, 1, 4, tzinfo=HongKong)
        with raises(
            _CheckValidZonedDateTimeUnequalError,
            match=escape(
                "Zoned datetime must be valid; got 1951-04-01 04:00:00+08:00 != 1951-04-01 05:00:00+09:00"
            ),
        ):
            check_valid_zoned_datetime(datetime)
