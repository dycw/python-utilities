from __future__ import annotations

import datetime as dt

from pytest import mark, param, raises

from utilities.platform import SYSTEM, System
from utilities.pytest import skipif_not_mac, skipif_not_windows
from utilities.xlrd import GetDateModeError, get_date_mode, to_date, to_datetime
from utilities.zoneinfo import UTC


class TestGetDateMode:
    def test_main(self) -> None:
        if SYSTEM is System.linux:
            with raises(GetDateModeError):
                _ = get_date_mode()
        else:
            assert get_date_mode() in {0, 1}


class TestToDate:
    @mark.parametrize(
        ("date", "expected"),
        [
            param(0.0, dt.date(1899, 12, 31), marks=skipif_not_windows),
            param(0.5, dt.date(1899, 12, 31), marks=skipif_not_windows),
            param(1.0, dt.date(1900, 1, 1), marks=skipif_not_windows),
            param(0.0, dt.date(1904, 1, 1), marks=skipif_not_mac),
            param(0.5, dt.date(1904, 1, 1), marks=skipif_not_mac),
            param(1.0, dt.date(1904, 1, 2), marks=skipif_not_mac),
        ],
    )
    def test_main(self, *, date: float, expected: dt.date) -> None:
        assert to_date(date) == expected


class TestToDatetime:
    @mark.parametrize(
        ("date", "expected"),
        [
            param(0.0, dt.datetime(1899, 12, 31, tzinfo=UTC), marks=skipif_not_windows),
            param(
                0.5, dt.datetime(1899, 12, 31, 12, tzinfo=UTC), marks=skipif_not_windows
            ),
            param(1.0, dt.datetime(1900, 1, 1, tzinfo=UTC), marks=skipif_not_windows),
            param(0.0, dt.datetime(1904, 1, 1, tzinfo=UTC), marks=skipif_not_mac),
            param(0.5, dt.datetime(1904, 1, 1, 12, tzinfo=UTC), marks=skipif_not_mac),
            param(1.0, dt.datetime(1904, 1, 2, tzinfo=UTC), marks=skipif_not_mac),
        ],
    )
    def test_main(self, *, date: float, expected: dt.datetime) -> None:
        assert to_datetime(date) == expected
