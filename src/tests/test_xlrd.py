from __future__ import annotations

import datetime as dt

import pytest

from utilities.datetime import UTC
from utilities.platform import SYSTEM, System
from utilities.pytest import skipif_not_mac, skipif_not_windows
from utilities.xlrd import GetDateModeError, get_date_mode, to_date, to_datetime


class TestGetDateMode:
    def test_main(self) -> None:
        if SYSTEM is System.linux:
            with pytest.raises(GetDateModeError):
                _ = get_date_mode()
        else:
            assert get_date_mode() in {0, 1}


class TestToDate:
    @pytest.mark.parametrize(
        ("date", "expected"),
        [
            pytest.param(0.0, dt.date(1899, 12, 31), marks=skipif_not_windows),
            pytest.param(0.5, dt.date(1899, 12, 31), marks=skipif_not_windows),
            pytest.param(1.0, dt.date(1900, 1, 1), marks=skipif_not_windows),
            pytest.param(0.0, dt.date(1904, 1, 1), marks=skipif_not_mac),
            pytest.param(0.5, dt.date(1904, 1, 1), marks=skipif_not_mac),
            pytest.param(1.0, dt.date(1904, 1, 2), marks=skipif_not_mac),
        ],
    )
    def test_main(self, *, date: float, expected: dt.date) -> None:
        assert to_date(date) == expected


class TestToDatetime:
    @pytest.mark.parametrize(
        ("date", "expected"),
        [
            pytest.param(
                0.0, dt.datetime(1899, 12, 31, tzinfo=UTC), marks=skipif_not_windows
            ),
            pytest.param(
                0.5, dt.datetime(1899, 12, 31, 12, tzinfo=UTC), marks=skipif_not_windows
            ),
            pytest.param(
                1.0, dt.datetime(1900, 1, 1, tzinfo=UTC), marks=skipif_not_windows
            ),
            pytest.param(
                0.0, dt.datetime(1904, 1, 1, tzinfo=UTC), marks=skipif_not_mac
            ),
            pytest.param(
                0.5, dt.datetime(1904, 1, 1, 12, tzinfo=UTC), marks=skipif_not_mac
            ),
            pytest.param(
                1.0, dt.datetime(1904, 1, 2, tzinfo=UTC), marks=skipif_not_mac
            ),
        ],
    )
    def test_main(self, *, date: float, expected: dt.datetime) -> None:
        assert to_datetime(date) == expected
