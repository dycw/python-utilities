from __future__ import annotations

import datetime as dt

from pytest import mark, param, raises

from utilities.datetime import UTC
from utilities.platform import (
    IS_NOT_MAC,
    IS_NOT_WINDOWS,
    SYSTEM,
    System,
    UnsupportedSystemError,
)
from utilities.xlrd import _get_date_mode, to_date, to_datetime


class TestGetDateMode:
    def test_main(self) -> None:
        if SYSTEM is System.linux:
            with raises(UnsupportedSystemError):
                _ = _get_date_mode()
        else:
            assert _get_date_mode() in {0, 1}


class TestToDate:
    @mark.parametrize(
        ("date", "expected"),
        [
            param(
                0.0,
                dt.date(1899, 12, 31),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                0.5,
                dt.date(1899, 12, 31),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                1.0,
                dt.date(1900, 1, 1),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                0.0,
                dt.date(1904, 1, 1),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
            param(
                0.5,
                dt.date(1904, 1, 1),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
            param(
                1.0,
                dt.date(1904, 1, 2),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
        ],
    )
    def test_main(self, *, date: float, expected: dt.date) -> None:
        assert to_date(date) == expected


class TestToDatetime:
    @mark.parametrize(
        ("date", "expected"),
        [
            param(
                0.0,
                dt.datetime(1899, 12, 31, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                0.5,
                dt.datetime(1899, 12, 31, 12, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                1.0,
                dt.datetime(1900, 1, 1, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_WINDOWS, reason="Windows only"),
            ),
            param(
                0.0,
                dt.datetime(1904, 1, 1, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
            param(
                0.5,
                dt.datetime(1904, 1, 1, 12, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
            param(
                1.0,
                dt.datetime(1904, 1, 2, tzinfo=UTC),
                marks=mark.skipif(IS_NOT_MAC, reason="Mac only"),
            ),
        ],
    )
    def test_main(self, *, date: float, expected: dt.datetime) -> None:
        assert to_datetime(date) == expected
