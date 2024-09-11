from __future__ import annotations

import datetime as dt
from enum import Enum, StrEnum, auto
from typing import TYPE_CHECKING, Any

from polars import int_range
from pytest import mark, param

from utilities.functions import identity
from utilities.reprlib import custom_print, custom_repr
from utilities.zoneinfo import UTC, HongKong

if TYPE_CHECKING:
    from collections.abc import Mapping


class TestCustomPrint:
    def test_main(self) -> None:
        custom_print({})


class TestCustomRepr:
    @mark.parametrize(
        ("mapping", "expected"),
        [
            param([], "[]"),
            param([1], "[1]"),
            param([1, 2], "[1, 2]"),
            param([1, 2, 3], "[1, 2, 3]"),
            param([1, 2, 3, 4], "[1, 2, 3, 4]"),
            param([1, 2, 3, 4, 5], "[1, 2, 3, 4, 5]"),
            param([1, 2, 3, 4, 5, 6], "[1, 2, 3, 4, 5, 6]"),
            param([1, 2, 3, 4, 5, 6, 7], "[1, 2, 3, 4, 5, 6, ...]"),
            param([1, 2, 3, 4, 5, 6, 7, 8], "[1, 2, 3, 4, 5, 6, ...]"),
            param({}, ""),
            param({"a": 1}, "a=1"),
            param({"a": 1, "b": 2}, "a=1, b=2"),
            param({"a": 1, "b": 2, "c": 3}, "a=1, b=2, c=3"),
            param({"a": 1, "b": 2, "c": 3, "d": 4}, "a=1, b=2, c=3, d=4"),
            param({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}, "a=1, b=2, c=3, d=4, ..."),
            param(
                {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6},
                "a=1, b=2, c=3, d=4, ...",
            ),
        ],
    )
    def test_main(self, *, mapping: Mapping[str, Any], expected: str) -> None:
        result = custom_repr(mapping)
        assert result == expected

    def test_class(self) -> None:
        class Truth(Enum): ...

        result = custom_repr(Truth)
        expected = "Truth"
        assert result == expected

    def test_dataframe(self) -> None:
        df = int_range(start=0, end=100, eager=True).rename("int").to_frame()
        result = custom_repr(df)
        expected = repr(df)
        assert result == expected

    def test_dataframe_fake(self) -> None:
        class DataFrame: ...

        _ = custom_repr(DataFrame())

    def test_date(self) -> None:
        result = custom_repr(dt.date(2000, 1, 1))
        expected = "2000-01-01"
        assert result == expected

    def test_datetime_local(self) -> None:
        result = custom_repr(dt.datetime(2000, 1, 1, tzinfo=UTC).replace(tzinfo=None))
        expected = "2000-01-01T00:00:00"
        assert result == expected

    def test_datetime_zoned(self) -> None:
        result = custom_repr(dt.datetime(2000, 1, 1, tzinfo=UTC))
        expected = "2000-01-01T00:00:00+00:00[UTC]"
        assert result == expected

    def test_enum_generic(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        result = custom_repr(list(Truth))
        expected = "['Truth.true', 'Truth.false']"
        assert result == expected

    def test_enum_str(self) -> None:
        class Truth(StrEnum):
            true_key = "true_value"
            false_key = "false_value"

        result = custom_repr(list(Truth))
        expected = "['true_value', 'false_value']"
        assert result == expected

    def test_function(self) -> None:
        result = custom_repr(identity)
        expected = "identity"
        assert result == expected

    def test_object(self) -> None:
        obj = object()
        result = custom_repr(obj)
        expected = "<object object>"
        assert result == expected

    def test_series(self) -> None:
        sr = int_range(start=0, end=100, eager=True).rename("int")
        result = custom_repr(sr)
        expected = repr(sr)
        assert result == expected

    def test_series_fake(self) -> None:
        class Series: ...

        _ = custom_repr(Series())

    def test_time(self) -> None:
        result = custom_repr(dt.time(0))
        expected = "00:00:00"
        assert result == expected

    def test_timedelta(self) -> None:
        result = custom_repr(dt.timedelta(0))
        expected = "P0D"
        assert result == expected

    def test_zone_info(self) -> None:
        result = custom_repr(HongKong)
        expected = "Asia/Hong_Kong"
        assert result == expected
