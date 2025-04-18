from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from types import NoneType
from typing import Literal

from hypothesis import given
from hypothesis.strategies import booleans, dates, floats, integers, sampled_from, times
from pytest import raises

from tests.test_operator import TruthEnum
from utilities.hypothesis import (
    local_datetimes,
    paths,
    text_ascii,
    timedeltas_2w,
    versions,
    zoned_datetimes,
)
from utilities.math import is_equal
from utilities.parse import ParseTextError, parse_text
from utilities.sentinel import Sentinel, sentinel
from utilities.version import Version
from utilities.whenever import (
    serialize_date,
    serialize_datetime,
    serialize_time,
    serialize_timedelta,
)


class TestParseText:
    @given(value=booleans())
    def test_bool(self, *, value: bool) -> None:
        text = str(value)
        result = parse_text(bool, text)
        assert result is value

    @given(date=dates())
    def test_date(self, *, date: dt.date) -> None:
        text = serialize_date(date)
        result = parse_text(dt.date, text)
        assert result == date

    @given(datetime=local_datetimes() | zoned_datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        text = serialize_datetime(datetime)
        result = parse_text(dt.datetime, text)
        assert result == datetime

    @given(truth=sampled_from(TruthEnum))
    def test_enum(self, *, truth: TruthEnum) -> None:
        text = truth.name
        result = parse_text(TruthEnum, text)
        assert result is truth

    @given(value=floats())
    def test_float(self, *, value: float) -> None:
        text = str(value)
        result = parse_text(float, text)
        assert is_equal(result, value)

    @given(value=integers())
    def test_int(self, *, value: int) -> None:
        text = str(value)
        result = parse_text(int, text)
        assert result == value

    @given(truth=sampled_from(["true", "false"]))
    def test_literal(self, *, truth: Literal["true", "false"]) -> None:
        result = parse_text(Literal["true", "false"], truth)
        assert result == truth

    def test_nullable_int_none(self) -> None:
        text = str(None)
        result = parse_text(int | None, text)
        assert result is None

    @given(value=integers())
    def test_nullable_int_int(self, *, value: int) -> None:
        text = str(value)
        result = parse_text(int | None, text)
        assert result == value

    def test_none(self) -> None:
        text = str(None)
        result = parse_text(None, text)
        assert result is None

    def test_none_type(self) -> None:
        text = str(None)
        result = parse_text(NoneType, text)
        assert result is None

    @given(path=paths())
    def test_path(self, *, path: Path) -> None:
        text = str(path)
        result = parse_text(Path, text)
        assert result == path

    def test_sentinel(self) -> None:
        text = str(sentinel)
        result = parse_text(Sentinel, text)
        assert result is sentinel

    @given(text=text_ascii())
    def test_str(self, *, text: str) -> None:
        result = parse_text(str, text)
        assert result == text

    @given(time=times())
    def test_time(self, *, time: dt.time) -> None:
        text = serialize_time(time)
        result = parse_text(dt.time, text)
        assert result == time

    @given(timedelta=timedeltas_2w())
    def test_timedelta(self, *, timedelta: dt.timedelta) -> None:
        text = serialize_timedelta(timedelta)
        result = parse_text(dt.timedelta, text)
        assert result == timedelta

    @given(version=versions())
    def test_version(self, *, version: Version) -> None:
        text = str(version)
        result = parse_text(Version, text)
        assert result == version

    def test_error_bool(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'bool'>; got 'invalid'"
        ):
            _ = parse_text(bool, "invalid")

    def test_error_date(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'datetime\.date'>; got 'invalid'",
        ):
            _ = parse_text(dt.date, "invalid")

    def test_error_datetime(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'datetime\.datetime'>; got 'invalid'",
        ):
            _ = parse_text(dt.datetime, "invalid")

    def test_error_enum(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <enum 'TruthEnum'>; got 'invalid'"
        ):
            _ = parse_text(TruthEnum, "invalid")

    def test_error_float(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'float'>; got 'invalid'"
        ):
            _ = parse_text(float, "invalid")

    def test_error_int(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'int'>; got 'invalid'"
        ):
            _ = parse_text(int, "invalid")

    def test_error_none(self) -> None:
        with raises(ParseTextError, match="Unable to parse None; got 'invalid'"):
            _ = parse_text(None, "invalid")

    def test_error_none_type(self) -> None:
        with raises(
            ParseTextError, match="Unable to parse <class 'NoneType'>; got 'invalid'"
        ):
            _ = parse_text(NoneType, "invalid")

    def test_error_nullable_int(self) -> None:
        with raises(
            ParseTextError, match=r"Unable to parse int \| None; got 'invalid'"
        ):
            _ = parse_text(int | None, "invalid")

    def test_error_nullable_not_type(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse collections\.abc\.Iterable\[None\] \| None; got 'invalid'",
        ):
            _ = parse_text(Iterable[None] | None, "invalid")

    def test_error_sentinel(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'utilities\.sentinel\.Sentinel'>; got 'invalid'",
        ):
            _ = parse_text(Sentinel, "invalid")

    def test_error_time(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'datetime\.time'>; got 'invalid'",
        ):
            _ = parse_text(dt.time, "invalid")

    def test_error_timedelta(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'datetime\.timedelta'>; got 'invalid'",
        ):
            _ = parse_text(dt.timedelta, "invalid")

    def test_error_unknown_annotation(self) -> None:
        with raises(ParseTextError, match=r"Unable to parse int \| str; got 'invalid'"):
            _ = parse_text(int | str, "invalid")

    def test_error_unknown_type(self) -> None:
        @dataclass(kw_only=True)
        class Example:
            pass

        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'tests\.test_parse\.TestParseText\.test_error_unknown_type\.<locals>\.Example'>; got 'invalid'",
        ):
            _ = parse_text(Example, "invalid")

    def test_error_version(self) -> None:
        with raises(
            ParseTextError,
            match=r"Unable to parse <class 'utilities\.version\.Version'>; got 'invalid'",
        ):
            _ = parse_text(Version, "invalid")
