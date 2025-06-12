from __future__ import annotations

from dataclasses import dataclass, field
from logging import DEBUG
from typing import TYPE_CHECKING, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import just, none, timezones
from pytest import mark, param, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    TimeDelta,
    ZonedDateTime,
)

from tests.conftest import IS_CI
from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import dates_whenever, sentinels, zoned_datetimes_whenever
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever2 import (
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    DATE_DELTA_PARSABLE_MAX,
    DATE_DELTA_PARSABLE_MIN,
    DATE_MAX,
    DATE_MIN,
    DATE_TIME_DELTA_MAX,
    DATE_TIME_DELTA_MIN,
    DATE_TIME_DELTA_PARSABLE_MAX,
    DATE_TIME_DELTA_PARSABLE_MIN,
    NOW_LOCAL,
    NOW_UTC,
    PLAIN_DATE_TIME_MAX,
    PLAIN_DATE_TIME_MIN,
    SECOND,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TODAY_LOCAL,
    TODAY_UTC,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
    WheneverLogRecord,
    format_compact,
    from_timestamp,
    from_timestamp_millis,
    from_timestamp_nanos,
    get_now,
    get_now_local,
    get_today,
    get_today_local,
    to_date,
    to_zoned_date_time,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from utilities.sentinel import Sentinel
    from utilities.types import MaybeCallableDate, MaybeCallableZonedDateTime


class TestFormatCompact:
    @given(datetime=zoned_datetimes_whenever())
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = PlainDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round().to_tz(LOCAL_TIME_ZONE_NAME).to_plain()
        assert parsed == expected


class TestFromTimeStamp:
    @given(datetime=zoned_datetimes_whenever(time_zone=UTC if IS_CI else timezones()))
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        datetime = datetime.round("second")
        timestamp = datetime.to_tz(UTC.key).timestamp()
        result = from_timestamp(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(datetime=zoned_datetimes_whenever(time_zone=UTC if IS_CI else timezones()))
    def test_millis(self, *, datetime: ZonedDateTime) -> None:
        datetime = datetime.round("millisecond")
        timestamp = datetime.to_tz(UTC.key).timestamp_millis()
        result = from_timestamp_millis(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(datetime=zoned_datetimes_whenever(time_zone=UTC if IS_CI else timezones()))
    def test_nanos(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.to_tz(UTC.key).timestamp_nanos()
        result = from_timestamp_nanos(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime


class TestGetNow:
    @given(time_zone=just(UTC) if IS_CI else timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone=time_zone)
        assert isinstance(now, ZonedDateTime)
        assert now.tz == time_zone.key

    def test_constant(self) -> None:
        assert isinstance(NOW_UTC, ZonedDateTime)
        assert NOW_UTC.tz == "UTC"


class TestGetNowLocal:
    def test_function(self) -> None:
        now = get_now_local()
        assert isinstance(now, ZonedDateTime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        time_zones = {ETC, HongKong, Tokyo, UTC}
        assert any(now.tz == time_zone.key for time_zone in time_zones)

    def test_constant(self) -> None:
        assert isinstance(NOW_LOCAL, ZonedDateTime)
        assert NOW_LOCAL.tz == LOCAL_TIME_ZONE_NAME


class TestGetToday:
    def test_function(self) -> None:
        today = get_today()
        assert isinstance(today, Date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_UTC, Date)


class TestGetTodayLocal:
    def test_function(self) -> None:
        today = get_today_local()
        assert isinstance(today, Date)

    def test_constant(self) -> None:
        assert isinstance(TODAY_LOCAL, Date)


class TestMinMax:
    def test_date_min(self) -> None:
        with raises(ValueError, match="Resulting date out of range"):
            _ = DATE_MIN - DateDelta(days=1)

    def test_date_max(self) -> None:
        with raises(ValueError, match="Resulting date out of range"):
            _ = DATE_MAX + DateDelta(days=1)

    def test_date_delta_min(self) -> None:
        with raises(ValueError, match="Addition result out of bounds"):
            _ = DATE_DELTA_MIN - DateDelta(days=1)

    def test_date_delta_max(self) -> None:
        with raises(ValueError, match="Addition result out of bounds"):
            _ = DATE_DELTA_MAX + DateDelta(days=1)

    def test_date_delta_parsable_min(self) -> None:
        def func(delta: DateDelta, /) -> None:
            _ = DateDelta.parse_common_iso(delta.format_common_iso())

        _ = func(DATE_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Invalid format: '.*'"):
            _ = func(DATE_DELTA_PARSABLE_MIN - DateDelta(days=1))

    def test_date_delta_parsable_max(self) -> None:
        def func(delta: DateDelta, /) -> None:
            _ = DateDelta.parse_common_iso(delta.format_common_iso())

        _ = func(DATE_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format: '.*'"):
            _ = func(DATE_DELTA_PARSABLE_MAX + DateDelta(days=1))

    @mark.parametrize(
        "delta",
        [
            param(DateTimeDelta(days=1)),
            param(DateTimeDelta(seconds=1)),
            param(DateTimeDelta(milliseconds=1)),
            param(DateTimeDelta(microseconds=1)),
            param(DateTimeDelta(nanoseconds=1)),
        ],
    )
    def test_date_time_delta_min(self, *, delta: DateTimeDelta) -> None:
        with raises(ValueError, match="Addition result out of bounds"):
            _ = DATE_TIME_DELTA_MIN - delta

    @mark.parametrize(
        ("delta", "is_ok"),
        [
            param(DateTimeDelta(days=1), False),
            param(DateTimeDelta(seconds=1), False),
            param(DateTimeDelta(milliseconds=999), True),
            param(DateTimeDelta(milliseconds=1000), False),
            param(DateTimeDelta(microseconds=999_999), True),
            param(DateTimeDelta(microseconds=1_000_000), False),
            param(DateTimeDelta(nanoseconds=999_999_999), True),
            param(DateTimeDelta(nanoseconds=1_000_000_000), False),
        ],
    )
    def test_date_time_delta_max(self, *, delta: DateTimeDelta, is_ok: bool) -> None:
        if is_ok:
            _ = DATE_TIME_DELTA_MAX + delta
        else:
            with raises(ValueError, match="Addition result out of bounds"):
                _ = DATE_TIME_DELTA_MAX + delta

    def test_date_time_delta_parsable_min(self) -> None:
        def func(delta: DateTimeDelta, /) -> None:
            _ = DateTimeDelta.parse_common_iso(delta.format_common_iso())

        _ = func(DATE_TIME_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Addition result out of bounds"):
            _ = func(DATE_TIME_DELTA_PARSABLE_MIN - DateTimeDelta(nanoseconds=1))

    def test_date_time_delta_parsable_max(self) -> None:
        def func(delta: DateTimeDelta, /) -> None:
            _ = DateTimeDelta.parse_common_iso(delta.format_common_iso())

        _ = func(DATE_TIME_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format or out of range: '.*'"):
            _ = func(DATE_TIME_DELTA_PARSABLE_MAX + TimeDelta(nanoseconds=1))

    def test_plain_date_time_min(self) -> None:
        with raises(ValueError, match=r"Result of subtract\(\) out of range"):
            _ = PLAIN_DATE_TIME_MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_date_time_max(self) -> None:
        _ = PLAIN_DATE_TIME_MAX.add(nanoseconds=999, ignore_dst=True)
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PLAIN_DATE_TIME_MAX.add(microseconds=1, ignore_dst=True)

    @mark.parametrize(
        "delta",
        [
            param(TimeDelta(seconds=1)),
            param(TimeDelta(milliseconds=1)),
            param(TimeDelta(microseconds=1)),
            param(TimeDelta(nanoseconds=1)),
        ],
    )
    def test_time_delta_min(self, *, delta: TimeDelta) -> None:
        _ = TimeDelta.parse_common_iso(TIME_DELTA_MIN.format_common_iso())
        with raises(ValueError, match="Addition result out of range"):
            _ = TIME_DELTA_MIN - delta

    @mark.parametrize(
        ("delta", "is_ok"),
        [
            param(TimeDelta(seconds=1), False),
            param(TimeDelta(milliseconds=999), True),
            param(TimeDelta(milliseconds=1000), False),
            param(TimeDelta(microseconds=999_999), True),
            param(TimeDelta(microseconds=1_000_000), False),
            param(TimeDelta(nanoseconds=999_999_999), True),
            param(TimeDelta(nanoseconds=1_000_000_000), False),
        ],
    )
    def test_time_delta_max(self, *, delta: TimeDelta, is_ok: bool) -> None:
        if is_ok:
            _ = TIME_DELTA_MAX + delta
            _ = TimeDelta.parse_common_iso(delta.format_common_iso())
        else:
            with raises(ValueError, match="Addition result out of range"):
                _ = TIME_DELTA_MAX + delta

    def test_zoned_date_time_min(self) -> None:
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MIN.subtract(nanoseconds=1)

    def test_zoned_date_time_max(self) -> None:
        _ = ZONED_DATE_TIME_MAX.add(nanoseconds=999)
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MAX.add(microseconds=1)


class TestToDate:
    @given(date=dates_whenever())
    def test_date(self, *, date: Date) -> None:
        assert to_date(date=date) == date

    @given(date=none() | sentinels())
    def test_none_or_sentinel(self, *, date: None | Sentinel) -> None:
        assert to_date(date=date) is date

    @given(date1=dates_whenever(), date2=dates_whenever())
    def test_replace_non_sentinel(self, *, date1: Date, date2: Date) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            date: Date = field(default_factory=get_today)

            def replace(self, *, date: MaybeCallableDate | Sentinel = sentinel) -> Self:
                return replace_non_sentinel(self, date=to_date(date=date))

        obj = Example(date=date1)
        assert obj.date == date1
        assert obj.replace().date == date1
        assert obj.replace(date=date2).date == date2
        assert obj.replace(date=get_today).date == get_today()

    @given(date=dates_whenever())
    def test_callable(self, *, date: Date) -> None:
        assert to_date(date=lambda: date) == date


class TestGetDateTime:
    @given(date_time=zoned_datetimes_whenever())
    def test_date_time(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(date_time=date_time) == date_time

    @given(date_time=none() | sentinels())
    def test_none_or_sentinel(self, *, date_time: None | Sentinel) -> None:
        assert to_zoned_date_time(date_time=date_time) is date_time

    @given(date_time1=zoned_datetimes_whenever(), date_time2=zoned_datetimes_whenever())
    def test_replace_non_sentinel(
        self, *, date_time1: ZonedDateTime, date_time2: ZonedDateTime
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            date_time: ZonedDateTime = field(default_factory=get_now)

            def replace(
                self, *, date_time: MaybeCallableZonedDateTime | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(
                    self, date_time=to_zoned_date_time(date_time=date_time)
                )

        obj = Example(date_time=date_time1)
        assert obj.date_time == date_time1
        assert obj.replace().date_time == date_time1
        assert obj.replace(date_time=date_time2).date_time == date_time2
        assert abs(obj.replace(date_time=get_now).date_time - get_now()) <= SECOND

    @given(date_time=zoned_datetimes_whenever())
    def test_callable(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(date_time=lambda: date_time) == date_time


class TestWheneverLogRecord:
    def test_init(self) -> None:
        _ = WheneverLogRecord("name", DEBUG, "pathname", 0, None, None, None)

    def test_get_length(self) -> None:
        assert isinstance(WheneverLogRecord._get_length(), int)

    def test_get_time_zone(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone(), ZoneInfo)

    def test_get_time_zone_key(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone_key(), str)
