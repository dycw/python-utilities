from __future__ import annotations

from dataclasses import dataclass, field
from logging import DEBUG
from typing import TYPE_CHECKING, ClassVar, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    integers,
    none,
    sampled_from,
    timezones,
)
from pytest import mark, param, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    TimeDelta,
    TimeZoneNotFoundError,
    ZonedDateTime,
)

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    dates,
    months,
    pairs,
    sentinels,
    zoned_datetimes,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever import (
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
    DAY,
    MICROSECOND,
    MINUTE,
    NOW_LOCAL,
    NOW_UTC,
    PLAIN_DATE_TIME_MAX,
    PLAIN_DATE_TIME_MIN,
    SECOND,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TODAY_LOCAL,
    TODAY_UTC,
    ZERO_DAYS,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
    MeanDateTimeError,
    MinMaxDateError,
    Month,
    ToDaysError,
    ToNanosError,
    WheneverLogRecord,
    _MinMaxDateMaxDateError,
    _MinMaxDateMinDateError,
    _MinMaxDatePeriodError,
    _MonthInvalidError,
    _MonthParseCommonISOError,
    datetime_utc,
    format_compact,
    from_timestamp,
    from_timestamp_millis,
    from_timestamp_nanos,
    get_now,
    get_now_local,
    get_today,
    get_today_local,
    mean_datetime,
    min_max_date,
    to_date,
    to_date_time_delta,
    to_days,
    to_nanos,
    to_time_delta,
    to_zoned_date_time,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable

    from utilities.sentinel import Sentinel
    from utilities.types import MaybeCallableDate, MaybeCallableZonedDateTime


class TestDatetimeUTC:
    @given(datetime=zoned_datetimes())
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        result = datetime_utc(
            datetime.year,
            datetime.month,
            datetime.day,
            hour=datetime.hour,
            minute=datetime.minute,
            second=datetime.second,
            nanosecond=datetime.nanosecond,
        )
        assert result == datetime


class TestFormatCompact:
    @given(datetime=zoned_datetimes())
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = PlainDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round().to_tz(LOCAL_TIME_ZONE_NAME).to_plain()
        assert parsed == expected


class TestFromTimeStamp:
    @given(
        datetime=zoned_datetimes(time_zone=timezones()).map(lambda d: d.round("second"))
    )
    def test_main(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp()
        result = from_timestamp(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(
        datetime=zoned_datetimes(time_zone=timezones()).map(
            lambda d: d.round("millisecond")
        )
    )
    def test_millis(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_millis()
        result = from_timestamp_millis(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime

    @given(datetime=zoned_datetimes(time_zone=timezones()))
    def test_nanos(self, *, datetime: ZonedDateTime) -> None:
        timestamp = datetime.timestamp_nanos()
        result = from_timestamp_nanos(timestamp, time_zone=ZoneInfo(datetime.tz))
        assert result == datetime


class TestGetNow:
    @given(time_zone=timezones())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
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


class TestMeanDateTime:
    threshold: ClassVar[TimeDelta] = 100 * MICROSECOND

    @given(datetime=zoned_datetimes())
    def test_one(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime])
        assert result == datetime

    @given(datetime=zoned_datetimes())
    def test_many(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE])
        expected = datetime + 30 * SECOND
        assert abs(result - expected) <= self.threshold

    @given(datetime=zoned_datetimes())
    def test_weights(self, *, datetime: ZonedDateTime) -> None:
        result = mean_datetime([datetime, datetime + MINUTE], weights=[1, 3])
        expected = datetime + 45 * SECOND
        assert abs(result - expected) <= self.threshold

    def test_error(self) -> None:
        with raises(MeanDateTimeError, match="Mean requires at least 1 datetime"):
            _ = mean_datetime([])


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
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MIN - DateDelta(days=1))

    def test_date_delta_parsable_max(self) -> None:
        self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format: '.*'"):
            self._format_parse_date_delta(DATE_DELTA_PARSABLE_MAX + DateDelta(days=1))

    def test_date_time_delta_min(self) -> None:
        nanos = to_nanos(DATE_TIME_DELTA_MIN)
        with raises(ValueError, match="Out of range"):
            _ = to_date_time_delta(nanos - 1)

    def test_date_time_delta_max(self) -> None:
        nanos = to_nanos(DATE_TIME_DELTA_MAX)
        with raises(ValueError, match="Out of range"):
            _ = to_date_time_delta(nanos + 1)

    def test_date_time_delta_parsable_min(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MIN)
        nanos = to_nanos(DATE_TIME_DELTA_PARSABLE_MIN)
        with raises(ValueError, match="Invalid format or out of range: '.*'"):
            self._format_parse_date_time_delta(to_date_time_delta(nanos - 1))

    def test_date_time_delta_parsable_max(self) -> None:
        self._format_parse_date_time_delta(DATE_TIME_DELTA_PARSABLE_MAX)
        nanos = to_nanos(DATE_TIME_DELTA_PARSABLE_MAX)
        with raises(ValueError, match="Invalid format or out of range: '.*'"):
            _ = self._format_parse_date_time_delta(to_date_time_delta(nanos + 1))

    def test_plain_date_time_min(self) -> None:
        with raises(ValueError, match=r"Result of subtract\(\) out of range"):
            _ = PLAIN_DATE_TIME_MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_date_time_max(self) -> None:
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PLAIN_DATE_TIME_MAX.add(microseconds=1, ignore_dst=True)

    def test_time_delta_min(self) -> None:
        nanos = TIME_DELTA_MIN.in_nanoseconds()
        with raises(ValueError, match="TimeDelta out of range"):
            _ = to_time_delta(nanos - 1)

    def test_time_delta_max(self) -> None:
        nanos = TIME_DELTA_MAX.in_nanoseconds()
        with raises(ValueError, match="TimeDelta out of range"):
            _ = to_time_delta(nanos + 1)

    def test_zoned_date_time_min(self) -> None:
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MIN.subtract(nanoseconds=1)

    def test_zoned_date_time_max(self) -> None:
        with raises(ValueError, match="Instant is out of range"):
            _ = ZONED_DATE_TIME_MAX.add(microseconds=1)

    def _format_parse_date_delta(self, delta: DateDelta, /) -> None:
        _ = DateDelta.parse_common_iso(delta.format_common_iso())

    def _format_parse_date_time_delta(self, delta: DateTimeDelta, /) -> None:
        _ = DateTimeDelta.parse_common_iso(delta.format_common_iso())


class TestMinMaxDate:
    @given(
        min_date=dates(max_value=TODAY_LOCAL) | none(),
        max_date=dates(max_value=TODAY_LOCAL) | none(),
        min_age=date_deltas(min_value=ZERO_DAYS) | none(),
        max_age=date_deltas(min_value=ZERO_DAYS) | none(),
    )
    def test_main(
        self,
        *,
        min_date: Date | None,
        max_date: Date | None,
        min_age: DateDelta | None,
        max_age: DateDelta | None,
    ) -> None:
        with (
            assume_does_not_raise(MinMaxDateError),
            assume_does_not_raise(ValueError, match="Resulting date out of range"),
        ):
            min_date_use, max_date_use = min_max_date(
                min_date=min_date, max_date=max_date, min_age=min_age, max_age=max_age
            )
        if (min_date is None) and (max_age is None):
            assert min_date_use is None
        else:
            assert min_date_use is not None
        if (max_date is None) and (min_age is None):
            assert max_date_use is None
        else:
            assert max_date_use is not None
        if min_date_use is not None:
            assert min_date_use <= get_today()
        if max_date_use is not None:
            assert max_date_use <= get_today()
        if (min_date_use is not None) and (max_date_use is not None):
            assert min_date_use <= max_date_use

    @given(date=dates(min_value=TODAY_UTC + DAY))
    def test_error_min_date(self, *, date: Date) -> None:
        with raises(
            _MinMaxDateMinDateError, match="Min date must be at most today; got .* > .*"
        ):
            _ = min_max_date(min_date=date)

    @given(date=dates(min_value=TODAY_UTC + DAY))
    def test_error_max_date(self, *, date: Date) -> None:
        with raises(
            _MinMaxDateMaxDateError, match="Max date must be at most today; got .* > .*"
        ):
            _ = min_max_date(max_date=date)

    @given(dates=pairs(dates(max_value=TODAY_UTC), unique=True, sorted=True))
    def test_error_period(self, *, dates: tuple[Date, Date]) -> None:
        with raises(
            _MinMaxDatePeriodError,
            match="Min date must be at most max date; got .* > .*",
        ):
            _ = min_max_date(min_date=dates[1], max_date=dates[0])


class TestMonth:
    @mark.parametrize(
        ("month", "n", "expected"),
        [
            param(Month(2000, 1), -2, Month(1999, 11)),
            param(Month(2000, 1), -1, Month(1999, 12)),
            param(Month(2000, 1), 0, Month(2000, 1)),
            param(Month(2000, 1), 1, Month(2000, 2)),
            param(Month(2000, 1), 2, Month(2000, 3)),
            param(Month(2000, 1), 11, Month(2000, 12)),
            param(Month(2000, 1), 12, Month(2001, 1)),
        ],
    )
    def test_add(self, *, month: Month, n: int, expected: Month) -> None:
        result = month + n
        assert result == expected

    @given(month=months())
    def test_common_iso(self, *, month: Month) -> None:
        result = Month.parse_common_iso(month.format_common_iso())
        assert result == month

    @given(data=data(), month=months())
    def test_ensure(self, *, data: DataObject, month: Month) -> None:
        str_or_value = data.draw(sampled_from([month, month.format_common_iso()]))
        result = Month.ensure(str_or_value)
        assert result == month

    @mark.parametrize(
        ("x", "y", "expected"),
        [
            param(Month(2000, 1), Month(1999, 11), 2),
            param(Month(2000, 1), Month(1999, 12), 1),
            param(Month(2000, 1), Month(2000, 1), 0),
            param(Month(2000, 1), Month(2000, 2), -1),
            param(Month(2000, 1), Month(2000, 3), -2),
            param(Month(2000, 1), Month(2000, 12), -11),
            param(Month(2000, 1), Month(2001, 1), -12),
        ],
    )
    def test_diff(self, *, x: Month, y: Month, expected: int) -> None:
        result = x - y
        assert result == expected

    @given(month=months())
    def test_hashable(self, *, month: Month) -> None:
        _ = hash(month)

    @mark.parametrize(
        ("text", "expected"),
        [
            param("2000-01", Month(2000, 1)),
            param("2000.01", Month(2000, 1)),
            param("2000 01", Month(2000, 1)),
            param("200001", Month(2000, 1)),
            param("20-01", Month(2020, 1)),
            param("20.01", Month(2020, 1)),
        ],
    )
    def test_parse_common_iso(self, *, text: str, expected: Month) -> None:
        result = Month.parse_common_iso(text)
        assert result == expected

    @mark.parametrize("func", [param(repr), param(str)])
    def test_repr(self, *, func: Callable[..., str]) -> None:
        result = func(Month(2000, 12))
        expected = "2000-12"
        assert result == expected

    @mark.parametrize(
        ("month", "n", "expected"),
        [
            param(Month(2000, 1), -2, Month(2000, 3)),
            param(Month(2000, 1), -1, Month(2000, 2)),
            param(Month(2000, 1), 0, Month(2000, 1)),
            param(Month(2000, 1), 1, Month(1999, 12)),
            param(Month(2000, 1), 2, Month(1999, 11)),
            param(Month(2000, 1), 12, Month(1999, 1)),
            param(Month(2000, 1), 13, Month(1998, 12)),
        ],
    )
    def test_subtract(self, *, month: Month, n: int, expected: Month) -> None:
        result = month - n
        assert result == expected

    @given(date=dates())
    def test_to_and_from_date(self, *, date: Date) -> None:
        month = Month.from_date(date)
        result = month.to_date(day=date.day)
        assert result == date

    def test_error_invalid(self) -> None:
        with raises(_MonthInvalidError, match=r"Invalid year and month: \d+, \d+"):
            _ = Month(2000, 13)

    @mark.parametrize("text", [param("invalid"), param("202-01")])
    def test_error_parse_common_iso(self, *, text: str) -> None:
        with raises(
            _MonthParseCommonISOError, match=r"Unable to parse month; got '.*'"
        ):
            _ = Month.parse_common_iso(text)


class TestToDate:
    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        assert to_date(date=date) == date

    @given(date=none() | sentinels())
    def test_none_or_sentinel(self, *, date: None | Sentinel) -> None:
        assert to_date(date=date) is date

    @given(date1=dates(), date2=dates())
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

    @given(date=dates())
    def test_callable(self, *, date: Date) -> None:
        assert to_date(date=lambda: date) == date


class TestToDateTimeDeltaAndNanos:
    @given(nanos=integers())
    def test_main(self, *, nanos: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="Out of range"),
            assume_does_not_raise(ValueError, match="total days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = to_date_time_delta(nanos)
        assert to_nanos(delta) == nanos

    def test_error(self) -> None:
        delta = DateTimeDelta(months=1)
        with raises(
            ToNanosError, match="Date-time delta must not contain months; got 1"
        ):
            _ = to_nanos(delta)


class TestToDays:
    @given(days=integers())
    def test_main(self, *, days: int) -> None:
        with (
            assume_does_not_raise(ValueError, match="days out of range"),
            assume_does_not_raise(
                OverflowError, match="Python int too large to convert to C long"
            ),
        ):
            delta = DateDelta(days=days)
        assert to_days(delta) == days

    def test_error(self) -> None:
        delta = DateDelta(months=1)
        with raises(ToDaysError, match="Date delta must not contain months; got 1"):
            _ = to_days(delta)


class TestToZonedDateTime:
    @given(date_time=zoned_datetimes())
    def test_date_time(self, *, date_time: ZonedDateTime) -> None:
        assert to_zoned_date_time(date_time=date_time) == date_time

    @given(date_time=none() | sentinels())
    def test_none_or_sentinel(self, *, date_time: None | Sentinel) -> None:
        assert to_zoned_date_time(date_time=date_time) is date_time

    @given(date_time1=zoned_datetimes(), date_time2=zoned_datetimes())
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

    @given(date_time=zoned_datetimes())
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
