from __future__ import annotations

from dataclasses import dataclass, field
from logging import DEBUG
from typing import TYPE_CHECKING, ClassVar, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from hypothesis.strategies import integers, none, sampled_from, timezones
from pytest import raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    PlainDateTime,
    Time,
    TimeDelta,
    TimeZoneNotFoundError,
    YearMonth,
    ZonedDateTime,
)

from utilities.dataclasses import replace_non_sentinel
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    dates,
    freqs,
    pairs,
    plain_datetimes,
    sentinels,
    times,
    zoned_datetimes,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.types import DateTimeRoundUnit
from utilities.typing import get_literal_elements
from utilities.tzdata import HongKong, Tokyo
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever import (
    DATE_DELTA_MAX,
    DATE_DELTA_MIN,
    DATE_DELTA_PARSABLE_MAX,
    DATE_DELTA_PARSABLE_MIN,
    DATE_TIME_DELTA_MAX,
    DATE_TIME_DELTA_MIN,
    DATE_TIME_DELTA_PARSABLE_MAX,
    DATE_TIME_DELTA_PARSABLE_MIN,
    DAY,
    MICROSECOND,
    MINUTE,
    NOW_LOCAL,
    NOW_UTC,
    SECOND,
    TIME_DELTA_MAX,
    TIME_DELTA_MIN,
    TODAY_LOCAL,
    TODAY_UTC,
    ZERO_DAYS,
    ZONED_DATE_TIME_MAX,
    ZONED_DATE_TIME_MIN,
    Freq,
    MeanDateTimeError,
    MinMaxDateError,
    ToDaysError,
    ToNanosError,
    WheneverLogRecord,
    _FreqDayIncrementError,
    _FreqIncrementError,
    _FreqParseError,
    _MinMaxDateMaxDateError,
    _MinMaxDateMinDateError,
    _MinMaxDatePeriodError,
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
    to_local_plain,
    to_nanos,
    to_time_delta,
    to_zoned_date_time,
    two_digit_year_month,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
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
    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        result = format_compact(date)
        assert isinstance(result, str)
        parsed = Date.parse_common_iso(result)
        assert parsed == date

    @given(time=times())
    def test_time(self, *, time: Time) -> None:
        result = format_compact(time)
        assert isinstance(result, str)
        parsed = Time.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = time.round()
        assert parsed == expected

    @given(datetime=plain_datetimes())
    def test_plain_datetime(self, *, datetime: PlainDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = PlainDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round()
        assert parsed == expected

    @given(datetime=zoned_datetimes())
    def test_zoned_datetime(self, *, datetime: ZonedDateTime) -> None:
        result = format_compact(datetime)
        assert isinstance(result, str)
        parsed = ZonedDateTime.parse_common_iso(result)
        assert parsed.nanosecond == 0
        expected = datetime.round()
        assert parsed == expected


class TestFreq:
    @given(freq=freqs())
    def test_main(self, *, freq: Freq) -> None:
        _ = get_now().round(unit=freq.unit, increment=freq.increment, mode="floor")

    @given(unit=sampled_from(get_literal_elements(DateTimeRoundUnit)))
    def test_abbreviate_and_expand(self, *, unit: DateTimeRoundUnit) -> None:
        result = Freq._expand(Freq._abbreviate(unit))
        assert result == unit

    @given(freqs=pairs(freqs()))
    def test_eq(self, *, freqs: tuple[Freq, Freq]) -> None:
        x, y = freqs
        result = x == y
        assert isinstance(result, bool)

    @given(freq=freqs())
    def test_eq_non_freq(self, *, freq: Freq) -> None:
        result = freq == 0
        assert not result

    @given(freq=freqs())
    def test_hashable(self, *, freq: Freq) -> None:
        _ = hash(freq)

    @given(freq=freqs())
    def test_repr(self, *, freq: Freq) -> None:
        _ = repr(freq)

    @given(freq=freqs())
    def test_serialize_and_parse(self, *, freq: Freq) -> None:
        result = Freq.parse(freq.serialize())
        assert result == freq

    def test_error_day(self) -> None:
        with raises(
            _FreqDayIncrementError,
            match="Increment must be 1 for the 'day' unit; got 2",
        ):
            _ = Freq(unit="day", increment=2)

    def test_error_hour(self) -> None:
        with raises(
            _FreqIncrementError,
            match="Increment must be a proper divisor of 24 for the 'hour' unit; got 5",
        ):
            _ = Freq(unit="hour", increment=5)

    def test_error_minute(self) -> None:
        with raises(
            _FreqIncrementError,
            match="Increment must be a proper divisor of 60 for the 'minute' unit; got 7",
        ):
            _ = Freq(unit="minute", increment=7)

    def test_error_milliseond(self) -> None:
        with raises(
            _FreqIncrementError,
            match="Increment must be a proper divisor of 1000 for the 'millisecond' unit; got 3",
        ):
            _ = Freq(unit="millisecond", increment=3)

    def test_error_parse(self) -> None:
        with raises(_FreqParseError, match="Unable to parse frequency; got 's'"):
            _ = Freq.parse("s")


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
            _ = PlainDateTime.MIN.subtract(nanoseconds=1, ignore_dst=True)

    def test_plain_date_time_max(self) -> None:
        with raises(ValueError, match=r"Result of add\(\) out of range"):
            _ = PlainDateTime.MAX.add(microseconds=1, ignore_dst=True)

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


class TestToLocalPlain:
    @given(date_time=zoned_datetimes())
    def test_main(self, *, date_time: ZonedDateTime) -> None:
        result = to_local_plain(date_time)
        assert isinstance(result, PlainDateTime)


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


class TestTwoDigitYearMonth:
    def test_parse_common_iso(self) -> None:
        result = two_digit_year_month(0, 1)
        expected = YearMonth(2000, 1)
        assert result == expected


class TestWheneverLogRecord:
    def test_init(self) -> None:
        _ = WheneverLogRecord("name", DEBUG, "pathname", 0, None, None, None)

    def test_get_length(self) -> None:
        assert isinstance(WheneverLogRecord._get_length(), int)

    def test_get_time_zone(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone(), ZoneInfo)

    def test_get_time_zone_key(self) -> None:
        assert isinstance(WheneverLogRecord._get_time_zone_key(), str)
