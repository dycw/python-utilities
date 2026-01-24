from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self
from zoneinfo import ZoneInfo

from hypothesis import given
from pytest import mark, param, raises
from whenever import Date, PlainDateTime, Time, ZonedDateTime

from utilities.constants import (
    DAY,
    HOUR,
    MICROSECOND,
    MILLISECOND,
    MINUTE,
    MONTH,
    NANOSECOND,
    SECOND,
    UTC,
    WEEK,
    YEAR,
    HongKong,
    Sentinel,
    Tokyo,
    sentinel,
)
from utilities.core import (
    NumDaysError,
    NumHoursError,
    NumMicroSecondsError,
    NumMilliSecondsError,
    NumMinutesError,
    NumMonthsError,
    NumNanoSecondsError,
    NumSecondsError,
    NumWeeksError,
    NumYearsError,
    _DeltaComponentsMixedSignError,
    _DeltaComponentsOutput,
    delta_components,
    duration_to_milliseconds,
    duration_to_seconds,
    get_now,
    get_now_local,
    get_now_local_plain,
    get_now_plain,
    get_time,
    get_time_local,
    get_today,
    get_today_local,
    num_days,
    num_hours,
    num_microseconds,
    num_milliseconds,
    num_minutes,
    num_months,
    num_nanoseconds,
    num_seconds,
    num_weeks,
    num_years,
    replace_non_sentinel,
    to_date,
)
from utilities.hypothesis import dates, pairs, zone_infos

if TYPE_CHECKING:
    from utilities.types import Delta, Duration, MaybeCallableDateLike, Number, Pair


class TestDeltaComponents:
    @mark.parametrize("sign", [param(1), param(-1)])
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(YEAR, _DeltaComponentsOutput(years=1)),
            param(24 * MONTH, _DeltaComponentsOutput(years=2)),
            param(18 * MONTH, _DeltaComponentsOutput(years=1, months=6)),
            param(MONTH, _DeltaComponentsOutput(months=1)),
            param(WEEK, _DeltaComponentsOutput(weeks=1)),
            param(14 * DAY, _DeltaComponentsOutput(weeks=2)),
            param(10 * DAY, _DeltaComponentsOutput(weeks=1, days=3)),
            param(7 * DAY, _DeltaComponentsOutput(weeks=1)),
            param(DAY, _DeltaComponentsOutput(days=1)),
            param(48 * HOUR, _DeltaComponentsOutput(days=2)),
            param(36 * HOUR, _DeltaComponentsOutput(days=1, hours=12)),
            param(24 * HOUR, _DeltaComponentsOutput(days=1)),
            param(HOUR, _DeltaComponentsOutput(hours=1)),
            param(120 * MINUTE, _DeltaComponentsOutput(hours=2)),
            param(90 * MINUTE, _DeltaComponentsOutput(hours=1, minutes=30)),
            param(60 * MINUTE, _DeltaComponentsOutput(hours=1)),
            param(120 * SECOND, _DeltaComponentsOutput(minutes=2)),
            param(90 * SECOND, _DeltaComponentsOutput(minutes=1, seconds=30)),
            param(60 * SECOND, _DeltaComponentsOutput(minutes=1)),
            param(SECOND, _DeltaComponentsOutput(seconds=1)),
            param(2000 * MILLISECOND, _DeltaComponentsOutput(seconds=2)),
            param(
                1500 * MILLISECOND, _DeltaComponentsOutput(seconds=1, milliseconds=500)
            ),
            param(1000 * MILLISECOND, _DeltaComponentsOutput(seconds=1)),
            param(MILLISECOND, _DeltaComponentsOutput(milliseconds=1)),
            param(2000 * MICROSECOND, _DeltaComponentsOutput(milliseconds=2)),
            param(
                1500 * MICROSECOND,
                _DeltaComponentsOutput(milliseconds=1, microseconds=500),
            ),
            param(1000 * MICROSECOND, _DeltaComponentsOutput(milliseconds=1)),
            param(MICROSECOND, _DeltaComponentsOutput(microseconds=1)),
            param(2000 * NANOSECOND, _DeltaComponentsOutput(microseconds=2)),
            param(
                1500 * NANOSECOND,
                _DeltaComponentsOutput(microseconds=1, nanoseconds=500),
            ),
            param(1000 * NANOSECOND, _DeltaComponentsOutput(microseconds=1)),
            param(NANOSECOND, _DeltaComponentsOutput(nanoseconds=1)),
        ],
    )
    def test_main(
        self, *, sign: int, delta: Delta, expected: _DeltaComponentsOutput
    ) -> None:
        result = delta_components(sign * delta)
        signed_expected = _DeltaComponentsOutput(
            years=sign * expected.years,
            months=sign * expected.months,
            weeks=sign * expected.weeks,
            days=sign * expected.days,
            hours=sign * expected.hours,
            minutes=sign * expected.minutes,
            seconds=sign * expected.seconds,
            milliseconds=sign * expected.milliseconds,
            microseconds=sign * expected.microseconds,
            nanoseconds=sign * expected.nanoseconds,
        )
        assert result == signed_expected

    @mark.parametrize("sign", [param(1), param(-1)])
    @mark.parametrize(
        ("input_", "expected"),
        [
            param({"months": 24}, {"years": 2}),
            param({"months": 18}, {"years": 1, "months": 6}),
            param({"months": 12}, {"years": 1}),
            param({"days": 14}, {"weeks": 2}),
            param({"days": 10}, {"weeks": 1, "days": 3}),
            param({"days": 7}, {"weeks": 1}),
            param({"hours": 48}, {"days": 2}),
            param({"hours": 36}, {"days": 1, "hours": 12}),
            param({"hours": 24}, {"days": 1}),
            param({"minutes": 120}, {"hours": 2}),
            param({"minutes": 90}, {"hours": 1, "minutes": 30}),
            param({"minutes": 60}, {"hours": 1}),
            param({"seconds": 120}, {"minutes": 2}),
            param({"seconds": 90}, {"minutes": 1, "seconds": 30}),
            param({"seconds": 60}, {"minutes": 1}),
            param({"milliseconds": 2000}, {"seconds": 2}),
            param({"milliseconds": 1500}, {"seconds": 1, "milliseconds": 500}),
            param({"milliseconds": 1000}, {"seconds": 1}),
            param({"microseconds": 2000}, {"milliseconds": 2}),
            param({"microseconds": 1500}, {"milliseconds": 1, "microseconds": 500}),
            param({"microseconds": 1000}, {"milliseconds": 1}),
            param({"nanoseconds": 2000}, {"microseconds": 2}),
            param({"nanoseconds": 1500}, {"microseconds": 1, "nanoseconds": 500}),
            param({"nanoseconds": 1000}, {"microseconds": 1}),
        ],
    )
    def test_normalize(
        self, *, sign: int, input_: dict[str, int], expected: dict[str, int]
    ) -> None:
        result = _DeltaComponentsOutput(**{k: sign * v for k, v in input_.items()})
        signed_expected = _DeltaComponentsOutput(**{
            k: sign * v for k, v in expected.items()
        })
        assert result == signed_expected

    @mark.parametrize(
        ("years", "months", "days"),
        [param(1, 0, -1), param(0, 1, -1), param(-1, 0, 1), param(0, -1, 1)],
    )
    def test_error_mixed_sign(self, *, years: int, months: int, days: int) -> None:
        with raises(
            _DeltaComponentsMixedSignError,
            match=r"Years, months and days must have the same sign; got .*, .* and .*",
        ):
            _ = _DeltaComponentsOutput(years=years, months=months, days=days)


class TestDurationToMilliSeconds:
    @mark.parametrize(
        ("duration", "expected"),
        [
            param(1, 1),
            param(1.0, 1.0),
            param(SECOND, 1000.0),
            param(MILLISECOND, 1.0),
            param(MICROSECOND, 0.001),
        ],
    )
    def test_main(self, *, duration: Duration, expected: Number) -> None:
        assert duration_to_milliseconds(duration) == expected


class TestDurationToSeconds:
    @mark.parametrize(
        ("duration", "expected"),
        [
            param(1, 1),
            param(1.0, 1.0),
            param(MINUTE, 60.0),
            param(SECOND, 1.0),
            param(MILLISECOND, 0.001),
        ],
    )
    def test_main(self, *, duration: Duration, expected: Number) -> None:
        assert duration_to_seconds(duration) == expected


class TestGetNow:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now(time_zone)
        assert isinstance(now, ZonedDateTime)
        assert now.tz == time_zone.key


class TestGetNowLocal:
    def test_function(self) -> None:
        now = get_now_local()
        assert isinstance(now, ZonedDateTime)
        ETC = ZoneInfo("Etc/UTC")  # noqa: N806
        time_zones = {ETC, HongKong, Tokyo, UTC}
        assert any(now.tz == time_zone.key for time_zone in time_zones)


class TestGetNowLocalPlain:
    def test_function(self) -> None:
        now = get_now_local_plain()
        assert isinstance(now, PlainDateTime)


class TestGetNowPlain:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_now_plain(time_zone)
        assert isinstance(now, PlainDateTime)


class TestGetTime:
    @given(time_zone=zone_infos())
    def test_function(self, *, time_zone: ZoneInfo) -> None:
        now = get_time(time_zone)
        assert isinstance(now, Time)


class TestGetTimeLocal:
    def test_function(self) -> None:
        now = get_time_local()
        assert isinstance(now, Time)


class TestGetToday:
    def test_function(self) -> None:
        today = get_today()
        assert isinstance(today, Date)


class TestGetTodayLocal:
    def test_function(self) -> None:
        today = get_today_local()
        assert isinstance(today, Date)


class TestNumDays:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 14),
            param(WEEK + 3 * DAY, 10),
            param(WEEK, 7),
            param(2 * DAY, 2),
            param(DAY, 1),
            param(48 * HOUR, 2),
            param(24 * HOUR, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_days(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(YEAR),
            param(MONTH),
            param(HOUR),
            param(MINUTE),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumDaysError,
            match=r"Delta must not contain years \(.*\), months \(.*\), hours \(.*\), minutes \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_days(delta)


class TestNumHours:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 336),
            param(WEEK + 3 * DAY, 240),
            param(WEEK, 168),
            param(2 * DAY, 48),
            param(DAY + 12 * HOUR, 36),
            param(DAY, 24),
            param(2 * HOUR, 2),
            param(HOUR, 1),
            param(120 * MINUTE, 2),
            param(60 * MINUTE, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_hours(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(YEAR),
            param(MONTH),
            param(MINUTE),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumHoursError,
            match=r"Delta must not contain years \(.*\), months \(.*\), minutes \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_hours(delta)


class TestNumMicroSeconds:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 1_209_600_000_000),
            param(WEEK + 3 * DAY, 864_000_000_000),
            param(WEEK, 604_800_000_000),
            param(2 * DAY, 172_800_000_000),
            param(DAY + 12 * HOUR, 129_600_000_000),
            param(DAY, 86_400_000_000),
            param(2 * HOUR, 7_200_000_000),
            param(HOUR + 30 * MINUTE, 5_400_000_000),
            param(HOUR, 3_600_000_000),
            param(2 * MINUTE, 120_000_000),
            param(MINUTE + 30 * SECOND, 90_000_000),
            param(MINUTE, 60_000_000),
            param(2 * SECOND, 2_000_000),
            param(SECOND + 500 * MILLISECOND, 1_500_000),
            param(SECOND, 1_000_000),
            param(2 * MILLISECOND, 2_000),
            param(MILLISECOND + 500 * MICROSECOND, 1_500),
            param(MILLISECOND, 1_000),
            param(2 * MICROSECOND, 2),
            param(MICROSECOND, 1),
            param(2000 * NANOSECOND, 2),
            param(1000 * NANOSECOND, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_microseconds(delta) == expected

    @mark.parametrize("delta", [param(YEAR), param(MONTH), param(NANOSECOND)])
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumMicroSecondsError,
            match=r"Delta must not contain years \(.*\), months \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_microseconds(delta)


class TestNumMilliSeconds:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 1_209_600_000),
            param(WEEK + 3 * DAY, 864_000_000),
            param(WEEK, 604_800_000),
            param(2 * DAY, 172_800_000),
            param(DAY + 12 * HOUR, 129_600_000),
            param(DAY, 86_400_000),
            param(2 * HOUR, 7_200_000),
            param(HOUR + 30 * MINUTE, 5_400_000),
            param(HOUR, 3_600_000),
            param(2 * MINUTE, 120_000),
            param(MINUTE + 30 * SECOND, 90_000),
            param(MINUTE, 60_000),
            param(2 * SECOND, 2_000),
            param(SECOND + 500 * MILLISECOND, 1_500),
            param(SECOND, 1_000),
            param(2 * MILLISECOND, 2),
            param(MILLISECOND, 1),
            param(2000 * MICROSECOND, 2),
            param(1000 * MICROSECOND, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_milliseconds(delta) == expected

    @mark.parametrize(
        "delta", [param(YEAR), param(MONTH), param(MICROSECOND), param(NANOSECOND)]
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumMilliSecondsError,
            match=r"Delta must not contain years \(.*\), months \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_milliseconds(delta)


class TestNumMinutes:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 20_160),
            param(WEEK + 3 * DAY, 14_400),
            param(WEEK, 10_080),
            param(2 * DAY, 2_880),
            param(DAY + 12 * HOUR, 2_160),
            param(DAY, 1_440),
            param(2 * HOUR, 120),
            param(HOUR + 30 * MINUTE, 90),
            param(HOUR, 60),
            param(2 * MINUTE, 2),
            param(MINUTE, 1),
            param(120 * SECOND, 2),
            param(60 * SECOND, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_minutes(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(YEAR),
            param(MONTH),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumMinutesError,
            match=r"Delta must not contain years \(.*\), months \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_minutes(delta)


class TestNumMonths:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * YEAR, 24),
            param(YEAR + 6 * MONTH, 18),
            param(YEAR, 12),
            param(2 * MONTH, 2),
            param(MONTH, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_months(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(WEEK),
            param(DAY),
            param(HOUR),
            param(MINUTE),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumMonthsError,
            match=r"Delta must not contain weeks \(.*\), days \(.*\), hours \(.*\), minutes \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_months(delta)


class TestNumNanoSeconds:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 1_209_600_000_000_000),
            param(WEEK + 3 * DAY, 864_000_000_000_000),
            param(WEEK, 604_800_000_000_000),
            param(2 * DAY, 172_800_000_000_000),
            param(DAY + 12 * HOUR, 129_600_000_000_000),
            param(DAY, 86_400_000_000_000),
            param(2 * HOUR, 7_200_000_000_000),
            param(HOUR + 30 * MINUTE, 5_400_000_000_000),
            param(HOUR, 3_600_000_000_000),
            param(2 * MINUTE, 120_000_000_000),
            param(MINUTE + 30 * SECOND, 90_000_000_000),
            param(MINUTE, 60_000_000_000),
            param(2 * SECOND, 2_000_000_000),
            param(SECOND + 500 * MILLISECOND, 1_500_000_000),
            param(SECOND, 1_000_000_000),
            param(2 * MILLISECOND, 2_000_000),
            param(MILLISECOND + 500 * MICROSECOND, 1_500_000),
            param(MILLISECOND, 1_000_000),
            param(2 * MICROSECOND, 2_000),
            param(MICROSECOND + 500 * NANOSECOND, 1_500),
            param(MICROSECOND, 1_000),
            param(2 * NANOSECOND, 2),
            param(NANOSECOND, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_nanoseconds(delta) == expected

    @mark.parametrize("delta", [param(YEAR), param(MONTH)])
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumNanoSecondsError,
            match=r"Delta must not contain years \(.*\) or months \(.*\)",
        ):
            _ = num_nanoseconds(delta)


class TestNumSeconds:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * WEEK, 1_209_600),
            param(WEEK + 3 * DAY, 864_000),
            param(WEEK, 604_800),
            param(2 * DAY, 172_800),
            param(DAY + 12 * HOUR, 129_600),
            param(DAY, 86_400),
            param(2 * HOUR, 7_200),
            param(HOUR + 30 * MINUTE, 5_400),
            param(HOUR, 3_600),
            param(2 * MINUTE, 120),
            param(MINUTE + 30 * SECOND, 90),
            param(MINUTE, 60),
            param(2 * SECOND, 2),
            param(SECOND, 1),
            param(2000 * MILLISECOND, 2),
            param(1000 * MILLISECOND, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_seconds(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(YEAR),
            param(MONTH),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumSecondsError,
            match=r"Delta must not contain years \(.*\), months \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_seconds(delta)


class TestNumWeeks:
    @mark.parametrize(
        ("delta", "expected"),
        [param(2 * WEEK, 2), param(WEEK, 1), param(14 * DAY, 2), param(7 * DAY, 1)],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_weeks(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(YEAR),
            param(MONTH),
            param(DAY),
            param(HOUR),
            param(MINUTE),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumWeeksError,
            match=r"Delta must not contain years \(.*\), months \(.*\), days \(.*\), hours \(.*\), minutes \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_weeks(delta)


class TestNumYears:
    @mark.parametrize(
        ("delta", "expected"),
        [
            param(2 * YEAR, 2),
            param(YEAR, 1),
            param(24 * MONTH, 2),
            param(12 * MONTH, 1),
        ],
    )
    def test_main(self, *, delta: Delta, expected: int) -> None:
        assert num_years(delta) == expected

    @mark.parametrize(
        "delta",
        [
            param(MONTH),
            param(WEEK),
            param(HOUR),
            param(MINUTE),
            param(SECOND),
            param(MILLISECOND),
            param(MICROSECOND),
            param(NANOSECOND),
        ],
    )
    def test_error(self, *, delta: Delta) -> None:
        with raises(
            NumYearsError,
            match=r"Delta must not contain months \(.*\), weeks \(.*\), days \(.*\), hours \(.*\), minutes \(.*\), seconds \(.*\), milliseconds \(.*\), microseconds \(.*\) or nanoseconds \(.*\)",
        ):
            _ = num_years(delta)


class TestToDate:
    def test_default(self) -> None:
        assert to_date() == get_today()

    @given(date=dates())
    def test_date(self, *, date: Date) -> None:
        assert to_date(date) == date

    @given(date=dates())
    def test_str(self, *, date: Date) -> None:
        assert to_date(date.format_iso()) == date

    @given(date=dates())
    def test_py_date(self, *, date: Date) -> None:
        assert to_date(date.py_date()) == date

    @given(date=dates())
    def test_callable(self, *, date: Date) -> None:
        assert to_date(lambda: date) == date

    def test_none(self) -> None:
        assert to_date(None) == get_today()

    def test_sentinel(self) -> None:
        assert to_date(sentinel) is sentinel

    @given(dates=pairs(dates()))
    def test_replace_non_sentinel(self, *, dates: Pair[Date]) -> None:
        date1, date2 = dates

        @dataclass(kw_only=True, slots=True)
        class Example:
            date: Date = field(default_factory=get_today)

            def replace(
                self, *, date: MaybeCallableDateLike | Sentinel = sentinel
            ) -> Self:
                return replace_non_sentinel(self, date=to_date(date))

        obj = Example(date=date1)
        assert obj.date == date1
        assert obj.replace().date == date1
        assert obj.replace(date=date2).date == date2
        assert obj.replace(date=get_today).date == get_today()
