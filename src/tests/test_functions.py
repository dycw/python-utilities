from __future__ import annotations

from subprocess import check_output
from sys import executable
from typing import TYPE_CHECKING, Any, ClassVar, cast

from hypothesis import given
from hypothesis.strategies import integers, sampled_from
from pytest import approx, mark, param, raises

from utilities.constants import HOME, NOW_UTC, ZERO_TIME, sentinel
from utilities.core import get_now, get_today, normalize_multi_line_str
from utilities.functions import (
    EnsureBoolError,
    EnsureBytesError,
    EnsureClassError,
    EnsureDateError,
    EnsureFloatError,
    EnsureIntError,
    EnsureMemberError,
    EnsureNotNoneError,
    EnsureNumberError,
    EnsurePathError,
    EnsurePlainDateTimeError,
    EnsureStrError,
    EnsureTimeDeltaError,
    EnsureTimeError,
    EnsureZonedDateTimeError,
    ensure_bool,
    ensure_bytes,
    ensure_class,
    ensure_date,
    ensure_float,
    ensure_int,
    ensure_member,
    ensure_not_none,
    ensure_number,
    ensure_path,
    ensure_plain_date_time,
    ensure_str,
    ensure_time,
    ensure_time_delta,
    ensure_zoned_date_time,
    yield_object_attributes,
)
from utilities.text import parse_bool

if TYPE_CHECKING:
    import datetime as dt

    from whenever import PlainDateTime, TimeDelta, ZonedDateTime

    from utilities.types import Number


class TestEnsureBool:
    @given(case=sampled_from([(True, False), (True, True), (None, True)]))
    def test_main(self, *, case: tuple[bool | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_bool(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a boolean"),
            (True, "Object '.*' of type '.*' must be a boolean or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureBoolError, match=match):
            _ = ensure_bool(sentinel, nullable=nullable)


class TestEnsureBytes:
    @given(case=sampled_from([(b"", False), (b"", True), (None, True)]))
    def test_main(self, *, case: tuple[bytes | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_bytes(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a byte string"),
            (True, "Object '.*' of type '.*' must be a byte string or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureBytesError, match=match):
            _ = ensure_bytes(sentinel, nullable=nullable)


class TestEnsureClass:
    @given(
        case=sampled_from([
            (True, bool, False),
            (True, bool, True),
            (True, (bool,), False),
            (True, (bool,), True),
            (None, bool, True),
        ])
    )
    def test_main(self, *, case: tuple[Any, Any, bool]) -> None:
        obj, cls, nullable = case
        _ = ensure_class(obj, cls, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be an instance of '.*'"),
            (True, "Object '.*' of type '.*' must be an instance of '.*' or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureClassError, match=match):
            _ = ensure_class(sentinel, bool, nullable=nullable)


class TestEnsureDate:
    @given(case=sampled_from([(get_today(), False), (get_today(), True), (None, True)]))
    def test_main(self, *, case: tuple[dt.date | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_date(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a date"),
            (True, "Object '.*' of type '.*' must be a date or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureDateError, match=match):
            _ = ensure_date(sentinel, nullable=nullable)


class TestEnsureFloat:
    @given(case=sampled_from([(0.0, False), (0.0, True), (None, True)]))
    def test_main(self, *, case: tuple[float | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_float(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a float"),
            (True, "Object '.*' of type '.*' must be a float or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureFloatError, match=match):
            _ = ensure_float(sentinel, nullable=nullable)


class TestEnsureInt:
    @given(case=sampled_from([(0, False), (0, True), (None, True)]))
    def test_main(self, *, case: tuple[int | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_int(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be an integer"),
            (True, "Object '.*' of type '.*' must be an integer or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureIntError, match=match):
            _ = ensure_int(sentinel, nullable=nullable)


class TestEnsureMember:
    @given(
        case=sampled_from([
            (True, True),
            (True, False),
            (False, True),
            (False, False),
            (None, True),
        ])
    )
    def test_main(self, *, case: tuple[Any, bool]) -> None:
        obj, nullable = case
        _ = ensure_member(obj, {True, False}, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object .* must be a member of .*"),
            (True, "Object .* must be a member of .* or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureMemberError, match=match):
            _ = ensure_member(sentinel, {True, False}, nullable=nullable)


class TestEnsureNotNone:
    def test_main(self) -> None:
        maybe_int = cast("int | None", 0)
        result = ensure_not_none(maybe_int)
        assert result == 0

    def test_error(self) -> None:
        with raises(EnsureNotNoneError, match=r"Object must not be None"):
            _ = ensure_not_none(None)

    def test_error_with_desc(self) -> None:
        with raises(EnsureNotNoneError, match=r"Name must not be None"):
            _ = ensure_not_none(None, desc="Name")


class TestEnsureNumber:
    @given(case=sampled_from([(0, False), (0.0, False), (0.0, True), (None, True)]))
    def test_main(self, *, case: tuple[Number, bool]) -> None:
        obj, nullable = case
        _ = ensure_number(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a number"),
            (True, "Object '.*' of type '.*' must be a number or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureNumberError, match=match):
            _ = ensure_number(sentinel, nullable=nullable)


class TestEnsurePath:
    @given(case=sampled_from([(HOME, False), (HOME, True), (None, True)]))
    def test_main(self, *, case: tuple[int | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_path(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a Path"),
            (True, "Object '.*' of type '.*' must be a Path or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsurePathError, match=match):
            _ = ensure_path(sentinel, nullable=nullable)


class TestEnsurePlainDateTime:
    @given(
        case=sampled_from([
            (NOW_UTC.to_plain(), False),
            (NOW_UTC.to_plain(), True),
            (None, True),
        ])
    )
    def test_main(self, *, case: tuple[PlainDateTime | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_plain_date_time(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a plain date-time"),
            (True, "Object '.*' of type '.*' must be a plain date-time or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsurePlainDateTimeError, match=match):
            _ = ensure_plain_date_time(sentinel, nullable=nullable)


class TestEnsureStr:
    @given(case=sampled_from([("", False), ("", True), (None, True)]))
    def test_main(self, *, case: tuple[bool | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_str(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a string"),
            (True, "Object '.*' of type '.*' must be a string or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureStrError, match=match):
            _ = ensure_str(sentinel, nullable=nullable)


class TestEnsureTime:
    @given(
        case=sampled_from([
            (get_now().time(), False),
            (get_now().time(), True),
            (None, True),
        ])
    )
    def test_main(self, *, case: tuple[dt.time | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_time(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a time"),
            (True, "Object '.*' of type '.*' must be a time or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureTimeError, match=match):
            _ = ensure_time(sentinel, nullable=nullable)


class TestEnsureTimeDelta:
    @given(case=sampled_from([(ZERO_TIME, False), (ZERO_TIME, True), (None, True)]))
    def test_main(self, *, case: tuple[TimeDelta | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_time_delta(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a time-delta"),
            (True, "Object '.*' of type '.*' must be a time-delta or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureTimeDeltaError, match=match):
            _ = ensure_time_delta(sentinel, nullable=nullable)


class TestEnsureZonedDateTime:
    @given(case=sampled_from([(NOW_UTC, False), (NOW_UTC, True), (None, True)]))
    def test_main(self, *, case: tuple[ZonedDateTime | None, bool]) -> None:
        obj, nullable = case
        _ = ensure_zoned_date_time(obj, nullable=nullable)

    @given(
        case=sampled_from([
            (False, "Object '.*' of type '.*' must be a zoned date-time"),
            (True, "Object '.*' of type '.*' must be a zoned date-time or None"),
        ])
    )
    def test_error(self, *, case: tuple[bool, str]) -> None:
        nullable, match = case
        with raises(EnsureZonedDateTimeError, match=match):
            _ = ensure_zoned_date_time(sentinel, nullable=nullable)


class TestSkipIfOptimize:
    @mark.parametrize("optimize", [param(True), param(False)])
    def test_main(self, *, optimize: bool) -> None:
        code = normalize_multi_line_str("""
            from utilities.functions import skip_if_optimize

            is_run = False

            @skip_if_optimize
            def func() -> None:
                global is_run
                is_run = True

            func()
            print(is_run)
        """)

        args = [executable]
        if optimize:
            args.append("-O")
        args.extend(["-c", code])
        is_run = parse_bool(check_output(args, text=True))
        assert is_run is not optimize


class TestYieldObjectAttributes:
    @given(n=integers())
    def test_main(self, *, n: int) -> None:
        class Example:
            attr: ClassVar[int] = n

        attrs = dict(yield_object_attributes(Example))
        assert len(attrs) == approx(29, rel=0.1)
        assert attrs["attr"] == n
