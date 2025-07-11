from __future__ import annotations

from itertools import pairwise
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any, cast

from hypothesis import Phase, assume, given, settings
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    floats,
    integers,
    just,
    none,
    sets,
    timezones,
)
from libcst import Import, ImportFrom
from numpy import inf, int64, isfinite, isinf, isnan, ravel, rint
from pathvalidate import validate_filepath
from pytest import mark, raises
from whenever import (
    Date,
    DateDelta,
    DateTimeDelta,
    MonthDay,
    PlainDateTime,
    Time,
    TimeDelta,
    TimeZoneNotFoundError,
    YearMonth,
    ZonedDateTime,
)

from utilities.functions import ensure_int
from utilities.hypothesis import (
    Shape,
    _Draw2DefaultGeneratedSentinelError,
    _Draw2InputResolvedToSentinelError,
    assume_does_not_raise,
    bool_arrays,
    date_deltas,
    date_time_deltas,
    dates,
    draw2,
    float32s,
    float64s,
    float_arrays,
    floats_extra,
    git_repos,
    hashables,
    import_froms,
    imports,
    int32s,
    int64s,
    int_arrays,
    lists_fixed_length,
    month_days,
    numbers,
    pairs,
    paths,
    plain_datetimes,
    random_states,
    sentinels,
    sets_fixed_length,
    settings_with_reduced_examples,
    setup_hypothesis_profiles,
    slices,
    str_arrays,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_ascii_lower,
    text_ascii_upper,
    text_clean,
    text_digits,
    text_printable,
    time_deltas,
    times,
    triples,
    uint32s,
    uint64s,
    versions,
    year_months,
    zoned_datetimes,
)
from utilities.iterables import one
from utilities.libcst import parse_import
from utilities.math import (
    MAX_FLOAT32,
    MAX_FLOAT64,
    MAX_INT32,
    MAX_INT64,
    MAX_UINT32,
    MAX_UINT64,
    MIN_FLOAT32,
    MIN_FLOAT64,
    MIN_INT32,
    MIN_INT64,
    MIN_UINT32,
    MIN_UINT64,
)
from utilities.os import temp_environ
from utilities.platform import maybe_yield_lower_case
from utilities.sentinel import Sentinel
from utilities.version import Version
from utilities.whenever import (
    DATE_TWO_DIGIT_YEAR_MAX,
    DATE_TWO_DIGIT_YEAR_MIN,
    to_days,
    to_nanoseconds,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Set as AbstractSet
    from zoneinfo import ZoneInfo

    from utilities.tempfile import TemporaryDirectory
    from utilities.types import Number


class TestAssumeDoesNotRaise:
    @given(x=booleans())
    def test_no_match_and_suppressed(self, *, x: bool) -> None:
        with assume_does_not_raise(ValueError):
            if x is True:
                msg = "x is True"
                raise ValueError(msg)
        assert x is False

    @given(x=booleans())
    def test_no_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with raises(ValueError, match=msg), assume_does_not_raise(RuntimeError):
                raise ValueError(msg)

    @given(x=booleans())
    def test_with_match_and_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with assume_does_not_raise(ValueError, match=msg):
                raise ValueError(msg)
        assert x is False

    @given(x=just(value=True))
    def test_with_match_and_not_suppressed(self, *, x: bool) -> None:
        msg = "x is True"
        if x is True:
            with (
                raises(ValueError, match=msg),
                assume_does_not_raise(ValueError, match="wrong"),
            ):
                raise ValueError(msg)


class TestBoolArrays:
    @given(data=data(), shape=array_shapes())
    def test_main(self, *, data: DataObject, shape: Shape) -> None:
        array = data.draw(bool_arrays(shape=shape))
        assert array.dtype == bool
        assert array.shape == shape


class TestDateDeltas:
    @given(data=data(), parsable=booleans())
    def test_main(self, *, data: DataObject, parsable: bool) -> None:
        min_value = data.draw(date_deltas() | none())
        max_value = data.draw(date_deltas() | none())
        with assume_does_not_raise(InvalidArgument):
            delta = data.draw(
                date_deltas(min_value=min_value, max_value=max_value, parsable=parsable)
            )
        assert isinstance(delta, DateDelta)
        days = to_days(delta)
        if min_value is not None:
            assert days >= to_days(min_value)
        if max_value is not None:
            assert days <= to_days(max_value)
        if parsable:
            assert DateDelta.parse_common_iso(delta.format_common_iso()) == delta


class TestDateTimeDeltas:
    @given(data=data(), parsable=booleans())
    def test_main(self, *, data: DataObject, parsable: bool) -> None:
        min_value = data.draw(date_time_deltas() | none())
        max_value = data.draw(date_time_deltas() | none())
        with assume_does_not_raise(InvalidArgument):
            delta = data.draw(
                date_time_deltas(
                    min_value=min_value, max_value=max_value, parsable=parsable
                )
            )
        assert isinstance(delta, DateTimeDelta)
        nanos = to_nanoseconds(delta)
        if min_value is not None:
            assert nanos >= to_nanoseconds(min_value)
        if max_value is not None:
            assert nanos <= to_nanoseconds(max_value)
        if parsable:
            assert DateTimeDelta.parse_common_iso(delta.format_common_iso()) == delta


class TestDates:
    @given(data=data(), two_digit=booleans())
    def test_main(self, *, data: DataObject, two_digit: bool) -> None:
        min_value = data.draw(dates() | none())
        max_value = data.draw(dates() | none())
        with assume_does_not_raise(InvalidArgument):
            date = data.draw(
                dates(min_value=min_value, max_value=max_value, two_digit=two_digit)
            )
        assert isinstance(date, Date)
        assert Date.parse_common_iso(date.format_common_iso()) == date
        if min_value is not None:
            assert date >= min_value
        if max_value is not None:
            assert date <= max_value
        if two_digit:
            assert DATE_TWO_DIGIT_YEAR_MIN <= date <= DATE_TWO_DIGIT_YEAR_MAX


class TestDraw2:
    @given(data=data())
    def test_none_no_default(self, *, data: DataObject) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> None:
            maybe_none = draw(none() | just(none()))
            return draw2(draw, maybe_none)

        result = data.draw(strategy())
        assert result is None

    @given(data=data())
    def test_none_with_default_no_sentinel(self, *, data: DataObject) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> bool:
            maybe_none = draw(none() | just(none()))
            return draw2(draw, maybe_none, booleans())

        result = data.draw(strategy())
        assert isinstance(result, bool)

    @given(data=data())
    def test_none_with_default_with_sentinel(self, *, data: DataObject) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> bool | None:
            maybe_none = draw(none() | just(none()))
            return draw2(draw, maybe_none, booleans(), sentinel=True)

        result = data.draw(strategy())
        assert result is None

    @given(data=data(), value=booleans())
    def test_sentinel_with_default(self, *, data: DataObject, value: bool) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> bool | None:
            return draw2(draw, sentinels(), just(value), sentinel=True)

        result = data.draw(strategy())
        assert result is value

    @given(data=data(), value=booleans(), sentinel=booleans())
    def test_value(self, *, data: DataObject, value: bool, sentinel: bool) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> bool | None:
            maybe_value = draw(just(value) | just(just(value)))
            maybe_default = draw(just(booleans()) | none())
            return draw2(draw, maybe_value, maybe_default, sentinel=sentinel)

        result = data.draw(strategy())
        assert result is value

    @given(data=data(), sentinel=booleans())
    def test_error_input_resolved_to_sentinel_no_default(
        self, *, data: DataObject, sentinel: bool
    ) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> Sentinel:
            return draw2(draw, sentinels(), sentinel=sentinel)

        with raises(
            _Draw2InputResolvedToSentinelError,
            match="The input resolved to the sentinel value; a default strategy is needed",
        ):
            _ = data.draw(strategy())

    @given(data=data())
    def test_error_input_resolved_to_sentinel_with_default(
        self, *, data: DataObject
    ) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> Sentinel:
            return draw2(draw, sentinels(), sentinels())

        with raises(
            _Draw2InputResolvedToSentinelError,
            match="The input resolved to the sentinel value; a default strategy is needed",
        ):
            _ = data.draw(strategy())

    @given(data=data())
    def test_error_default_generated_sentinel_with_none(
        self, *, data: DataObject
    ) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> Sentinel:
            maybe_none = draw(none() | just(none()))
            return draw2(draw, maybe_none, sentinels())

        with raises(
            _Draw2DefaultGeneratedSentinelError,
            match="The default search strategy generated the sentinel value",
        ):
            _ = data.draw(strategy())

    @given(data=data())
    def test_error_default_generated_sentinel_with_sentinel(
        self, *, data: DataObject
    ) -> None:
        @composite
        def strategy(draw: DrawFn, /) -> Any:
            return draw2(draw, sentinels(), sentinels(), sentinel=True)

        with raises(
            _Draw2DefaultGeneratedSentinelError,
            match="The default search strategy generated the sentinel value",
        ):
            _ = data.draw(strategy())


class TestFloat32s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(float32s(), sorted=True))
        x = data.draw(float32s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_FLOAT32) <= x <= min(max_value, MAX_FLOAT32)


class TestFloat64s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(float64s(), sorted=True))
        x = data.draw(float64s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_FLOAT64) <= x <= min(max_value, MAX_FLOAT64)


class TestFloatArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                float_arrays(
                    shape=shape,
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                    unique=unique,
                )
            )
        assert array.dtype == float
        assert array.shape == shape
        if min_value is not None:
            assert ((isfinite(array) & (array >= min_value)) | ~isfinite(array)).all()
        if max_value is not None:
            assert ((isfinite(array) & (array <= max_value)) | ~isfinite(array)).all()
        if not allow_nan:
            assert (~isnan(array)).all()
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert (~isinf(array)).all()
            if not allow_pos_inf:
                assert (array != inf).all()
            if not allow_neg_inf:
                assert (array != -inf).all()
        if integral:
            assert ((array == rint(array)) | isnan(array)).all()
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestFloatsExtra:
    @given(
        data=data(),
        min_value=floats() | none(),
        max_value=floats() | none(),
        allow_nan=booleans(),
        allow_inf=booleans(),
        allow_pos_inf=booleans(),
        allow_neg_inf=booleans(),
        integral=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_value: float | None,
        max_value: float | None,
        allow_nan: bool,
        allow_inf: bool,
        allow_pos_inf: bool,
        allow_neg_inf: bool,
        integral: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=allow_nan,
                    allow_inf=allow_inf,
                    allow_pos_inf=allow_pos_inf,
                    allow_neg_inf=allow_neg_inf,
                    integral=integral,
                )
            )
        if min_value is not None:
            assert (isfinite(x) and x >= min_value) or not isfinite(x)
        if max_value is not None:
            assert (isfinite(x) and x <= max_value) or not isfinite(x)
        if not allow_nan:
            assert not isnan(x)
        if not allow_inf:
            if not (allow_pos_inf or allow_neg_inf):
                assert not isinf(x)
            if not allow_pos_inf:
                assert x != inf
            if not allow_neg_inf:
                assert x != -inf
        if integral:
            assert (isfinite(x) and x == round(x)) or not isfinite(x)

    @given(data=data(), min_value=floats() | none(), max_value=floats() | none())
    def test_finite_and_integral(
        self, *, data: DataObject, min_value: float | None, max_value: float | None
    ) -> None:  # hard to reach
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                floats_extra(
                    min_value=min_value,
                    max_value=max_value,
                    allow_nan=False,
                    allow_inf=False,
                    allow_pos_inf=False,
                    allow_neg_inf=False,
                    integral=True,
                )
            )
        assert isfinite(x)
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value
        assert x == round(x)


class TestGitRepos:
    @given(data=data())
    @settings_with_reduced_examples()
    def test_main(self, *, data: DataObject) -> None:
        root = data.draw(git_repos())
        files = set(root.iterdir())
        assert Path(root, ".git") in files


class TestHashables:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        x = data.draw(hashables())
        _ = hash(x)


class TestImports:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_depth = data.draw(integers(0, 10) | none())
        max_depth = data.draw(integers(0, 10) | none())
        with assume_does_not_raise(InvalidArgument):
            imp = data.draw(imports(min_depth=min_depth, max_depth=max_depth))
        assert isinstance(imp, Import)
        parsed = one(parse_import(imp))
        if min_depth is not None:
            assert len(parsed.module.split(".")) >= min_depth
        if max_depth is not None:
            assert len(parsed.module.split(".")) <= max_depth


class TestImportFroms:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_depth = data.draw(integers(0, 10) | none())
        max_depth = data.draw(integers(0, 10) | none())
        with assume_does_not_raise(InvalidArgument):
            imp = data.draw(import_froms(min_depth=min_depth, max_depth=max_depth))
        assert isinstance(imp, ImportFrom)
        parsed = one(parse_import(imp))
        if min_depth is not None:
            assert len(parsed.module.split(".")) >= min_depth
        if max_depth is not None:
            assert len(parsed.module.split(".")) <= max_depth


class TestIntArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=int64s(),
        max_value=int64s(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_value: int,
        max_value: int,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                int_arrays(
                    shape=shape, min_value=min_value, max_value=max_value, unique=unique
                )
            )
        assert array.dtype == int64
        assert array.shape == shape
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestInt32s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(int32s(), sorted=True))
        x = data.draw(int32s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_INT32) <= x <= min(max_value, MAX_INT32)


class TestInt64s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(int64s(), sorted=True))
        x = data.draw(int64s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_INT64) <= x <= min(max_value, MAX_INT64)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10), unique=booleans(), sorted_=booleans())
    def test_main(
        self, *, data: DataObject, size: int, unique: bool, sorted_: bool
    ) -> None:
        result = data.draw(
            lists_fixed_length(integers(), size, unique=unique, sorted=sorted_)
        )
        assert isinstance(result, list)
        assert len(result) == size
        if unique:
            assert len(set(result)) == len(result)
        if sorted_:
            assert sorted(result) == result


class TestMonthDays:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value = data.draw(month_days() | none())
        max_value = data.draw(month_days() | none())
        with assume_does_not_raise(InvalidArgument):
            month_day = data.draw(month_days(min_value=min_value, max_value=max_value))
        assert isinstance(month_day, MonthDay)
        assert MonthDay.parse_common_iso(month_day.format_common_iso()) == month_day
        if min_value is not None:
            assert month_day >= min_value
        if max_value is not None:
            assert month_day <= max_value


class TestNumbers:
    @given(data=data(), min_value=numbers() | none(), max_value=numbers() | none())
    def test_main(
        self, *, data: DataObject, min_value: Number | None, max_value: Number | None
    ) -> None:
        if min_value is not None:
            _ = assume(min_value == float(min_value))
        if max_value is not None:
            _ = assume(max_value == float(max_value))
        if (min_value is not None) and (max_value is not None):
            _ = assume(min_value <= max_value)
        x = data.draw(numbers(min_value=min_value, max_value=max_value))
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestPairs:
    @given(data=data(), unique=booleans(), sorted_=booleans())
    def test_main(self, *, data: DataObject, unique: bool, sorted_: bool) -> None:
        result = data.draw(pairs(integers(), unique=unique, sorted=sorted_))
        assert isinstance(result, tuple)
        assert len(result) == 2
        first, second = result
        if unique:
            assert first != second
        if sorted_:
            assert first <= second


class TestPaths:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_depth = data.draw(integers(0, 5) | none())
        max_depth = data.draw(integers(0, 5) | none())
        with assume_does_not_raise(InvalidArgument):
            path = data.draw(paths(min_depth=min_depth, max_depth=max_depth))
        assert isinstance(path, Path)
        assert not path.is_absolute()
        validate_filepath(str(path))
        if min_depth is not None:
            assert len(path.parts) >= min_depth
        if max_depth is not None:
            assert len(path.parts) <= max_depth


class TestPlainDateTimes:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value = data.draw(plain_datetimes() | none())
        max_value = data.draw(plain_datetimes() | none())
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(
                plain_datetimes(min_value=min_value, max_value=max_value)
            )
        assert isinstance(datetime, PlainDateTime)
        assert PlainDateTime.parse_common_iso(datetime.format_common_iso()) == datetime
        if min_value is not None:
            assert datetime >= min_value
        if max_value is not None:
            assert datetime <= max_value


class TestRandomStates:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        _ = data.draw(random_states())


class TestReducedExamples:
    @given(frac=floats(0.0, 10.0))
    def test_main(self, *, frac: float) -> None:
        @settings_with_reduced_examples(frac)
        def test() -> None:
            pass

        result = cast("Any", test)._hypothesis_internal_use_settings.max_examples
        expected = max(round(frac * ensure_int(settings().max_examples)), 1)
        assert result == expected


class TestSentinels:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        sentinel = data.draw(sentinels())
        assert isinstance(sentinel, Sentinel)


class TestSetsFixedLength:
    @given(data=data(), size=integers(1, 10))
    def test_main(self, *, data: DataObject, size: int) -> None:
        result = data.draw(sets_fixed_length(integers(), size))
        assert isinstance(result, set)
        assert len(result) == size


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
        curr = settings()
        assert Phase.shrink in cast("Iterable[Phase]", curr.phases)
        assert curr.max_examples in {10, 100, 1000}

    def test_no_shrink(self) -> None:
        with temp_environ({"HYPOTHESIS_NO_SHRINK": "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in cast("Iterable[Phase]", settings().phases)

    @given(max_examples=integers(1, 100))
    def test_max_examples(self, *, max_examples: int) -> None:
        with temp_environ({"HYPOTHESIS_MAX_EXAMPLES": str(max_examples)}):
            setup_hypothesis_profiles()
        assert settings().max_examples == max_examples


class TestSlices:
    @given(data=data(), iter_len=integers(0, 10))
    def test_main(self, *, data: DataObject, iter_len: int) -> None:
        slice_len = data.draw(integers(0, iter_len) | none())
        slice_ = data.draw(slices(iter_len, slice_len=slice_len))
        range_slice = range(iter_len)[slice_]
        assert all(i + 1 == j for i, j in pairwise(range_slice))
        if slice_len is not None:
            assert len(range_slice) == slice_len

    @given(data=data(), iter_len=integers(0, 10))
    def test_error(self, *, data: DataObject, iter_len: int) -> None:
        with raises(
            InvalidArgument, match=r"Slice length \d+ exceeds iterable length \d+"
        ):
            _ = data.draw(slices(iter_len, slice_len=iter_len + 1))


class TestStrArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        allow_none=booleans(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_size: int,
        max_size: int | None,
        allow_none: bool,
        unique: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            array = data.draw(
                str_arrays(
                    shape=shape,
                    min_size=min_size,
                    max_size=max_size,
                    allow_none=allow_none,
                    unique=unique,
                )
            )
        assert array.dtype == object
        assert array.shape == shape
        flat = ravel(array)
        flat_text = [i for i in flat if i is not None]
        assert all(len(t) >= min_size for t in flat_text)
        if max_size is not None:
            assert all(len(t) <= max_size for t in flat_text)
        if not allow_none:
            assert len(flat_text) == array.size
        if unique:
            flat = ravel(array)
            assert len(set(flat)) == len(flat)


class TestTempDirs:
    @given(temp_dir=temp_dirs())
    def test_main(self, *, temp_dir: TemporaryDirectory) -> None:
        path = temp_dir.path
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(temp_dir=temp_dirs(), contents=sets(text_ascii(min_size=1), max_size=10))
    def test_writing_files(
        self, *, temp_dir: TemporaryDirectory, contents: AbstractSet[str]
    ) -> None:
        path = temp_dir.path
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            Path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTempPaths:
    @given(path=temp_paths())
    def test_main(self, *, path: Path) -> None:
        assert path.is_dir()
        assert len(set(path.iterdir())) == 0

    @given(path=temp_paths(), contents=sets(text_ascii(min_size=1), max_size=10))
    @mark.flaky
    def test_writing_files(self, *, path: Path, contents: AbstractSet[str]) -> None:
        assert len(set(path.iterdir())) == 0
        as_set = set(maybe_yield_lower_case(contents))
        for content in as_set:
            Path(path, content).touch()
        assert len(set(path.iterdir())) == len(as_set)


class TestTextAscii:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_ascii(min_size=min_size, max_size=max_size))
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextAsciiLower:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_ascii_lower(min_size=min_size, max_size=max_size))
        assert search("^[a-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextAsciiUpper:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_ascii_upper(min_size=min_size, max_size=max_size))
        assert search("^[A-Z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextClean:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_clean(min_size=min_size, max_size=max_size))
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextDigits:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_digits(min_size=min_size, max_size=max_size))
        assert search("^[0-9]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTextPrintable:
    @given(data=data(), min_size=integers(0, 100), max_size=integers(0, 100) | none())
    def test_main(
        self, *, data: DataObject, min_size: int, max_size: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(text_printable(min_size=min_size, max_size=max_size))
        assert search(r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size


class TestTimeDeltas:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value = data.draw(time_deltas() | none())
        max_value = data.draw(time_deltas() | none())
        with assume_does_not_raise(InvalidArgument):
            delta = data.draw(time_deltas(min_value=min_value, max_value=max_value))
        assert isinstance(delta, TimeDelta)
        assert TimeDelta.parse_common_iso(delta.format_common_iso()) == delta
        if min_value is not None:
            assert delta >= min_value
        if max_value is not None:
            assert delta <= max_value


class TestTimes:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value = data.draw(times() | none())
        max_value = data.draw(times() | none())
        with assume_does_not_raise(InvalidArgument):
            time = data.draw(times(min_value=min_value, max_value=max_value))
        assert isinstance(time, Time)
        assert Time.parse_common_iso(time.format_common_iso()) == time
        if min_value is not None:
            assert time >= min_value
        if max_value is not None:
            assert time <= max_value


class TestTriples:
    @given(data=data(), unique=booleans(), sorted_=booleans())
    def test_main(self, *, data: DataObject, unique: bool, sorted_: bool) -> None:
        result = data.draw(triples(integers(), unique=unique, sorted=sorted_))
        assert isinstance(result, tuple)
        assert len(result) == 3
        first, second, third = result
        if unique:
            assert first != second
            assert second != third
        if sorted_:
            assert first <= second <= third


class TestUInt32s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(uint32s(), sorted=True))
        x = data.draw(uint32s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_UINT32) <= x <= min(max_value, MAX_UINT32)


class TestUInt64s:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        min_value, max_value = data.draw(pairs(uint64s(), sorted=True))
        x = data.draw(uint64s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_UINT64) <= x <= min(max_value, MAX_UINT64)


class TestVersions:
    @given(data=data(), suffix=booleans())
    def test_main(self, *, data: DataObject, suffix: bool) -> None:
        version = data.draw(versions(suffix=suffix))
        assert isinstance(version, Version)
        if suffix:
            assert version.suffix is not None
        else:
            assert version.suffix is None


class TestYearMonths:
    @given(data=data(), two_digit=booleans())
    def test_main(self, *, data: DataObject, two_digit: bool) -> None:
        min_value = data.draw(year_months() | none())
        max_value = data.draw(year_months() | none())
        with assume_does_not_raise(InvalidArgument):
            year_month = data.draw(
                year_months(
                    min_value=min_value, max_value=max_value, two_digit=two_digit
                )
            )
        assert isinstance(year_month, YearMonth)
        assert YearMonth.parse_common_iso(year_month.format_common_iso()) == year_month
        if min_value is not None:
            assert year_month >= min_value
        if max_value is not None:
            assert year_month <= max_value
        if two_digit:
            assert year_month.on_day(1) >= DATE_TWO_DIGIT_YEAR_MIN
            assert year_month.on_day(28) <= DATE_TWO_DIGIT_YEAR_MAX


class TestZonedDateTimes:
    @given(data=data(), time_zone=timezones())
    def test_main(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        min_value = data.draw(zoned_datetimes() | none())
        max_value = data.draw(zoned_datetimes() | none())
        with assume_does_not_raise(InvalidArgument):
            datetime = data.draw(
                zoned_datetimes(
                    min_value=min_value, max_value=max_value, time_zone=time_zone
                )
            )
        assert isinstance(datetime, ZonedDateTime)
        _ = datetime.py_datetime()
        assert ZonedDateTime.parse_common_iso(datetime.format_common_iso()) == datetime
        assert datetime.tz == time_zone.key
        if min_value is not None:
            assert datetime >= min_value
        if max_value is not None:
            assert datetime <= max_value

    @given(data=data(), time_zone=timezones())
    def test_examples(self, *, data: DataObject, time_zone: ZoneInfo) -> None:
        with assume_does_not_raise(TimeZoneNotFoundError):
            max_value = ZonedDateTime(1, 1, 2, tz=time_zone.key)
        datetime = data.draw(zoned_datetimes(max_value=max_value, time_zone=time_zone))
        _ = datetime.py_datetime()
