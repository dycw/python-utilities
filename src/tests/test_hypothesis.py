from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from itertools import pairwise
from pathlib import Path
from re import search
from subprocess import PIPE, check_output
from typing import TYPE_CHECKING, Any, cast

from hypothesis import HealthCheck, Phase, assume, given, settings
from hypothesis.errors import InvalidArgument
from hypothesis.extra.numpy import array_shapes
from hypothesis.strategies import (
    DataObject,
    DrawFn,
    booleans,
    composite,
    data,
    datetimes,
    floats,
    integers,
    just,
    none,
    sampled_from,
    sets,
    timedeltas,
    timezones,
    uuids,
)
from luigi import Task
from numpy import inf, int64, isfinite, isinf, isnan, ravel, rint
from pathvalidate import validate_filepath
from pytest import raises
from sqlalchemy import Column, Integer, MetaData, Table, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.conftest import FLAKY, SKIPIF_CI_AND_NOT_LINUX, SKIPIF_CI_AND_WINDOWS
from utilities.datetime import (
    date_duration_to_timedelta,
    datetime_duration_to_float,
    datetime_duration_to_timedelta,
    is_integral_timedelta,
    is_local_datetime,
    is_zoned_datetime,
)
from utilities.functions import ensure_int
from utilities.git import (
    _GIT_REMOTE_GET_URL_ORIGIN,
    _GIT_REV_PARSE_ABBREV_REV_HEAD,
    _GIT_TAG_POINTS_AT,
    MASTER,
)
from utilities.hypothesis import (
    _SQLALCHEMY_ENGINE_DIALECTS,
    _ZONED_DATETIMES_LEFT_MOST,
    _ZONED_DATETIMES_RIGHT_MOST,
    MaybeSearchStrategy,
    Shape,
    _lift_draw,
    assume_does_not_raise,
    bool_arrays,
    date_durations,
    datetime_durations,
    draw2,
    draw_non_null_element,
    float_arrays,
    floats_extra,
    git_repos,
    hashables,
    int32s,
    int64s,
    int_arrays,
    lists_fixed_length,
    months,
    namespace_mixins,
    numbers,
    pairs,
    paths,
    random_states,
    sets_fixed_length,
    settings_with_reduced_examples,
    setup_hypothesis_profiles,
    slices,
    sqlalchemy_engines,
    str_arrays,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_ascii_lower,
    text_ascii_upper,
    text_clean,
    text_digits,
    text_printable,
    timedeltas_2w,
    triples,
    uint32s,
    uint64s,
    versions,
    yield_test_redis,
    zoned_datetimes,
)
from utilities.math import (
    MAX_INT32,
    MAX_INT64,
    MAX_UINT32,
    MAX_UINT64,
    MIN_INT32,
    MIN_INT64,
    MIN_UINT32,
    MIN_UINT64,
    is_at_least,
    is_at_most,
)
from utilities.os import temp_environ
from utilities.platform import maybe_yield_lower_case
from utilities.sqlalchemy import Dialect, _get_dialect
from utilities.types import Duration, Number
from utilities.version import Version
from utilities.whenever import (
    MAX_SERIALIZABLE_TIMEDELTA,
    MIN_SERIALIZABLE_TIMEDELTA,
    check_valid_zoned_datetime,
    parse_duration,
    parse_timedelta,
    serialize_duration,
    serialize_timedelta,
)

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet
    from uuid import UUID
    from zoneinfo import ZoneInfo

    from utilities.datetime import Month
    from utilities.tempfile import TemporaryDirectory


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


class TestDateDurations:
    @given(
        data=data(),
        min_int=integers() | none(),
        max_int=integers() | none(),
        min_timedelta=timedeltas() | none(),
        max_timedelta=timedeltas() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_int: int | None,
        max_int: int | None,
        min_timedelta: dt.timedelta | None,
        max_timedelta: dt.timedelta | None,
    ) -> None:
        duration = data.draw(
            date_durations(
                min_int=min_int,
                max_int=max_int,
                min_timedelta=min_timedelta,
                max_timedelta=max_timedelta,
            )
        )
        assert isinstance(duration, Duration)
        match duration:
            case int():
                if min_int is not None:
                    assert duration >= min_int
                if max_int is not None:
                    assert duration <= max_int
                if min_timedelta is not None:
                    assert date_duration_to_timedelta(duration) >= min_timedelta
                if max_timedelta is not None:
                    assert date_duration_to_timedelta(duration) <= max_timedelta
            case float():
                assert duration == round(duration)
                if min_int is not None:
                    assert duration >= min_int
                if max_int is not None:
                    assert duration <= max_int
                if min_timedelta is not None:
                    assert date_duration_to_timedelta(duration) >= min_timedelta
                if max_timedelta is not None:
                    assert date_duration_to_timedelta(duration) <= max_timedelta
            case dt.timedelta():
                assert is_integral_timedelta(duration)
                if min_int is not None:
                    assert duration >= date_duration_to_timedelta(min_int)
                if max_int is not None:
                    assert duration <= date_duration_to_timedelta(max_int)
                if min_timedelta is not None:
                    assert duration >= min_timedelta
                if max_timedelta is not None:
                    assert duration <= max_timedelta

    @given(data=data())
    def test_two_way(self, *, data: DataObject) -> None:
        duration = data.draw(date_durations(two_way=True))
        ser = serialize_duration(duration)
        _ = parse_duration(ser)


class TestDateTimeDurations:
    @given(
        data=data(),
        min_number=numbers() | none(),
        max_number=numbers() | none(),
        min_timedelta=timedeltas() | none(),
        max_timedelta=timedeltas() | none(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_number: Number | None,
        max_number: Number | None,
        min_timedelta: dt.timedelta | None,
        max_timedelta: dt.timedelta | None,
    ) -> None:
        duration = data.draw(
            datetime_durations(
                min_number=min_number,
                max_number=max_number,
                min_timedelta=min_timedelta,
                max_timedelta=max_timedelta,
            )
        )
        assert isinstance(duration, Duration)
        match duration:
            case int() | float():
                if min_number is not None:
                    assert is_at_least(duration, min_number, abs_tol=1e-6)
                if max_number is not None:
                    assert is_at_most(duration, max_number, abs_tol=1e-6)
                if min_timedelta is not None:
                    assert is_at_least(
                        duration, datetime_duration_to_float(min_timedelta)
                    )
                if max_timedelta is not None:
                    assert is_at_most(
                        duration, datetime_duration_to_float(max_timedelta)
                    )
            case dt.timedelta():
                if min_number is not None:
                    assert duration >= datetime_duration_to_timedelta(min_number)
                if max_number is not None:
                    assert duration <= datetime_duration_to_timedelta(max_number)
                if min_timedelta is not None:
                    assert duration >= min_timedelta
                if max_timedelta is not None:
                    assert duration <= max_timedelta

    @given(data=data())
    def test_two_way(self, *, data: DataObject) -> None:
        duration = data.draw(datetime_durations(two_way=True))
        ser = serialize_duration(duration)
        _ = parse_duration(ser)


class TestDrawElement:
    @given(data=data(), value=booleans())
    def test_main(self, *, data: DataObject, value: bool) -> None:
        @composite
        def strategy(
            draw: DrawFn, /, *, maybe_value: MaybeSearchStrategy[bool] = value
        ) -> bool:
            return draw2(draw, maybe_value)

        result = data.draw(strategy())
        assert result is value


class TestDrawNonNullElement:
    @given(data=data(), value=booleans() | none())
    def test_main(self, *, data: DataObject, value: bool | None) -> None:
        @composite
        def strategy(
            draw: DrawFn, /, *, maybe_value: MaybeSearchStrategy[bool | None] = value
        ) -> bool:
            return draw_non_null_element(draw, maybe_value, booleans())

        result = data.draw(strategy())
        if isinstance(value, bool):
            assert result is value
        else:
            assert isinstance(result, bool)


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
        branch = data.draw(text_ascii(min_size=1) | none())
        remote = data.draw(text_ascii(min_size=1) | none())
        git_version = data.draw(versions() | none())
        hatch_version = data.draw(versions() | none())
        root = data.draw(
            git_repos(
                branch=branch,
                remote=remote,
                git_version=git_version,
                hatch_version=hatch_version,
            )
        )
        files = set(root.iterdir())
        assert Path(root, ".git") in files
        if branch is not None:
            output = check_output(
                _GIT_REV_PARSE_ABBREV_REV_HEAD, stderr=PIPE, cwd=root, text=True
            )
            assert output.strip("\n") == branch
        if remote is not None:
            output = check_output(
                _GIT_REMOTE_GET_URL_ORIGIN, stderr=PIPE, cwd=root, text=True
            )
            assert output.strip("\n") == remote
        if git_version is not None:
            output = check_output(
                [*_GIT_TAG_POINTS_AT, MASTER], stderr=PIPE, cwd=root, text=True
            )
            assert output.strip("\n") == str(git_version)
        if hatch_version is not None:
            assert {
                Path(root, "LICENSE.txt"),
                Path(root, "README.md"),
                Path(root, "pyproject.toml"),
                Path(root, "src"),
                Path(root, "tests"),
            }.issubset(files)
            output = check_output(
                ["hatch", "version"], stderr=PIPE, cwd=root, text=True
            )
            assert output.strip("\n") == str(hatch_version)

    @given(data=data())
    @settings(max_examples=1)
    def test_hatch_version_001(self, *, data: DataObject) -> None:
        root = data.draw(git_repos(hatch_version=Version(0, 0, 1)))
        output = check_output(["hatch", "version"], stderr=PIPE, cwd=root, text=True)
        assert output.strip("\n") == "0.0.1"


class TestHashables:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        x = data.draw(hashables())
        _ = hash(x)


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
    @given(data=data(), min_value=int32s(), max_value=int32s())
    def test_main(self, *, data: DataObject, min_value: int, max_value: int) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int32s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_INT32) <= x <= min(max_value, MAX_INT32)


class TestInt64s:
    @given(data=data(), min_value=int64s(), max_value=int64s())
    def test_main(self, *, data: DataObject, min_value: int, max_value: int) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int64s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_INT64) <= x <= min(max_value, MAX_INT64)


class TestLiftDraw:
    @given(data=data(), value=booleans())
    def test_fixed(self, *, data: DataObject, value: bool) -> None:
        @composite
        def strategy(_draw: DrawFn, /) -> bool:
            draw = _lift_draw(_draw)
            return draw(value)

        result = data.draw(strategy())
        assert result is value

    @given(data=data(), value=booleans())
    def test_strategy(self, *, data: DataObject, value: bool) -> None:
        @composite
        def strategy(_draw: DrawFn, /) -> bool:
            draw = _lift_draw(_draw)
            return draw(just(value))

        result = data.draw(strategy())
        assert result is value


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


class TestMonths:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        _ = data.draw(months())

    @given(data=data(), min_value=months(), max_value=months())
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_min_and_max_value(
        self, *, data: DataObject, min_value: Month, max_value: Month
    ) -> None:
        _ = assume(min_value <= max_value)
        month = data.draw(months(min_value=min_value, max_value=max_value))
        assert min_value <= month <= max_value


class TestNamespaceMixins:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        _ = data.draw(namespace_mixins())

    @given(namespace_mixin=namespace_mixins())
    def test_first(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = Example()

    @given(namespace_mixin=namespace_mixins())
    def test_second(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = Example()


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
        path = data.draw(paths())
        assert isinstance(path, Path)
        assert not path.is_absolute()
        validate_filepath(str(path))


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

        result = cast(Any, test)._hypothesis_internal_use_settings.max_examples
        expected = max(round(frac * ensure_int(settings().max_examples)), 1)
        assert result == expected


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
        assert Phase.shrink in cast(Iterable[Phase], curr.phases)
        assert curr.max_examples in {10, 100, 1000}

    def test_no_shrink(self) -> None:
        with temp_environ({"HYPOTHESIS_NO_SHRINK": "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in cast(Iterable[Phase], settings().phases)

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


class TestSQLAlchemyEngines:
    @FLAKY
    @given(
        data=data(),
        name=uuids(),
        dialect=_SQLALCHEMY_ENGINE_DIALECTS,
        ids=sets(integers(0, 10), min_size=1),
    )
    @settings(phases={Phase.generate})
    async def test_main(
        self, *, data: DataObject, name: UUID, dialect: Dialect, ids: set[int]
    ) -> None:
        table = Table(
            f"test_{name}", MetaData(), Column("id_", Integer, primary_key=True)
        )
        engine = await sqlalchemy_engines(data, table, dialect=dialect)
        assert isinstance(engine, AsyncEngine)
        assert _get_dialect(engine) == dialect
        if dialect == "sqlite":
            database = engine.url.database
            assert database is not None
            assert not Path(database).exists()
        async with engine.begin() as conn:
            await conn.run_sync(table.metadata.create_all)
        ins = insert(table).values([(id_,) for id_ in ids])
        async with engine.begin() as conn:
            _ = await conn.execute(ins)
        sel = select(table.c["id_"])
        async with engine.begin() as conn:
            results = (await conn.execute(sel)).scalars().all()
        assert set(results) == ids


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

    @FLAKY
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

    @FLAKY
    @given(path=temp_paths(), contents=sets(text_ascii(min_size=1), max_size=10))
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


class TestTimeDeltas2W:
    @given(
        data=data(),
        min_value=timedeltas(
            min_value=MIN_SERIALIZABLE_TIMEDELTA, max_value=MAX_SERIALIZABLE_TIMEDELTA
        ),
        max_value=timedeltas(
            min_value=MIN_SERIALIZABLE_TIMEDELTA, max_value=MAX_SERIALIZABLE_TIMEDELTA
        ),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self, *, data: DataObject, min_value: dt.timedelta, max_value: dt.timedelta
    ) -> None:
        _ = assume(min_value <= max_value)
        timedelta = data.draw(timedeltas_2w(min_value=min_value, max_value=max_value))
        ser = serialize_timedelta(timedelta)
        _ = parse_timedelta(ser)
        assert min_value <= timedelta <= max_value


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
    @given(data=data(), min_value=uint32s(), max_value=uint32s())
    def test_main(self, *, data: DataObject, min_value: int, max_value: int) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint32s(min_value=min_value, max_value=max_value))
        assert max(min_value, MIN_UINT32) <= x <= min(max_value, MAX_UINT32)


class TestUInt64s:
    @given(data=data(), min_value=uint64s(), max_value=uint64s())
    def test_main(self, *, data: DataObject, min_value: int, max_value: int) -> None:
        with assume_does_not_raise(InvalidArgument):
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


@SKIPIF_CI_AND_NOT_LINUX
class TestYieldTestRedis:
    @given(data=data(), value=int32s())
    @settings_with_reduced_examples()
    async def test_core(self, *, data: DataObject, value: int) -> None:
        async with yield_test_redis(data) as test:
            assert not await test.redis.exists(test.key)
            _ = await test.redis.set(test.key, value)
            result = int(cast(str, await test.redis.get(test.key)))
            assert result == value


class TestZonedDateTimes:
    @given(
        data=data(),
        min_value=datetimes(timezones=timezones() | just(dt.UTC) | none()),
        max_value=timezones() | datetimes(timezones=just(dt.UTC) | none()),
        time_zone1=timezones() | just(dt.UTC),
        time_zone2=timezones() | just(dt.UTC),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self,
        *,
        data: DataObject,
        min_value: dt.datetime,
        max_value: dt.datetime,
        time_zone1: ZoneInfo,
        time_zone2: ZoneInfo,
    ) -> None:
        _ = assume(
            (is_local_datetime(min_value) and is_local_datetime(max_value))
            or (is_zoned_datetime(min_value) and is_zoned_datetime(max_value))
        )
        _ = assume(min_value <= max_value)
        datetime = data.draw(
            zoned_datetimes(
                min_value=min_value, max_value=max_value, time_zone=time_zone1
            )
        )
        assert datetime.tzinfo is time_zone1
        if min_value.tzinfo is None:
            min_value_use = min_value.replace(tzinfo=time_zone1)
        else:
            min_value_use = min_value.astimezone(time_zone1)
        if max_value.tzinfo is None:
            max_value_use = max_value.replace(tzinfo=time_zone1)
        else:
            max_value_use = max_value.astimezone(time_zone1)
        assert min_value_use <= datetime <= max_value_use
        _ = datetime.astimezone(time_zone2)

    @given(
        time_zone=timezones()
        | sampled_from([_ZONED_DATETIMES_LEFT_MOST, _ZONED_DATETIMES_RIGHT_MOST])
        | just(dt.UTC)
    )
    def test_min(self, *, time_zone: ZoneInfo) -> None:
        datetime = dt.datetime.min.replace(tzinfo=_ZONED_DATETIMES_LEFT_MOST)
        _ = datetime.astimezone(time_zone)

    @given(
        time_zone=timezones()
        | sampled_from([_ZONED_DATETIMES_LEFT_MOST, _ZONED_DATETIMES_RIGHT_MOST])
        | just(dt.UTC)
    )
    def test_max(self, *, time_zone: ZoneInfo) -> None:
        datetime = dt.datetime.max.replace(tzinfo=_ZONED_DATETIMES_RIGHT_MOST)
        _ = datetime.astimezone(time_zone)

    @given(
        data=data(),
        min_value=zoned_datetimes(valid=True),
        max_value=zoned_datetimes(valid=True),
    )
    @SKIPIF_CI_AND_WINDOWS
    def test_valid(
        self, *, data: DataObject, min_value: dt.datetime, max_value: dt.datetime
    ) -> None:
        _ = assume(min_value <= max_value)
        datetime = data.draw(zoned_datetimes(valid=True))
        check_valid_zoned_datetime(datetime)
