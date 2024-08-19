from __future__ import annotations

import datetime as dt
from itertools import pairwise
from pathlib import Path
from re import search
from subprocess import PIPE, check_output
from typing import TYPE_CHECKING, Any, cast

import redis.asyncio
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
    sets,
    timedeltas,
)
from numpy import (
    iinfo,
    inf,
    int32,
    int64,
    isfinite,
    isinf,
    isnan,
    ravel,
    rint,
    uint32,
    uint64,
    zeros,
)
from pytest import mark, param, raises
from semver import Version
from sqlalchemy import Column, Engine, Integer, MetaData, Select, Table, select
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass

from tests.conftest import FLAKY, SKIPIF_CI_AND_NOT_LINUX
from utilities.git import _GET_BRANCH_NAME
from utilities.hypothesis import (
    Shape,
    aiosqlite_engines,
    assume_does_not_raise,
    bool_arrays,
    concatenated_arrays,
    datetimes_utc,
    durations,
    float_arrays,
    floats_extra,
    git_repos,
    hashables,
    int32s,
    int64s,
    int_arrays,
    lists_fixed_length,
    longs,
    months,
    redis_clients,
    redis_time_series,
    settings_with_reduced_examples,
    setup_hypothesis_profiles,
    slices,
    sqlite_engines,
    str_arrays,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_clean,
    text_printable,
    timedeltas_2w,
    uint32s,
    uint64s,
    versions,
)
from utilities.math import MAX_LONG, MIN_LONG
from utilities.os import temp_environ
from utilities.pathvalidate import valid_path
from utilities.platform import maybe_yield_lower_case
from utilities.sqlalchemy import get_table, insert_items, insert_items_async
from utilities.types import Duration, Number, make_isinstance
from utilities.whenever import (
    MAX_TWO_WAY_TIMEDELTA,
    MIN_TWO_WAY_TIMEDELTA,
    parse_duration,
    parse_timedelta,
    serialize_duration,
    serialize_timedelta,
)
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Sequence
    from collections.abc import Set as AbstractSet
    from uuid import UUID

    import redis
    from redis.commands.timeseries import TimeSeries

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


class TestConcatenatedArrays:
    @given(data=data(), m=integers(0, 10), n=integers(0, 10))
    def test_1d(self, *, data: DataObject, m: int, n: int) -> None:
        arrays = just(zeros(n, dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, n))
        assert array.shape == (m, n)

    @given(data=data(), m=integers(0, 10), n=integers(0, 10), p=integers(0, 10))
    def test_2d(self, *, data: DataObject, m: int, n: int, p: int) -> None:
        arrays = just(zeros((n, p), dtype=float))
        array = data.draw(concatenated_arrays(arrays, m, (n, p)))
        assert array.shape == (m, n, p)


class TestDatetimesUTC:
    @given(data=data(), min_value=datetimes(), max_value=datetimes())
    def test_main(
        self, *, data: DataObject, min_value: dt.datetime, max_value: dt.datetime
    ) -> None:
        min_value, max_value = (v.replace(tzinfo=UTC) for v in [min_value, max_value])
        _ = assume(min_value <= max_value)
        datetime = data.draw(datetimes_utc(min_value=min_value, max_value=max_value))
        assert min_value <= datetime <= max_value


class TestDurations:
    @given(
        data=data(),
        min_number=integers() | floats() | none(),
        max_number=integers() | floats() | none(),
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
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(
                durations(
                    min_number=min_number,
                    max_number=max_number,
                    min_timedelta=min_timedelta,
                    max_timedelta=max_timedelta,
                )
            )
        assert isinstance(x, Duration)
        if isinstance(x, int):
            if isinstance(min_number, int):
                assert x >= min_number
            if isinstance(max_number, int):
                assert x <= max_number
        elif isinstance(x, float):
            if min_number is not None:
                assert x >= min_number
            if max_number is not None:
                assert x <= max_number
        else:
            if min_timedelta is not None:
                assert x >= min_timedelta
            if max_timedelta is not None:
                assert x <= max_timedelta

    @given(
        data=data(),
        min_value=durations(two_way=True).filter(make_isinstance(dt.timedelta)),
        max_value=durations(two_way=True).filter(make_isinstance(dt.timedelta)),
    )
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_two_way(
        self, *, data: DataObject, min_value: dt.timedelta, max_value: dt.timedelta
    ) -> None:
        _ = assume(min_value <= max_value)
        duration = data.draw(durations(two_way=True))
        ser = serialize_duration(duration)
        _ = parse_duration(ser)


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
    @settings_with_reduced_examples(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(self, *, data: DataObject) -> None:
        branch = data.draw(text_ascii(min_size=1) | none())
        path = data.draw(git_repos(branch=branch))
        assert set(path.iterdir()) == {Path(path, ".git")}
        if branch is not None:
            output = check_output(_GET_BRANCH_NAME, stderr=PIPE, cwd=path, text=True)
            assert output.strip("\n") == branch


class TestHashables:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        x = data.draw(hashables())
        _ = hash(x)


class TestIntArrays:
    @given(
        data=data(),
        shape=array_shapes(),
        min_value=int64s() | none(),
        max_value=int64s() | none(),
        unique=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        shape: Shape,
        min_value: int | None,
        max_value: int | None,
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
    @given(data=data(), min_value=int32s() | none(), max_value=int32s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int32s(min_value=min_value, max_value=max_value))
        info = iinfo(int32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestInt64s:
    @given(data=data(), min_value=int64s() | none(), max_value=int64s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(int64s(min_value=min_value, max_value=max_value))
        info = iinfo(int64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestLiftDraw:
    @given(data=data(), x=booleans())
    def test_fixed(self, *, data: DataObject, x: bool) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            _ = _draw(booleans())
            return x

        result = data.draw(func())
        assert result is x

    @given(data=data())
    def test_strategy(self, *, data: DataObject) -> None:
        @composite
        def func(_draw: DrawFn, /) -> bool:
            return _draw(booleans())

        result = data.draw(func())
        assert isinstance(result, bool)


class TestListsFixedLength:
    @given(data=data(), size=integers(1, 10))
    @mark.parametrize(
        "unique", [param(True, id="unique"), param(False, id="no unique")]
    )
    @mark.parametrize(
        "sorted_", [param(True, id="sorted"), param(False, id="no sorted")]
    )
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


class TestLongs:
    @given(data=data(), min_value=longs() | none(), max_value=longs() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(longs(min_value=min_value, max_value=max_value))
        assert MIN_LONG <= x <= MAX_LONG
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


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


@SKIPIF_CI_AND_NOT_LINUX
class TestRedisClients:
    @given(client_pair=redis_clients(), key=text_ascii(), value=integers())
    def test_main(
        self, *, client_pair: tuple[redis.Redis, UUID], key: str, value: int
    ) -> None:
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        _ = client.set(full_key, value)
        result = int(cast(str, client.get(full_key)))
        assert result == value


@SKIPIF_CI_AND_NOT_LINUX
class TestRedisTimeSeries:
    @given(ts_pair=redis_time_series(), key=text_ascii(), value=longs())
    def test_main(
        self, *, ts_pair: tuple[TimeSeries, UUID], key: str, value: int
    ) -> None:
        ts, uuid = ts_pair
        full_key = f"{uuid}_{key}"
        _ = ts.add(full_key, "*", value)
        res_timestamp, res_value = ts.get(full_key)
        assert isinstance(res_timestamp, int)
        assert res_value == value


class TestReducedExamples:
    @given(frac=floats(0.0, 10.0))
    def test_main(self, *, frac: float) -> None:
        @settings_with_reduced_examples(frac)
        def test() -> None:
            pass

        result = cast(Any, test)._hypothesis_internal_use_settings.max_examples  # noqa: SLF001
        expected = max(round(frac * settings().max_examples), 1)
        assert result == expected


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


class TestSetupHypothesisProfiles:
    def test_main(self) -> None:
        setup_hypothesis_profiles()
        curr = settings()
        assert Phase.shrink in curr.phases
        assert curr.max_examples in {10, 100, 1000}

    def test_no_shrink(self) -> None:
        with temp_environ({"HYPOTHESIS_NO_SHRINK": "1"}):
            setup_hypothesis_profiles()
        assert Phase.shrink not in settings().phases

    @given(max_examples=integers(1, 100))
    def test_max_examples(self, *, max_examples: int) -> None:
        with temp_environ({"HYPOTHESIS_MAX_EXAMPLES": str(max_examples)}):
            setup_hypothesis_profiles()
        assert settings().max_examples == max_examples


class TestSQLiteEngines:
    @given(engine=sqlite_engines())
    def test_sync(self, *, engine: Engine) -> None:
        assert isinstance(engine, Engine)
        database = engine.url.database
        assert database is not None
        assert not valid_path(database).exists()

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_sync_table(self, *, data: DataObject, ids: set[int]) -> None:
        metadata, table = self._metadata_and_table()
        engine = data.draw(sqlite_engines(metadata=metadata))
        self._run_test_sync(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 10)))
    def test_sync_mapped_class(self, *, data: DataObject, ids: set[int]) -> None:
        base, mapped_class = self._base_and_mapped_class()
        engine = data.draw(sqlite_engines(base=base))
        self._run_test_sync(engine, mapped_class, ids)

    @given(data=data())
    async def test_async(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        assert isinstance(engine, AsyncEngine)
        database = engine.url.database
        assert database is not None
        assert not valid_path(database).exists()

    @given(data=data(), ids=sets(integers(0, 10)))
    async def test_async_table(self, *, data: DataObject, ids: set[int]) -> None:
        metadata, table = self._metadata_and_table()
        engine = await aiosqlite_engines(data, metadata=metadata)
        await self._run_test_async(engine, table, ids)

    @given(data=data(), ids=sets(integers(0, 10)))
    async def test_async_mapped_class(self, *, data: DataObject, ids: set[int]) -> None:
        base, mapped_class = self._base_and_mapped_class()
        engine = await aiosqlite_engines(data, base=base)
        await self._run_test_async(engine, mapped_class, ids)

    def _metadata_and_table(self) -> tuple[MetaData, Table]:
        metadata = MetaData()
        table = Table("example", metadata, Column("id_", Integer, primary_key=True))
        return metadata, table

    def _base_and_mapped_class(self) -> tuple[type[Any], type[Any]]:
        class Base(DeclarativeBase, MappedAsDataclass): ...  # pyright: ignore[reportUnsafeMultipleInheritance]

        class Example(Base):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        return Base, Example

    def _run_test_sync(
        self, engine: Engine, table_or_mapped_class: Table | type[Any], ids: set[int], /
    ) -> None:
        insert_items(engine, ([(id_,) for id_ in ids], table_or_mapped_class))
        sel = self._get_select(table_or_mapped_class)
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        self._assert_results(res, ids)

    async def _run_test_async(
        self,
        engine: AsyncEngine,
        table_or_mapped_class: Table | type[Any],
        ids: set[int],
        /,
    ) -> None:
        await insert_items_async(
            engine, ([(id_,) for id_ in ids], table_or_mapped_class)
        )
        sel = self._get_select(table_or_mapped_class)
        async with engine.begin() as conn:
            res = (await conn.execute(sel)).scalars().all()
        self._assert_results(res, ids)

    def _get_select(self, table_or_mapped_class: Table | type[Any], /) -> Select[Any]:
        return select(get_table(table_or_mapped_class).c["id_"])

    def _assert_results(self, results: Sequence[Any], ids: set[int], /) -> None:
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
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_ascii(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^[A-Za-z]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextClean:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_clean(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search("^\\S[^\\r\\n]*$|^$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTextPrintable:
    @given(
        data=data(),
        min_size=integers(0, 100),
        max_size=integers(0, 100) | none(),
        disallow_na=booleans(),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        min_size: int,
        max_size: int | None,
        disallow_na: bool,
    ) -> None:
        with assume_does_not_raise(InvalidArgument, AssertionError):
            text = data.draw(
                text_printable(
                    min_size=min_size, max_size=max_size, disallow_na=disallow_na
                )
            )
        assert search(r"^[0-9A-Za-z!\"#$%&'()*+,-./:;<=>?@\[\\\]^_`{|}~\s]*$", text)
        assert len(text) >= min_size
        if max_size is not None:
            assert len(text) <= max_size
        if disallow_na:
            assert text != "NA"


class TestTimeDeltas2W:
    @given(
        data=data(),
        min_value=timedeltas(
            min_value=MIN_TWO_WAY_TIMEDELTA, max_value=MAX_TWO_WAY_TIMEDELTA
        ),
        max_value=timedeltas(
            min_value=MIN_TWO_WAY_TIMEDELTA, max_value=MAX_TWO_WAY_TIMEDELTA
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


class TestUInt32s:
    @given(data=data(), min_value=uint32s() | none(), max_value=uint32s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint32s(min_value=min_value, max_value=max_value))
        info = iinfo(uint32)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestUInt64s:
    @given(data=data(), min_value=uint64s() | none(), max_value=uint64s() | none())
    def test_main(
        self, *, data: DataObject, min_value: int | None, max_value: int | None
    ) -> None:
        with assume_does_not_raise(InvalidArgument):
            x = data.draw(uint64s(min_value=min_value, max_value=max_value))
        info = iinfo(uint64)
        assert info.min <= x <= info.max
        if min_value is not None:
            assert x >= min_value
        if max_value is not None:
            assert x <= max_value


class TestVersions:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        version = data.draw(versions())
        assert isinstance(version, Version)

    @given(data=data())
    def test_min_version(self, data: DataObject) -> None:
        min_version = data.draw(versions())
        version = data.draw(versions(min_version=min_version))
        assert version >= min_version

    @given(data=data())
    def test_max_version(self, data: DataObject) -> None:
        max_version = data.draw(versions())
        version = data.draw(versions(max_version=max_version))
        assert version <= max_version

    @given(data=data())
    def test_min_and_max_version(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        min_version = min(version1, version2)
        max_version = max(version1, version2)
        version = data.draw(versions(min_version=min_version, max_version=max_version))
        assert min_version <= version <= max_version

    @given(data=data())
    def test_error(self, data: DataObject) -> None:
        version1, version2 = data.draw(lists_fixed_length(versions(), 2))
        _ = assume(version1 != version2)
        with raises(InvalidArgument):
            _ = data.draw(
                versions(
                    min_version=max(version1, version2),
                    max_version=min(version1, version2),
                )
            )
