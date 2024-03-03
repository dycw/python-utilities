from __future__ import annotations

from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest
from freezegun import freeze_time
from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    dates,
    integers,
    iterables,
    sampled_from,
    sets,
    times,
    tuples,
)
from luigi import BoolParameter, Parameter, Task
from sqlalchemy import Column, Engine, Integer, MetaData, Table, select
from sqlalchemy.orm import declarative_base
from typing_extensions import override

from utilities.datetime import serialize_date, serialize_datetime, serialize_time
from utilities.hypothesis import (
    datetimes_utc,
    namespace_mixins,
    sqlite_engines,
    temp_paths,
    text_ascii,
    versions,
)
from utilities.luigi import (
    AwaitTask,
    AwaitTime,
    DatabaseTarget,
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EngineParameter,
    EnumParameter,
    ExternalFile,
    ExternalTask,
    FrozenSetIntsParameter,
    FrozenSetStrsParameter,
    PathTarget,
    TableParameter,
    TimeParameter,
    VersionParameter,
    WeekdayParameter,
    build,
    clone,
    yield_dependencies,
)
from utilities.pathlib import ensure_path
from utilities.sqlalchemy import insert_items

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Iterable

    from semver import VersionInfo

    from utilities.types import IterableStrs


class TestAwaitTask:
    @given(namespace_mixin=namespace_mixins(), is_complete=booleans())
    def test_main(self, *, namespace_mixin: Any, is_complete: bool) -> None:
        class Example(namespace_mixin, Task):
            is_complete = cast(bool, BoolParameter())

            @override
            def complete(self) -> bool:
                return self.is_complete

        example = Example(is_complete=is_complete)
        task: AwaitTask[Any] = cast(Any, AwaitTask)(example)
        result = task.complete()
        assert result is is_complete


class TestAwaitTime:
    @given(time_start=datetimes_utc(), time_now=datetimes_utc())
    def test_main(self, *, time_start: dt.datetime, time_now: dt.datetime) -> None:
        _ = assume(time_start.microsecond == 0)
        task: AwaitTime = cast(Any, AwaitTime)(time_start)
        with freeze_time(time_now):
            result = task.exists()
        expected = time_now >= time_start
        assert result is expected


class TestBuild:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, *, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task): ...

        _ = build([Example()], local_scheduler=True)


class TestClone:
    @given(namespace_mixin=namespace_mixins(), truth=booleans())
    def test_main(self, *, namespace_mixin: Any, truth: bool) -> None:
        class A(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        class B(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        a = A(truth)
        result = clone(a, B)
        expected = B(truth)
        assert result is expected

    @given(namespace_mixin=namespace_mixins(), truth=booleans())
    def test_await(self, *, namespace_mixin: Any, truth: bool) -> None:
        class A(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        class B(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        a = A(truth)
        result = clone(a, B, await_=True)
        expected = AwaitTask(B(truth))
        assert result is expected


class TestDatabaseTarget:
    @given(engine=sqlite_engines(), rows=sets(tuples(integers(0, 10), integers(0, 10))))
    def test_main(self, *, engine: Engine, rows: set[tuple[int, int]]) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        sel = select(table).where(table.c["id1"] == 0)
        target = DatabaseTarget(sel, engine)
        assert not target.exists()
        insert_items(engine, (rows, table))
        expected = any(first == 0 for first, _ in rows)
        assert target.exists() is expected


class TestDateParameter:
    @given(data=data(), date=dates())
    def test_main(self, *, data: DataObject, date: dt.date) -> None:
        param = DateParameter()
        input_ = data.draw(sampled_from([date, serialize_date(date)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestDateTimeParameter:
    @given(data=data(), datetime=datetimes_utc())
    @pytest.mark.parametrize(
        "param_cls",
        [
            pytest.param(DateHourParameter),
            pytest.param(DateMinuteParameter),
            pytest.param(DateSecondParameter),
        ],
    )
    def test_main(
        self, data: DataObject, datetime: dt.datetime, param_cls: type[Parameter]
    ) -> None:
        param = param_cls()
        input_ = data.draw(sampled_from([datetime, serialize_datetime(datetime)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestEngineParameter:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        param = EngineParameter()
        norm = param.normalize(engine)
        new_engine = param.parse(param.serialize(norm))
        assert new_engine.url == norm.url


class TestEnumParameter:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        class Example(Enum):
            member = auto()

        param = EnumParameter(Example)
        input_ = data.draw(sampled_from([Example.member, "member"]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestExternalFile:
    @given(namespace_mixin=namespace_mixins(), root=temp_paths())
    def test_main(self, *, namespace_mixin: Any, root: Path) -> None:
        path = ensure_path(root, "file")

        class Example(namespace_mixin, ExternalFile): ...

        task = Example(path)
        assert not task.exists()
        path.touch()
        assert task.exists()


class TestExternalTask:
    @given(namespace_mixin=namespace_mixins(), is_complete=booleans())
    def test_main(self, *, namespace_mixin: Any, is_complete: bool) -> None:
        class Example(namespace_mixin, ExternalTask):
            is_complete = cast(bool, BoolParameter())

            @override
            def exists(self) -> bool:
                return self.is_complete

        task = Example(is_complete=is_complete)
        result = task.exists()
        assert result is is_complete


class TestFrozenSetIntsParameter:
    @given(values=iterables(integers()))
    def test_main(self, *, values: Iterable[int]) -> None:
        param = FrozenSetIntsParameter()
        norm = param.normalize(values)
        assert param.parse(param.serialize(norm)) == norm


class TestFrozenSetStrsParameter:
    @given(text=iterables(text_ascii()))
    def test_main(self, *, text: IterableStrs) -> None:
        param = FrozenSetStrsParameter()
        norm = param.normalize(text)
        assert param.parse(param.serialize(norm)) == norm


class TestGetDependencies:
    @given(namespace_mixin=namespace_mixins())
    def test_recursive(self, *, namespace_mixin: Any) -> None:
        class A(namespace_mixin, Task): ...

        class B(namespace_mixin, Task):
            @override
            def requires(self) -> A:
                return clone(self, A)

        class C(namespace_mixin, Task):
            @override
            def requires(self) -> B:
                return clone(self, B)

        a, b, c = A(), B(), C()
        assert set(yield_dependencies(a, recursive=True)) == set()
        assert set(yield_dependencies(b, recursive=True)) == {a}
        assert set(yield_dependencies(c, recursive=True)) == {a, b}

    @given(namespace_mixin=namespace_mixins())
    def test_non_recursive(self, *, namespace_mixin: Any) -> None:
        class A(namespace_mixin, Task): ...

        class B(namespace_mixin, Task):
            @override
            def requires(self) -> A:
                return clone(self, A)

        class C(namespace_mixin, Task):
            @override
            def requires(self) -> B:
                return clone(self, B)

        a, b, c = A(), B(), C()
        assert set(yield_dependencies(a)) == set()
        assert set(yield_dependencies(b)) == {a}
        assert set(yield_dependencies(c)) == {b}


class TestPathTarget:
    def test_main(self, *, tmp_path: Path) -> None:
        target = PathTarget(path := ensure_path(tmp_path, "file"))
        assert isinstance(target.path, Path)
        assert not target.exists()
        path.touch()
        assert target.exists()


class TestTableParameter:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, namespace_mixin: Any) -> None:
        class ExampleTask(namespace_mixin, Task):
            table = TableParameter()

        class ExampleTable(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        _ = ExampleTask(ExampleTable)


class TestTimeParameter:
    @given(data=data(), time=times())
    def test_main(self, *, data: DataObject, time: dt.time) -> None:
        param = TimeParameter()
        input_ = data.draw(sampled_from([time, serialize_time(time)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == time


class TestVersionParameter:
    @given(version=versions())
    def test_main(self, version: VersionInfo) -> None:
        param = VersionParameter()
        norm = param.normalize(version)
        assert param.parse(param.serialize(norm)) == norm


class TestWeekdayParameter:
    @given(data=data(), rounding=sampled_from(["prev", "next"]), date=dates())
    def test_main(
        self, *, data: DataObject, rounding: Literal["prev", "next"], date: dt.date
    ) -> None:
        param = WeekdayParameter(rounding=rounding)
        input_ = data.draw(sampled_from([date, serialize_date(date)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm
