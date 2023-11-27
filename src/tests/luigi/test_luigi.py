from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from enum import Enum, auto
from pathlib import Path
from typing import Any, Literal, cast

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
    times,
)
from luigi import BoolParameter, Parameter, Task
from pytest import mark, param
from semver import VersionInfo
from typing_extensions import override

from utilities.datetime import serialize_date, serialize_datetime, serialize_time
from utilities.hypothesis import (
    datetimes_utc,
    namespace_mixins,
    temp_paths,
    text_ascii,
    versions,
)
from utilities.luigi import (
    AwaitTask,
    AwaitTime,
    DateHourParameter,
    DateMinuteParameter,
    DateParameter,
    DateSecondParameter,
    EnumParameter,
    ExternalFile,
    ExternalTask,
    FrozenSetIntsParameter,
    FrozenSetStrsParameter,
    PathTarget,
    TimeParameter,
    VersionParameter,
    WeekdayParameter,
    build,
    clone,
    yield_dependencies,
)
from utilities.pathvalidate import valid_path
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
        class Example(namespace_mixin, Task):
            ...

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


class TestDateParameter:
    @given(data=data(), date=dates())
    def test_main(self, *, data: DataObject, date: dt.date) -> None:
        param = DateParameter()
        input_ = data.draw(sampled_from([date, serialize_date(date)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestDateTimeParameter:
    @given(data=data(), datetime=datetimes_utc())
    @mark.parametrize(
        "param_cls",
        [
            param(DateHourParameter),
            param(DateMinuteParameter),
            param(DateSecondParameter),
        ],
    )
    def test_main(
        self, data: DataObject, datetime: dt.datetime, param_cls: type[Parameter]
    ) -> None:
        param = param_cls()
        input_ = data.draw(sampled_from([datetime, serialize_datetime(datetime)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


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
        path = valid_path(root, "file")

        class Example(namespace_mixin, ExternalFile):
            ...

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
        class A(namespace_mixin, Task):
            ...

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
        class A(namespace_mixin, Task):
            ...

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
        target = PathTarget(path := valid_path(tmp_path, "file"))
        assert isinstance(target.path, Path)
        assert not target.exists()
        path.touch()
        assert target.exists()


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
