import datetime as dt
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Any, Literal, cast

from freezegun import freeze_time
from hypothesis import assume, given, settings
from hypothesis.strategies import DataObject, booleans, data, dates, sampled_from, times
from luigi import BoolParameter, Task
from luigi.notifications import smtp

from utilities.datetime import serialize_date, serialize_time
from utilities.hypothesis import datetimes_utc, temp_paths
from utilities.hypothesis.luigi import namespace_mixins
from utilities.luigi import (
    AwaitTask,
    AwaitTime,
    DateParameter,
    EnumParameter,
    ExternalFile,
    ExternalTask,
    PathTarget,
    TimeParameter,
    WeekdayParameter,
    _yield_task_classes,
    build,
    clone,
    get_dependencies_downstream,
    get_dependencies_upstream,
    get_task_classes,
)


class TestAwaitTask:
    @given(namespace_mixin=namespace_mixins(), is_complete=booleans())
    def test_main(self, namespace_mixin: Any, is_complete: bool) -> None:
        class Example(namespace_mixin, Task):
            is_complete = cast(bool, BoolParameter())

            def complete(self) -> bool:
                return self.is_complete

        example = Example(is_complete=is_complete)
        task: AwaitTask = cast(Any, AwaitTask)(example)
        result = task.complete()
        assert result is is_complete


class TestAwaitTime:
    @given(time_start=datetimes_utc(), time_now=datetimes_utc())
    def test_main(self, time_start: dt.datetime, time_now: dt.datetime) -> None:
        _ = assume(time_start.microsecond == 0)
        task: AwaitTime = cast(Any, AwaitTime)(time_start)
        with freeze_time(time_now):
            result = task.exists()
        expected = time_now >= time_start
        assert result is expected


class TestBuild:
    @given(namespace_mixin=namespace_mixins())
    def test_main(self, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task):
            ...

        _ = build([Example()], local_scheduler=True)


class TestClone:
    @given(namespace_mixin=namespace_mixins(), truth=booleans())
    def test_main(self, namespace_mixin: Any, truth: bool) -> None:
        class A(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        class B(namespace_mixin, Task):
            truth = cast(bool, BoolParameter())

        a = A(truth)
        result = clone(a, B)
        expected = B(truth)
        assert result is expected


class TestDateParameter:
    @given(data=data(), date=dates())
    def test_main(self, data: DataObject, date: dt.date) -> None:
        param = DateParameter()
        input_ = data.draw(sampled_from([date, serialize_date(date)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestEnumParameter:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        class Example(Enum):
            member = auto()

        param = EnumParameter(Example)
        input_ = data.draw(sampled_from([Example.member, "member"]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


class TestExternalFile:
    @given(namespace_mixin=namespace_mixins(), root=temp_paths())
    def test_main(self, namespace_mixin: Any, root: Path) -> None:
        path = root.joinpath("file")

        class Example(namespace_mixin, ExternalFile):
            ...

        task = Example(path)
        assert not task.exists()
        path.touch()
        assert task.exists()


class TestExternalTask:
    @given(namespace_mixin=namespace_mixins(), is_complete=booleans())
    def test_main(self, namespace_mixin: Any, is_complete: bool) -> None:
        class Example(namespace_mixin, ExternalTask):
            is_complete = cast(bool, BoolParameter())

            def exists(self) -> bool:
                return self.is_complete

        task = Example(is_complete=is_complete)
        result = task.exists()
        assert result is is_complete


class TestGetDependencies:
    @given(namespace_mixin=namespace_mixins())
    @settings(max_examples=1)
    def test_main(self, namespace_mixin: Any) -> None:
        class A(namespace_mixin, Task):
            ...

        class B(namespace_mixin, Task):
            def requires(self) -> A:
                return clone(self, A)

        class C(namespace_mixin, Task):
            def requires(self) -> B:
                return clone(self, B)

        a, b, c = A(), B(), C()
        ((up_a, down_a), (up_b, down_b), (up_c, down_c)) = map(
            self._get_sets, [a, b, c]
        )
        assert up_a == set()
        assert down_a == {b}
        assert up_b == {a}
        assert down_b == {c}
        assert up_c == {b}
        assert down_c == set()

        ((up_a_rec, down_a_rec), (up_b_rec, down_b_rec), (up_c_rec, down_c_rec)) = map(
            partial(self._get_sets, recursive=True), [a, b, c]
        )
        assert up_a_rec == set()
        assert down_a_rec == {b, c}
        assert up_b_rec == {a}
        assert down_b_rec == {c}
        assert up_c_rec == {a, b}
        assert down_c_rec == set()

    @staticmethod
    def _get_sets(
        task: Task, /, *, recursive: bool = False
    ) -> tuple[set[Task], set[Task]]:
        return set(get_dependencies_upstream(task, recursive=recursive)), set(
            get_dependencies_downstream(task, recursive=recursive)
        )


class TestGetTaskClasses:
    @given(namespace_mixin=namespace_mixins())
    @settings(max_examples=1)
    def test_main(self, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task):
            ...

        assert Example in get_task_classes()

    def test_notifications(self) -> None:
        assert smtp not in _yield_task_classes()

    @given(namespace_mixin=namespace_mixins())
    @settings(max_examples=1)
    def test_filter(self, namespace_mixin: Any) -> None:
        class Parent(namespace_mixin, Task):
            ...

        class Child(Parent):
            ...

        result = get_task_classes(cls=Parent)
        expected = frozenset([Child])
        assert result == expected


class TestPathTarget:
    def test_main(self, tmp_path: Path) -> None:
        target = PathTarget(path := tmp_path.joinpath("file"))
        assert isinstance(target.path, Path)
        assert not target.exists()
        path.touch()
        assert target.exists()


class TestTimeParameter:
    @given(data=data(), time=times())
    def test_main(self, data: DataObject, time: dt.time) -> None:
        param = TimeParameter()
        input_ = data.draw(sampled_from([time, serialize_time(time)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == time


class TestWeekdayParameter:
    @given(data=data(), rounding=sampled_from(["prev", "next"]), date=dates())
    def test_main(
        self, data: DataObject, rounding: Literal["prev", "next"], date: dt.date
    ) -> None:
        param = WeekdayParameter(rounding=rounding)
        input_ = data.draw(sampled_from([date, serialize_date(date)]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm
