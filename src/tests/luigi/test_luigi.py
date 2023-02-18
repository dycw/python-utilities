import datetime as dt
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Any, cast

from hypothesis import given, settings
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    sampled_from,
    times,
)
from luigi import BoolParameter, Task
from luigi.notifications import smtp

from utilities.hypothesis.luigi import namespace_mixins
from utilities.luigi import (
    EnumParameter,
    PathTarget,
    TimeParameter,
    _yield_task_classes,
    build,
    clone,
    get_dependencies_downstream,
    get_dependencies_upstream,
    get_task_classes,
)


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


class TestEnumParameter:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        class Example(Enum):
            member = auto()

        param = EnumParameter(Example)
        input_ = data.draw(sampled_from([Example.member, "member"]))
        norm = param.normalize(input_)
        assert param.parse(param.serialize(norm)) == norm


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
            self._get_sets,
            [a, b, c],
        )
        assert up_a == set()
        assert down_a == {b}
        assert up_b == {a}
        assert down_b == {c}
        assert up_c == {b}
        assert down_c == set()

        (
            (up_a_rec, down_a_rec),
            (up_b_rec, down_b_rec),
            (up_c_rec, down_c_rec),
        ) = map(partial(self._get_sets, recursive=True), [a, b, c])
        assert up_a_rec == set()
        assert down_a_rec == {b, c}
        assert up_b_rec == {a}
        assert down_b_rec == {c}
        assert up_c_rec == {a, b}
        assert down_c_rec == set()

    @staticmethod
    def _get_sets(
        task: Task,
        /,
        *,
        recursive: bool = False,
    ) -> tuple[set[Task], set[Task]]:
        return set(get_dependencies_upstream(task, recursive=recursive)), set(
            get_dependencies_downstream(task, recursive=recursive),
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
    @given(time=times())
    def test_main(self, time: dt.time) -> None:
        param = TimeParameter()
        norm = param.normalize(time)
        assert param.parse(param.serialize(norm)) == time
