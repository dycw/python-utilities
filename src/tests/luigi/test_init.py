from functools import partial
from pathlib import Path
from typing import cast

from hypothesis import given
from hypothesis.strategies import booleans
from luigi import BoolParameter
from luigi import Task
from luigi.notifications import smtp

from utilities.hypothesis.luigi import task_namespaces
from utilities.luigi import PathTarget
from utilities.luigi import build
from utilities.luigi import clone
from utilities.luigi import yield_dependencies_downstream
from utilities.luigi import yield_dependencies_upstream
from utilities.luigi import yield_task_classes


class TestBuild:
    @given(namespace=task_namespaces())
    def test_main(self, namespace: str) -> None:
        class Example(Task):
            task_namespace = namespace

        _ = build([Example()], local_scheduler=True)


class TestClone:
    @given(namespace=task_namespaces(), truth=booleans())
    def test_main(self, namespace: str, truth: bool) -> None:
        class A(Task):
            task_namespace = namespace

            truth = cast(bool, BoolParameter())

        class B(Task):
            task_namespace = namespace

            truth = cast(bool, BoolParameter())

        a = A(truth)
        result = clone(a, B)
        expected = B(truth)
        assert result is expected


class TestPathTarget:
    def test_main(self, tmp_path: Path) -> None:
        target = PathTarget(path := tmp_path.joinpath("file"))
        assert isinstance(target.path, Path)
        assert not target.exists()
        path.touch()
        assert target.exists()


class TestYieldDependencies:
    @given(namespace=task_namespaces())
    def test_main(self, namespace: str) -> None:
        class A(Task):
            task_namespace = namespace

        class B(Task):
            task_namespace = namespace

            def requires(self) -> A:
                return clone(self, A)

        class C(Task):
            task_namespace = namespace

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
        task: Task, /, *, recursive: bool = False
    ) -> tuple[set[Task], set[Task]]:
        return set(yield_dependencies_upstream(task, recursive=recursive)), set(
            yield_dependencies_downstream(task, recursive=recursive)
        )


class TestYieldTaskClasses:
    @given(namespace=task_namespaces())
    def test_main(self, namespace: str) -> None:
        class Example(Task):
            task_namespace = namespace

        assert Example in yield_task_classes()

    def test_notifications(self) -> None:
        assert smtp not in yield_task_classes()

    @given(namespace=task_namespaces())
    def test_filter(self, namespace: str) -> None:
        class Parent(Task):
            task_namespace = namespace

        class Child(Parent):
            ...

        result = set(yield_task_classes(cls=Parent))
        expected = {Child}
        assert result == expected
