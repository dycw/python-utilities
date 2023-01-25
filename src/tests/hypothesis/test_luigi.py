from hypothesis import given
from hypothesis.strategies import DataObject, data
from luigi import Task

from utilities.hypothesis.luigi import task_namespaces


class TestTaskNamespaces:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        _ = data.draw(task_namespaces())

    @given(namespace=task_namespaces())
    def test_first(self, namespace: str) -> None:
        class Example(Task):
            task_namespace = namespace

        _ = Example()

    @given(namespace=task_namespaces())
    def test_second(self, namespace: str) -> None:
        class Example(Task):
            task_namespace = namespace
            ...

        _ = Example()
