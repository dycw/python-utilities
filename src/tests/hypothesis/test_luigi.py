from typing import Any

from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import data
from luigi import Task

from utilities.hypothesis.luigi import namespace_mixins


class TestNamespaceMixins:
    @given(data=data())
    def test_main(self, data: DataObject) -> None:
        _ = data.draw(namespace_mixins())

    @given(namespace_mixin=namespace_mixins())
    def test_first(self, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task):
            ...

        _ = Example()

    @given(namespace_mixin=namespace_mixins())
    def test_second(self, namespace_mixin: Any) -> None:
        class Example(namespace_mixin, Task):
            ...

        _ = Example()
