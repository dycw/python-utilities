from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING, cast

from pytest import approx
from treelib import Node

from utilities.tracer import _NodeData, get_tracer_tree, tracer

if TYPE_CHECKING:
    from pytest import CaptureFixture


def outer1() -> None:
    with tracer():
        sleep(0.01)  # 0.01
        mid1()  # 0.01
        mid2()  # 0.02


def mid1() -> None:
    with tracer():
        sleep(0.01)  # 0.01


def mid2() -> None:
    with tracer():
        sleep(0.01)  # 0.01
        inner()  # 0.01


def inner() -> None:
    with tracer():
        sleep(0.01)  # 0.01


class TestTracer:
    def test_main(self) -> None:
        _ = outer1()
        tree = get_tracer_tree()
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer", "outer1", 0.04, 0.3)
        mid1, mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(mid1, "tests.test_tracer", "mid1", 0.01, 0.3)
        self._check_node(mid2, "tests.test_tracer", "mid2", 0.02, 0.3)
        assert len(tree.children(mid1.identifier)) == 0
        (inner,) = cast(list[Node], tree.children(mid2.identifier))
        self._check_node(inner, "tests.test_tracer", "inner", 0.01, 0.2)

    def _check_node(
        self, node: Node, module: str, name: str, duration: float, rel: float, /
    ) -> None:
        assert node.tag == f"{module}:{name}"
        data = cast(_NodeData, node.data)
        assert data["module"] == module
        assert data["name"] == name
        end_time = data.get("end_time")
        assert end_time is not None
        assert end_time - data["start_time"] == approx(duration, rel=rel)
