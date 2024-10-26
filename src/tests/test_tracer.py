from __future__ import annotations

from time import sleep
from typing import Literal, NoReturn, cast

from pytest import approx, fixture, raises
from treelib import Node

from tests.conftest import FLAKY
from utilities.iterables import one
from utilities.tracer import _NodeData, get_tracer_trees, set_tracer_trees, tracer


@fixture(autouse=True)
def set_tracer_tree_per_function() -> None:
    set_tracer_trees([])


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


def fails() -> NoReturn:
    with tracer():
        sleep(0.01)  # 0.01
        msg = "Always fails"
        raise ValueError(msg)


class TestTracer:
    @FLAKY
    def test_main(self) -> None:
        _ = outer1()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer", "outer1", 0.04, 0.5, "success")
        mid1, mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(mid1, "tests.test_tracer", "mid1", 0.01, 0.5, "success")
        self._check_node(mid2, "tests.test_tracer", "mid2", 0.02, 0.5, "success")
        assert len(tree.children(mid1.identifier)) == 0
        (inner,) = cast(list[Node], tree.children(mid2.identifier))
        self._check_node(inner, "tests.test_tracer", "inner", 0.01, 0.5, "success")

    @FLAKY
    def test_multiple_calls(self) -> None:
        _ = inner()
        _ = inner()
        trees = get_tracer_trees()
        assert len(trees) == 2
        for tree in trees:
            root: Node = tree[tree.root]
            self._check_node(root, "tests.test_tracer", "inner", 0.01, 0.5, "success")

    @FLAKY
    def test_error(self) -> None:
        with raises(ValueError, match="Always fails"):
            _ = fails()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer", "fails", 0.01, 0.5, "failure")

    def _check_node(
        self,
        node: Node,
        module: str,
        name: str,
        duration: float,
        rel: float,
        outcome: Literal["success", "failure"],
        /,
    ) -> None:
        assert node.tag == f"{module}:{name}"
        data = cast(_NodeData, node.data)
        assert data["module"] == module
        assert data["name"] == name
        end_time = data.get("end_time")
        assert end_time is not None
        assert end_time - data["start_time"] == approx(duration, rel=rel)
        assert data["outcome"] == outcome
