from __future__ import annotations

from time import sleep
from typing import Literal, NoReturn, cast

from pytest import fixture, raises
from treelib import Node

from utilities.iterables import one
from utilities.tracer2 import _NodeData, get_tracer_trees, set_tracer_trees, tracer2


@fixture(autouse=True)
def set_tracer_tree_per_function() -> None:
    set_tracer_trees([])


@tracer2
def outer1() -> None:
    sleep(0.01)  # 0.01
    mid1()  # 0.01
    mid2()  # 0.02


@tracer2
def mid1() -> None:
    sleep(0.01)  # 0.01


@tracer2
def mid2() -> None:
    sleep(0.01)  # 0.01
    inner()  # 0.01


@tracer2
def inner() -> None:
    sleep(0.01)  # 0.01


@tracer2
def fails() -> NoReturn:
    sleep(0.01)  # 0.01
    msg = "Always fails"
    raise ValueError(msg)


class TestTracer:
    def test_main(self) -> None:
        _ = outer1()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer2", "outer1", 0.04, "success")
        mid1, mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(mid1, "tests.test_tracer2", "mid1", 0.01, "success")
        self._check_node(mid2, "tests.test_tracer2", "mid2", 0.02, "success")
        assert len(tree.children(mid1.identifier)) == 0
        (inner,) = cast(list[Node], tree.children(mid2.identifier))
        self._check_node(inner, "tests.test_tracer2", "inner", 0.01, "success")

    def test_multiple_calls(self) -> None:
        _ = inner()
        _ = inner()
        trees = get_tracer_trees()
        assert len(trees) == 2
        for tree in trees:
            root: Node = tree[tree.root]
            self._check_node(root, "tests.test_tracer2", "inner", 0.01, "success")

    def test_error(self) -> None:
        with raises(ValueError, match="Always fails"):
            _ = fails()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer2", "fails", 0.01, "failure")

    def _check_node(
        self,
        node: Node,
        module: str,
        name: str,
        duration: float,
        outcome: Literal["success", "failure"],
        /,
    ) -> None:
        assert node.tag == f"{module}:{name}"
        data = cast(_NodeData, node.data)
        assert data["module"] == module
        assert data["name"] == name
        data_duration = data.get("duration")
        assert data_duration is not None
        assert data_duration.total_seconds() <= 2 * duration
        assert data["outcome"] == outcome
