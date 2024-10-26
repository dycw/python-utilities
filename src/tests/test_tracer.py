from __future__ import annotations

import asyncio
import time
from typing import Literal, cast

from pytest import fixture, raises
from treelib import Node

from utilities.iterables import one
from utilities.tracer import _NodeData, get_tracer_trees, set_tracer_trees, tracer
from utilities.zoneinfo import HongKong


@fixture(autouse=True)
def set_tracer_tree_per_function() -> None:
    set_tracer_trees([])


@tracer
def outer1() -> None:
    time.sleep(0.01)  # 0.01
    mid1()  # 0.01
    mid2()  # 0.02


@tracer
def mid1() -> None:
    time.sleep(0.01)  # 0.01


@tracer
def mid2() -> None:
    time.sleep(0.01)  # 0.01
    inner()  # 0.01


@tracer
def inner() -> None:
    time.sleep(0.01)  # 0.01


@tracer
async def async_outer1() -> None:
    await asyncio.sleep(0.01)  # 0.01
    await async_mid1()  # 0.01
    await async_mid2()  # 0.02


@tracer
async def async_mid1() -> None:
    await asyncio.sleep(0.01)  # 0.01


@tracer
async def async_mid2() -> None:
    await asyncio.sleep(0.01)  # 0.01
    await async_inner()  # 0.01


@tracer
async def async_inner() -> None:
    await asyncio.sleep(0.01)  # 0.01


class TestTracer:
    def test_main(self) -> None:
        _ = outer1()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer", "outer1", 0.04, "success")
        mid1, mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(mid1, "tests.test_tracer", "mid1", 0.01, "success")
        self._check_node(mid2, "tests.test_tracer", "mid2", 0.02, "success")
        assert len(tree.children(mid1.identifier)) == 0
        (inner,) = cast(list[Node], tree.children(mid2.identifier))
        self._check_node(inner, "tests.test_tracer", "inner", 0.01, "success")

    async def test_async(self) -> None:
        _ = await async_outer1()
        trees = get_tracer_trees()
        tree = one(trees)
        root: Node = tree[tree.root]
        self._check_node(root, "tests.test_tracer", "async_outer1", 0.04, "success")
        mid1, mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(mid1, "tests.test_tracer", "async_mid1", 0.01, "success")
        self._check_node(mid2, "tests.test_tracer", "async_mid2", 0.02, "success")
        assert len(tree.children(mid1.identifier)) == 0
        (inner,) = cast(list[Node], tree.children(mid2.identifier))
        self._check_node(inner, "tests.test_tracer", "async_inner", 0.01, "success")

    def test_multiple_calls(self) -> None:
        @tracer
        def func() -> None:
            time.sleep(0.01)

        _ = func()
        _ = func()
        trees = get_tracer_trees()
        assert len(trees) == 2
        for tree in trees:
            root: Node = tree[tree.root]
            self._check_node(
                root,
                "tests.test_tracer",
                "TestTracer.test_multiple_calls.<locals>.func",
                0.02,
                "success",
            )

    def test_time_zone(self) -> None:
        @tracer(time_zone=HongKong)
        def func() -> None:
            return

        _ = func()
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data["start_time"].tzinfo is HongKong
        assert data["end_time"].tzinfo is HongKong

    def test_error(self) -> None:
        @tracer
        def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = func()
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data["outcome"] == "failure"
        assert data.get("error") is ValueError

    def _check_node(
        self,
        node: Node,
        module: str,
        qualname: str,
        duration: float,
        outcome: Literal["success", "failure"],
        /,
    ) -> None:
        assert node.tag == f"{module}:{qualname}"
        data = cast(_NodeData, node.data)
        assert data["module"] == module
        assert data["qualname"] == qualname
        assert data["duration"].total_seconds() <= 2 * duration
        assert data["outcome"] == outcome
