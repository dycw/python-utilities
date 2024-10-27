from __future__ import annotations

import asyncio
import time
from re import search
from typing import TYPE_CHECKING, Any, Literal, cast

from pytest import approx, fixture, raises
from treelib import Node

from tests.conftest import FLAKY
from utilities.functions import get_class_name
from utilities.iterables import one
from utilities.tracer import _NodeData, get_tracer_trees, set_tracer_trees, tracer
from utilities.zoneinfo import HongKong

if TYPE_CHECKING:
    from collections.abc import Callable


@fixture(autouse=True)
def set_tracer_tree_per_function() -> None:
    set_tracer_trees([])


class TestTracer:
    @FLAKY
    def test_sync(self) -> None:
        @tracer
        def outer() -> None:
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

        outer()
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        self._check_node(
            root, "tests.test_tracer", "TestTracer.test_sync.<locals>.outer", 0.04
        )
        node_mid1, node_mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(
            node_mid1, "tests.test_tracer", "TestTracer.test_sync.<locals>.mid1", 0.01
        )
        self._check_node(
            node_mid2, "tests.test_tracer", "TestTracer.test_sync.<locals>.mid2", 0.02
        )
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = cast(list[Node], tree.children(node_mid2.identifier))
        self._check_node(
            node_inner, "tests.test_tracer", "TestTracer.test_sync.<locals>.inner", 0.01
        )

    @FLAKY
    async def test_async(self) -> None:
        @tracer
        async def outer() -> None:
            await asyncio.sleep(0.01)  # 0.01
            await mid1()  # 0.01
            await mid2()  # 0.02

        @tracer
        async def mid1() -> None:
            await asyncio.sleep(0.01)  # 0.01

        @tracer
        async def mid2() -> None:
            await asyncio.sleep(0.01)  # 0.01
            await inner()  # 0.01

        @tracer
        async def inner() -> None:
            await asyncio.sleep(0.01)  # 0.01

        await outer()
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        self._check_node(
            root, "tests.test_tracer", "TestTracer.test_async.<locals>.outer", 0.04
        )
        node_mid1, node_mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(
            node_mid1, "tests.test_tracer", "TestTracer.test_async.<locals>.mid1", 0.01
        )
        self._check_node(
            node_mid2, "tests.test_tracer", "TestTracer.test_async.<locals>.mid2", 0.02
        )
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = cast(list[Node], tree.children(node_mid2.identifier))
        self._check_node(
            node_inner,
            "tests.test_tracer",
            "TestTracer.test_async.<locals>.inner",
            0.01,
        )

    @FLAKY
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
            )

    def test_time_zone(self) -> None:
        @tracer(time_zone=HongKong)
        def func() -> None:
            return

        _ = func()
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.start_time.tzinfo is HongKong
        assert data.end_time is not None
        assert data.end_time.tzinfo is HongKong

    def test_suppress(self) -> None:
        @tracer(suppress=ValueError)
        def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = func()
        self._check_error_node(func, outcome="suppressed")

    def test_error_sync(self) -> None:
        @tracer
        def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = func()
        self._check_error_node(func, outcome="failure")

    async def test_error_async(self) -> None:
        @tracer
        async def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = await func()
        self._check_error_node(func, outcome="failure")

    def _check_node(
        self, node: Node, module: str, qualname: str, duration: float, /
    ) -> None:
        assert node.tag == f"{module}:{qualname}"
        data = cast(_NodeData, node.data)
        assert data.module == module
        assert data.qualname == qualname
        assert data.duration is not None
        assert data.duration.total_seconds() == approx(duration, abs=1.0)
        assert data.outcome == "success"

    def _check_error_node(
        self, func: Callable[..., Any], /, *, outcome: Literal["failure", "suppressed"]
    ) -> None:
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.outcome == outcome
        tag = f"{func.__module__}:{func.__qualname__}"
        timedelta = r"\d:\d{2}:\d{2}\.\d{6}"
        match outcome:
            case "failure":
                pattern = rf"{tag} \(ValueError, {timedelta}\)"
            case "suppressed":
                pattern = rf"{tag} \({timedelta}\)"
        assert search(pattern, data.desc)
        assert data.error is ValueError
