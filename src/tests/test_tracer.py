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
    from pathlib import Path


@fixture(autouse=True)
def set_tracer_tree_per_function() -> None:
    set_tracer_trees([])


class TestTracer:
    @FLAKY
    def test_sync(self) -> None:
        @tracer
        def outer(n: int, /) -> int:
            time.sleep(0.01)  # 0.01
            n = mid1(n + 1)  # 0.01
            return mid2(n + 1)  # 0.02

        @tracer
        def mid1(n: int, /) -> int:
            time.sleep(0.01)  # 0.01
            return n + 1

        @tracer
        def mid2(n: int, /) -> int:
            time.sleep(0.01)  # 0.01
            return inner(n + 1)  # e.01

        @tracer
        def inner(n: int, /) -> int:
            time.sleep(0.01)  # 0.01
            return n + 1

        assert outer(1) == 6
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        self._check_node(root, outer, 0.04)
        node_mid1, node_mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(node_mid1, mid1, 0.01)
        self._check_node(node_mid2, mid2, 0.02)
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = cast(list[Node], tree.children(node_mid2.identifier))
        self._check_node(node_inner, inner, 0.01)

    @FLAKY
    async def test_async(self) -> None:
        @tracer
        async def outer(n: int, /) -> int:
            await asyncio.sleep(0.01)  # 0.01
            n = await mid1(n + 1)  # 0.01
            return await mid2(n + 1)  # 0.02

        @tracer
        async def mid1(n: int, /) -> int:
            await asyncio.sleep(0.01)  # 0.01
            return n + 1

        @tracer
        async def mid2(n: int, /) -> int:
            await asyncio.sleep(0.01)  # 0.01
            return await inner(n + 1)  # 0.01

        @tracer
        async def inner(n: int, /) -> int:
            await asyncio.sleep(0.01)  # 0.01
            return n + 1

        assert await outer(1) == 6
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        self._check_node(root, outer, 0.04)
        node_mid1, node_mid2 = cast(list[Node], tree.children(root.identifier))
        self._check_node(node_mid1, mid1, 0.01)
        self._check_node(node_mid2, mid2, 0.02)
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = cast(list[Node], tree.children(node_mid2.identifier))
        self._check_node(node_inner, inner, 0.01)

    @FLAKY
    def test_multiple_calls(self) -> None:
        @tracer
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        assert func(1) == 2
        trees = get_tracer_trees()
        assert len(trees) == 2
        for tree in trees:
            root: Node = tree[tree.root]
            self._check_node(root, func, 0.02)

    def test_add_args_sync(self) -> None:
        @tracer(add_args=True)
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_add_args()

    async def test_add_args_async(self) -> None:
        @tracer(add_args=True)
        async def func(n: int, /) -> int:
            await asyncio.sleep(0.01)
            return n + 1

        assert await func(1) == 2
        self._check_add_args()

    def test_time_zone(self) -> None:
        @tracer(time_zone=HongKong)
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.start_time.tzinfo is HongKong
        assert data.end_time is not None
        assert data.end_time.tzinfo is HongKong

    def test_pre_call_sync(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def pre_call(n: int, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Calling with {n=}")

        @tracer(pre_call=pre_call)
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_pre_call(path)

    async def test_pre_call_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def pre_call(n: int, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Calling with {n=}")

        @tracer(pre_call=pre_call)
        async def func(n: int, /) -> int:
            await asyncio.sleep(0.01)
            return n + 1

        assert await func(1) == 2
        self._check_pre_call(path)

    def test_suppress(self) -> None:
        @tracer(suppress=ValueError)
        def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = func()
        self._check_error(func, outcome="suppressed")

    def test_post_error_sync(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def post_error(error: Exception, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Raised a {get_class_name(error)}")

        @tracer(post_error=post_error)
        def func() -> int:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            assert func()
        self._check_post_error(path)

    async def test_post_error_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def post_error(error: Exception, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Raised a {get_class_name(error)}")

        @tracer(post_error=post_error)
        async def func() -> int:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = await func()
        self._check_post_error(path)

    def test_post_result_sync(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def post_result(n: int, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Result was {n=}")

        @tracer(post_result=post_result)
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_post_result(path)

    async def test_post_result_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        def post_result(n: int, /) -> None:
            with path.open(mode="w") as fh:
                _ = fh.write(f"Result was {n=}")

        @tracer(post_result=post_result)
        async def func(n: int, /) -> int:
            await asyncio.sleep(0.01)
            return n + 1

        assert await func(1) == 2
        self._check_post_result(path)

    def test_add_result_sync(self) -> None:
        @tracer(add_result=True)
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_add_result()

    async def test_add_result_async(self) -> None:
        @tracer(add_result=True)
        async def func(n: int, /) -> int:
            await asyncio.sleep(0.01)
            return n + 1

        assert await func(1) == 2
        self._check_add_result()

    def test_error_sync(self) -> None:
        @tracer
        def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = func()
        self._check_error(func, outcome="failure")

    async def test_error_async(self) -> None:
        @tracer
        async def func() -> None:
            msg = "Always fails"
            raise ValueError(msg)

        with raises(ValueError, match="Always fails"):
            _ = await func()
        self._check_error(func, outcome="failure")

    def _check_node(
        self, node: Node, func: Callable[..., Any], duration: float, /
    ) -> None:
        tag = f"{func.__module__}:{func.__qualname__}"
        assert node.tag == tag
        data = cast(_NodeData, node.data)
        assert data.module == func.__module__
        assert data.qualname == func.__qualname__
        assert data.duration is not None
        assert data.duration.total_seconds() == approx(duration, abs=1.0)
        assert data.outcome == "success"

    def _check_add_args(self) -> None:
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.args == (1,)
        assert data.kwargs == {}

    def _check_pre_call(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == ["Calling with n=1"]

    def _check_post_error(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == ["Raised a ValueError"]

    def _check_post_result(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == ["Result was n=2"]

    def _check_add_result(self) -> None:
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.result == 2

    def _check_error(
        self, func: Callable[..., Any], /, *, outcome: Literal["failure", "suppressed"]
    ) -> None:
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        data = cast(_NodeData, root.data)
        assert data.outcome == outcome
        tag = f"{func.__module__}:{func.__qualname__}"
        timedelta = r"\d:\d{2}:\d{2}(?:\.\d{6})?"
        match outcome:
            case "failure":
                pattern = rf"{tag} \(ValueError, {timedelta}\)"
            case "suppressed":
                pattern = rf"{tag} \({timedelta}\)"
        assert search(pattern, data.desc)
        assert data.error is ValueError


class TestFilterFailures:
    def test_main(self) -> None:
        @tracer
        def outer(n: int, /) -> int:
            return mid(n + 1)

        @tracer
        def mid(n: int, /) -> int:
            n = inner(n + 1)
            msg = "Always fails"
            raise ValueError(msg)

        @tracer
        def inner(n: int, /) -> int:
            return n + 1

        with raises(ValueError, match="Always fails"):
            _ = outer(1)
        tree = one(get_tracer_trees())
        root: Node = tree[tree.root]
        cast(_NodeData, root.data)
