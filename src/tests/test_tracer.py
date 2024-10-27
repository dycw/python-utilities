from __future__ import annotations

import asyncio
import time
from asyncio import Task, TaskGroup, sleep
from functools import partial
from re import search
from typing import TYPE_CHECKING, Any, Literal

from pytest import approx, fixture, raises

from tests.conftest import FLAKY
from utilities.functions import get_class_name
from utilities.iterables import one
from utilities.tracer import (
    NodeData,
    filter_failures,
    get_tracer_trees,
    set_tracer_trees,
    tracer,
)
from utilities.zoneinfo import HongKong

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from utilities.treelib import Node


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
        root = tree[tree.root]
        self._check_node(root, outer, 0.04)
        node_mid1, node_mid2 = tree.children(root.identifier)
        self._check_node(node_mid1, mid1, 0.01)
        self._check_node(node_mid2, mid2, 0.02)
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = tree.children(node_mid2.identifier)
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
        root = tree[tree.root]
        self._check_node(root, outer, 0.04)
        node_mid1, node_mid2 = tree.children(root.identifier)
        self._check_node(node_mid1, mid1, 0.01)
        self._check_node(node_mid2, mid2, 0.02)
        assert len(tree.children(node_mid1.identifier)) == 0
        (node_inner,) = tree.children(node_mid2.identifier)
        self._check_node(node_inner, inner, 0.01)

    def test_methods(self) -> None:
        class Example:
            @tracer
            def func(self, n: int, /) -> int:
                return n + 1

        assert Example().func(1) == 2
        tree = one(get_tracer_trees())
        root = tree[tree.root]
        assert (
            root.data.tag
            == "tests.test_tracer:TestTracer.test_methods.<locals>.Example.func"
        )

    @FLAKY
    def test_multiple_calls(self) -> None:
        @tracer
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        assert func(1) == 2
        trees = get_tracer_trees()
        assert len(trees) == 2

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
        data = tree[tree.root].data
        assert data.start_time.tzinfo is HongKong
        assert data.end_time is not None
        assert data.end_time.tzinfo is HongKong

    def test_pre_call_sync(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        @tracer(pre_call=partial(self.pre_call, path=path))
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_pre_call(path)

    async def test_pre_call_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        @tracer(pre_call=partial(self.pre_call, path=path))
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

        @tracer(post_error=partial(self.post_error, path=path))
        def func(n: int, /) -> int:
            if n >= 1:
                return n + 1
            msg = f"{n=} must be positive"
            raise ValueError(msg)

        with raises(ValueError, match="n=0 must be positive"):
            _ = func(0)
        self._check_post_error(path)

    async def test_post_error_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        @tracer(post_error=partial(self.post_error, path=path))
        async def func(n: int, /) -> int:
            await sleep(0.01)
            if n >= 1:
                return n + 1
            msg = f"{n=} must be positive"
            raise ValueError(msg)

        with raises(ValueError, match="n=0 must be positive"):
            _ = await func(0)
        self._check_post_error(path)

    def test_post_result_sync(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        @tracer(post_result=partial(self.post_result, path=path))
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        self._check_post_result(path)

    async def test_post_result_async(self, *, tmp_path: Path) -> None:
        path = tmp_path.joinpath("log")

        @tracer(post_result=partial(self.post_result, path=path))
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
        data = node.data
        assert data.module == func.__module__
        assert data.qualname == func.__qualname__
        assert data.duration is not None
        assert data.duration.total_seconds() == approx(duration, abs=1.0)
        assert data.outcome == "success"

    def _check_add_args(self) -> None:
        tree = one(get_tracer_trees())
        data = tree[tree.root].data
        assert data.args == (1,)
        assert data.kwargs == {}

    def pre_call(self, _: NodeData[Any], n: int, /, *, path: Path) -> None:
        with path.open(mode="w") as fh:
            _ = fh.write(f"Calling with {n=}")  # pyright: ignore[reportAssignmentType]

    def _check_pre_call(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == ["Calling with n=1"]

    def post_error(self, data: NodeData[Any], /, *, path: Path) -> None:
        assert data.args is not None
        assert data.kwargs is not None
        assert data.end_time is not None
        assert data.outcome in {"failure", "suppressed"}
        assert data.error is not None
        with path.open(mode="w") as fh:
            _ = fh.write(
                f"Raised a {get_class_name(data.error)} with {data.args=}/{data.kwargs=}"
            )

    def _check_post_error(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == [
                "Raised a ValueError with data.args=(0,)/data.kwargs={}"
            ]

    def post_result(self, data: NodeData[Any], result: int, /, *, path: Path) -> None:
        assert data.end_time is not None
        assert data.outcome == "success"
        with path.open(mode="w") as fh:
            _ = fh.write(f"Result was {result}")

    def _check_post_result(self, path: Path, /) -> None:
        with path.open() as fh:
            assert fh.readlines() == ["Result was 2"]

    def _check_add_result(self) -> None:
        tree = one(get_tracer_trees())
        data = tree[tree.root].data
        assert data.result == 2

    def _check_error(
        self, func: Callable[..., Any], /, *, outcome: Literal["failure", "suppressed"]
    ) -> None:
        tree = one(get_tracer_trees())
        data = tree[tree.root].data
        assert data.outcome == outcome
        tag = f"{func.__module__}:{func.__qualname__}"
        timedelta = r"\d:\d{2}:\d{2}(?:\.\d{6})?"
        match outcome:
            case "failure":
                pattern = rf"^{tag} \(ValueError, {timedelta}\)$"
            case "suppressed":
                pattern = rf"^{tag} \({timedelta}\)$"
        assert search(pattern, data.desc)
        assert data.args is not None
        assert data.kwargs is not None
        assert data.end_time is not None
        assert data.outcome in {"failure", "suppressed"}
        assert isinstance(data.error, ValueError)


class TestFilterFailures:
    def test_sync(self) -> None:
        @tracer
        def outer(n: int, /) -> list[int]:
            return list(map(inner, range(n, -n - 1, -1)))

        @tracer
        def inner(n: int, /) -> int:
            if n >= 1:
                return n + 1
            msg = f"{n=} must be positive"
            raise ValueError(msg)

        with raises(ValueError, match="n=0 must be positive"):
            _ = outer(3)
        tree = one(get_tracer_trees())
        assert tree.size() == 5
        subtree = filter_failures(tree)
        assert subtree is not None
        assert subtree.size() == 2

    @FLAKY
    async def test_async(self) -> None:
        @tracer
        async def outer(n: int, /) -> list[int]:
            tasks: set[Task[int]] = set()
            async with TaskGroup() as tg:
                for i in range(n, -n - 1, -1):
                    tasks.add(tg.create_task(inner(i)))
            return [t.result() for t in tasks]

        @tracer
        async def inner(n: int, /) -> int:
            await asyncio.sleep(0.01)
            if n >= 1:
                return n + 1
            msg = f"{n=} must be positive"
            raise ValueError(msg)

        with raises(ExceptionGroup):
            _ = await outer(3)
        tree = one(get_tracer_trees())
        assert tree.size() == 8
        subtree = filter_failures(tree)
        assert subtree is not None
        assert subtree.size() == 5

    def test_no_failure(self) -> None:
        @tracer
        def func(n: int, /) -> int:
            return n + 1

        assert func(1) == 2
        tree = one(get_tracer_trees())
        result = filter_failures(tree)
        assert result is None

    @FLAKY
    async def test_list(self) -> None:
        @tracer
        async def outer(n: int, /) -> list[int]:
            tasks: set[Task[int]] = set()
            async with TaskGroup() as tg:
                for i in range(n, -n - 1, -1):
                    tasks.add(tg.create_task(inner(i)))
            return [t.result() for t in tasks]

        @tracer
        async def inner(n: int, /) -> int:
            await asyncio.sleep(0.01)
            if n >= 1:
                return n + 1
            msg = f"{n=} must be positive"
            raise ValueError(msg)

        assert await outer(-1) == []
        with raises(ExceptionGroup):
            _ = await outer(3)
        with raises(ExceptionGroup):
            _ = await outer(4)
        trees = get_tracer_trees()
        assert len(trees) == 3
        assert [t.size() for t in trees] == [1, 8, 10]
        subtrees = filter_failures(trees)
        assert len(subtrees) == 2
        assert [t.size() for t in subtrees] == [5, 6]
