from __future__ import annotations

import asyncio
import time
from asyncio import run
from dataclasses import dataclass
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING, override

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI
from utilities.contextlib import (
    enhanced_async_context_manager,
    enhanced_context_manager,
    suppress_super_object_attribute_error,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from utilities.types import PathLike


def _test_enhanced_context_manager(path: PathLike, /, *, sleep: float = 0.1) -> None:
    path = Path(path)
    path.touch()

    @enhanced_context_manager
    def yield_marker() -> Iterator[None]:
        try:
            yield
        finally:
            path.unlink(missing_ok=True)

    with yield_marker():
        time.sleep(sleep)


def _test_enhanced_async_context_manager_entry(
    path: PathLike, /, *, sleep: float = 0.1
) -> None:
    run(_test_enhanced_async_context_manager_core(path, sleep=sleep))


async def _test_enhanced_async_context_manager_core(
    path: PathLike, /, *, sleep: float = 0.1
) -> None:
    path = Path(path)
    path.touch()

    @enhanced_async_context_manager
    async def yield_marker() -> AsyncIterator[None]:
        try:
            yield
        finally:
            path.unlink(missing_ok=True)

    async with yield_marker():
        await asyncio.sleep(sleep)


class TestEnhancedContextManager:
    @given(
        sigabrt=booleans(),
        sigfpe=booleans(),
        sigill=booleans(),
        sigint=booleans(),
        sigsegv=booleans(),
        sigterm=booleans(),
    )
    def test_sync(
        self,
        *,
        sigabrt: bool,
        sigfpe: bool,
        sigill: bool,
        sigint: bool,
        sigsegv: bool,
        sigterm: bool,
    ) -> None:
        cleared = False

        @enhanced_context_manager(
            sigabrt=sigabrt,
            sigfpe=sigfpe,
            sigill=sigill,
            sigint=sigint,
            sigsegv=sigsegv,
            sigterm=sigterm,
        )
        def yield_marker() -> Iterator[None]:
            try:
                yield
            finally:
                nonlocal cleared
                cleared |= True

        with yield_marker():
            assert not cleared
        assert cleared

    @given(
        sigabrt=booleans(),
        sigfpe=booleans(),
        sigill=booleans(),
        sigint=booleans(),
        sigsegv=booleans(),
        sigterm=booleans(),
    )
    async def test_async(
        self,
        *,
        sigabrt: bool,
        sigfpe: bool,
        sigill: bool,
        sigint: bool,
        sigsegv: bool,
        sigterm: bool,
    ) -> None:
        cleared = False

        @enhanced_async_context_manager(
            sigabrt=sigabrt,
            sigfpe=sigfpe,
            sigill=sigill,
            sigint=sigint,
            sigsegv=sigsegv,
            sigterm=sigterm,
        )
        async def yield_marker() -> AsyncIterator[None]:
            try:
                yield
            finally:
                nonlocal cleared
                cleared |= True

        async with yield_marker():
            assert not cleared
        assert cleared

    @mark.parametrize(
        "target",
        [
            param(_test_enhanced_context_manager),
            param(_test_enhanced_async_context_manager_entry),
        ],
    )
    @SKIPIF_CI
    def test_multiprocessing_sigterm(
        self, *, tmp_path: Path, target: Callable[..., None]
    ) -> None:
        sleep = 0.5
        marker = tmp_path.joinpath("marker")
        proc = Process(target=target, args=(marker,), kwargs={"sleep": 4 * sleep})
        proc.start()
        assert proc.pid is not None
        assert proc.is_alive()
        assert not marker.is_file()
        time.sleep(sleep)
        assert proc.is_alive()
        assert marker.is_file()
        proc.terminate()
        time.sleep(sleep)
        assert proc.is_alive()
        assert not marker.is_file()


class TestSuppressSuperObjectAttributeError:
    def test_main(self) -> None:
        inits: list[str] = []

        @dataclass(kw_only=True)
        class A:
            def __post_init__(self) -> None:
                with suppress_super_object_attribute_error():
                    super().__post_init__()  # pyright:ignore [reportAttributeAccessIssue]
                nonlocal inits
                inits.append("A")

        @dataclass(kw_only=True)
        class B: ...

        @dataclass(kw_only=True)
        class C:
            def __post_init__(self) -> None:
                with suppress_super_object_attribute_error():
                    super().__post_init__()  # pyright:ignore [reportAttributeAccessIssue]
                nonlocal inits
                inits.append("C")

        @dataclass(kw_only=True)
        class D: ...

        @dataclass(kw_only=True)
        class E(A, B, C, D):
            @override
            def __post_init__(self) -> None:
                super().__post_init__()
                nonlocal inits
                inits.append("E")

        _ = E()
        assert inits == ["C", "A", "E"]

    def test_error(self) -> None:
        @dataclass(kw_only=True)
        class Parent:
            def __post_init__(self) -> None:
                with suppress_super_object_attribute_error():
                    _ = self.error  # pyright:ignore [reportAttributeAccessIssue]

        @dataclass(kw_only=True)
        class Child(Parent):
            @override
            def __post_init__(self) -> None:
                super().__post_init__()

        with raises(AttributeError, match="'Child' object has no attribute 'error'"):
            _ = Child()
