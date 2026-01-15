from __future__ import annotations

from asyncio import run
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from inspect import signature
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING, override

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import mark, param, raises

import utilities.asyncio
import utilities.time
from utilities.constants import SECOND
from utilities.contextlib import (
    enhanced_async_context_manager,
    enhanced_context_manager,
    suppress_super_object_attribute_error,
)
from utilities.pytest import skipif_ci

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from whenever import TimeDelta

    from utilities.types import Duration, PathLike


_DURATION: TimeDelta = 0.05 * SECOND


def _test_enhanced_context_manager(
    path: PathLike, /, *, duration: Duration = _DURATION
) -> None:
    path = Path(path)
    path.touch()

    @enhanced_context_manager
    def yield_marker() -> Iterator[None]:
        try:
            yield
        finally:
            path.unlink(missing_ok=True)

    with yield_marker():
        utilities.time.sleep(duration)


def _test_enhanced_async_context_manager_entry(
    path: PathLike, /, *, duration: Duration = _DURATION
) -> None:
    run(_test_enhanced_async_context_manager_core(path, duration=duration))


async def _test_enhanced_async_context_manager_core(
    path: PathLike, /, *, duration: Duration = _DURATION
) -> None:
    path = Path(path)
    path.touch()

    @enhanced_async_context_manager
    async def yield_marker() -> AsyncIterator[None]:
        await utilities.asyncio.sleep()
        try:
            yield
        finally:
            path.unlink(missing_ok=True)

    async with yield_marker():
        await utilities.asyncio.sleep(duration)


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

    def test_sync_signature(self) -> None:
        @enhanced_context_manager
        def yield_marker(x: int, y: int, /) -> Iterator[int]:
            yield x + y

        sig = set(signature(yield_marker).parameters)
        expected = {"x", "y"}
        assert sig == expected

    def test_sync_threaded(self) -> None:
        cleared = False

        def func() -> None:
            with yield_marker():
                ...

        @enhanced_context_manager
        def yield_marker() -> Iterator[None]:
            try:
                yield
            finally:
                nonlocal cleared
                cleared |= True

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(func)
            future.result()

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
            await utilities.asyncio.sleep()
            try:
                yield
            finally:
                nonlocal cleared
                cleared |= True

        async with yield_marker():
            assert not cleared
        assert cleared

    def test_async_signature(self) -> None:
        @enhanced_async_context_manager
        async def yield_marker(x: int, y: int, /) -> AsyncIterator[int]:
            await utilities.asyncio.sleep()
            yield x + y

        sig = set(signature(yield_marker).parameters)
        expected = {"x", "y"}
        assert sig == expected

    def test_async_threaded(self) -> None:
        cleared = False

        def sync_func() -> None:
            run(async_func())

        async def async_func() -> None:
            async with yield_marker():
                ...

        @enhanced_async_context_manager
        async def yield_marker() -> AsyncIterator[None]:
            await utilities.asyncio.sleep()
            try:
                yield
            finally:
                nonlocal cleared
                cleared |= True

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(sync_func)
            future.result()

    @mark.parametrize(
        "target",
        [
            param(_test_enhanced_context_manager),
            param(_test_enhanced_async_context_manager_entry),
        ],
    )
    @skipif_ci
    def test_multiprocessing_sigterm(
        self, *, tmp_path: Path, target: Callable[..., None]
    ) -> None:
        duration = SECOND
        marker = tmp_path.joinpath("marker")
        proc = Process(target=target, args=(marker,), kwargs={"sleep": 4 * duration})
        proc.start()
        assert proc.pid is not None
        assert proc.is_alive()
        assert not marker.exists()
        utilities.time.sleep(duration)
        assert proc.is_alive()
        assert marker.is_file()
        proc.terminate()
        utilities.time.sleep(duration)
        assert proc.is_alive()
        assert not marker.exists()


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

        with raises(AttributeError, match=r"'Child' object has no attribute 'error'"):
            _ = Child()
