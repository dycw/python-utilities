from __future__ import annotations

import time
from dataclasses import dataclass
from multiprocessing import Process
from pathlib import Path
from typing import TYPE_CHECKING, override

from hypothesis import given
from hypothesis.strategies import booleans
from pytest import raises

from utilities.contextlib import (
    enhanced_context_manager,
    suppress_super_object_attribute_error,
)
from utilities.hypothesis import temp_paths

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


def _test_enhanced_context_manager(path: PathLike, /, *, sleep: float = 0.1) -> None:
    path = Path(path)
    path.touch()

    @enhanced_context_manager
    def _yield_marker() -> Iterator[None]:
        try:
            yield
        finally:
            path.unlink(missing_ok=True)

    with _yield_marker():
        time.sleep(sleep)


class TestEnhancedContextManager:
    @given(
        root=temp_paths(),
        sigabrt=booleans(),
        sigfpe=booleans(),
        sigill=booleans(),
        sigint=booleans(),
        sigsegv=booleans(),
        sigterm=booleans(),
    )
    def test_main(
        self,
        *,
        root: Path,
        sigabrt: bool,
        sigfpe: bool,
        sigill: bool,
        sigint: bool,
        sigsegv: bool,
        sigterm: bool,
    ) -> None:
        path = root.joinpath("marker")
        path.touch()

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
                path.unlink(missing_ok=True)

        with yield_marker():
            time.sleep(0.01)
            assert path.is_file()
        time.sleep(0.01)
        assert not path.is_file()

    def test_sigterm(self, *, tmp_path: Path) -> None:
        sleep = 0.5
        marker = tmp_path.joinpath("marker")
        proc = Process(
            target=_test_enhanced_context_manager,
            args=(marker,),
            kwargs={"sleep": 3 * sleep},
        )
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
