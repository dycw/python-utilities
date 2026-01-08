from __future__ import annotations

from inspect import signature
from time import sleep
from typing import TYPE_CHECKING, ClassVar

from pytest import fixture, mark, param, raises

from utilities.asyncio import sleep_td, timeout_td
from utilities.iterables import one
from utilities.os import temp_environ
from utilities.pytest import (
    IS_CI,
    _NodeIdToPathNotGetTailError,
    _NodeIdToPathNotPythonFileError,
    node_id_path,
    throttle,
)
from utilities.throttle import throttle
from utilities.types import Delta
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.legacypath import Testdir
    from whenever import TimeDelta


_DELTA: TimeDelta = 0.1 * SECOND


class TestThrottle:
    @mark.only
    async def test_basic(self, *, tmp_path: Path) -> None:
        counter = 0
        path = tmp_path / "file.txt"

        @throttle(delta=_DELTA, path=path)
        def func() -> None:
            nonlocal counter
            counter += 1

        for _ in range(2):
            func()
            assert counter == 1
            assert path.is_file()
        await sleep_td(2 * _DELTA)
        for _ in range(2):
            func()
            assert counter == 2
            assert path.is_file()

    @mark.flaky
    @mark.parametrize("asyncio_first", [param(True), param(False)])
    @mark.parametrize("on_try", [param(True), param(False)])
    def test_async(
        self, *, testdir: Testdir, tmp_path: Path, asyncio_first: bool, on_try: bool
    ) -> None:
        if asyncio_first:
            _ = testdir.makepyfile(
                f"""
                from whenever import TimeDelta

                from pytest import mark

                from utilities.pytest import throttle

                @mark.asyncio
                @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
                async def test_main() -> None:
                    assert True
                """
            )
        else:
            _ = testdir.makepyfile(
                f"""
                from whenever import TimeDelta

                from pytest import mark

                from utilities.pytest import throttle

                @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
                @mark.asyncio
                async def test_main() -> None:
                    assert True
                """
            )
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(self.delta)
        testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    @mark.parametrize("on_try", [param(True), param(False)])
    def test_disabled_via_env_var(
        self, *, testdir: Testdir, tmp_path: Path, on_try: bool
    ) -> None:
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}), on_try={on_try})
            def test_main() -> None:
                assert True
            """
        )
        with temp_environ(THROTTLE="1"):
            testdir.runpytest().assert_outcomes(passed=1)
            testdir.runpytest().assert_outcomes(passed=1)
            sleep(self.delta)
            testdir.runpytest().assert_outcomes(passed=1)

    @mark.flaky
    def test_on_pass(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            from pytest import fixture

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @fixture
            def is_pass(request) -> bool:
                return request.config.getoption("--pass")
            """
        )
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main(*, is_pass: bool) -> None:
                assert is_pass
            """
        )
        for delta_use in [self.delta, 0.0]:
            testdir.runpytest().assert_outcomes(failed=1)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            testdir.runpytest("--pass").assert_outcomes(skipped=1)
            sleep(delta_use)

    @mark.flaky
    def test_on_try(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makeconftest(
            """
            from pytest import fixture

            def pytest_addoption(parser):
                parser.addoption("--pass", action="store_true")

            @fixture
            def is_pass(request):
                return request.config.getoption("--pass")
            """
        )
        root_str = str(tmp_path)
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={root_str!r}, delta=TimeDelta(seconds={self.delta}), on_try=True)
            def test_main(*, is_pass: bool) -> None:
                assert is_pass
            """
        )
        for delta_use in [self.delta, 0.0]:
            testdir.runpytest().assert_outcomes(failed=1)
            testdir.runpytest().assert_outcomes(skipped=1)
            sleep(self.delta)
            testdir.runpytest("--pass").assert_outcomes(passed=1)
            testdir.runpytest().assert_outcomes(skipped=1)
            sleep(delta_use)

    @mark.flaky
    def test_long_name(self, *, testdir: Testdir, tmp_path: Path) -> None:
        _ = testdir.makepyfile(
            f"""
            from pytest import mark
            from string import printable
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @mark.parametrize("arg", [10 * printable])
            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main(*, arg: str) -> None:
                assert True
            """
        )
        testdir.runpytest().assert_outcomes(passed=1)
        testdir.runpytest().assert_outcomes(skipped=1)
        sleep(self.delta)
        testdir.runpytest().assert_outcomes(passed=1)

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)

    @mark.flaky
    def test_error_decoding_timestamp(
        self, *, testdir: Testdir, tmp_path: Path
    ) -> None:
        _ = testdir.makepyfile(
            f"""
            from whenever import TimeDelta

            from utilities.pytest import throttle

            @throttle(root={str(tmp_path)!r}, delta=TimeDelta(seconds={self.delta}))
            def test_main() -> None:
                assert True
            """
        )
        testdir.runpytest().assert_outcomes(passed=1)
        path = one(tmp_path.iterdir())
        _ = path.write_text("invalid")
        testdir.runpytest().assert_outcomes(passed=1)
