from __future__ import annotations

from inspect import signature
from time import sleep
from typing import TYPE_CHECKING, NoReturn

from pytest import mark, param, raises

from utilities.asyncio import sleep_td
from utilities.iterables import one
from utilities.os import temp_environ
from utilities.throttle import throttle
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.legacypath import Testdir
    from whenever import TimeDelta


_DELTA: TimeDelta = 0.1 * SECOND


class TestThrottle:
    async def test_sync_on_pass_func_passing(self, *, temp_file: Path) -> None:
        counter = 0

        @throttle(delta=_DELTA, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1

        for _ in range(2):
            func()
            assert counter == 1
            assert temp_file.is_file()
        await sleep_td(2 * _DELTA)
        for _ in range(2):
            func()
            assert counter == 2
            assert temp_file.is_file()

    async def test_sync_on_pass_func_failing(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        @throttle(delta=_DELTA, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1
            raise CustomError

        for i in range(1, 3):
            with raises(CustomError):
                func()
            assert counter == i
            assert not temp_file.exists()

    async def test_sync_on_pass_with_raiser(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        def raiser() -> NoReturn:
            raise CustomError

        @throttle(delta=_DELTA, path=temp_file, raiser=raiser)
        def func() -> None:
            nonlocal counter
            counter += 1

        func()
        assert counter == 1
        assert temp_file.is_file()
        with raises(CustomError):
            func()
        assert counter == 1
        assert temp_file.is_file()

    @mark.skip
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

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)

    @mark.skip
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
