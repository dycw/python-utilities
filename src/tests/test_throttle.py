from __future__ import annotations

from inspect import signature
from typing import TYPE_CHECKING, NoReturn

from pytest import mark, param, raises

from utilities.asyncio import sleep
from utilities.constants import SECOND
from utilities.os import temp_environ
from utilities.throttle import (
    _ThrottleMarkerFileError,
    _ThrottleParseZonedDateTimeError,
    throttle,
)

if TYPE_CHECKING:
    from pathlib import Path

    from whenever import TimeDelta


_DURATION: TimeDelta = 0.01 * SECOND
_MULTIPLE: int = 2


class TestThrottle:
    @mark.parametrize("on_try", [param(False), param(True)])
    async def test_sync_func_passing(self, *, on_try: bool, temp_file: Path) -> None:
        counter = 0

        @throttle(on_try=on_try, duration=_DURATION, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1

        for _ in range(2):
            func()
            assert counter == 1
            assert temp_file.is_file()
        await sleep(_MULTIPLE * _DURATION)
        for _ in range(2):
            func()
            assert counter == 2

    @mark.parametrize("on_try", [param(False), param(True)])
    async def test_sync_func_with_raiser(
        self, *, on_try: bool, temp_file: Path
    ) -> None:
        class CustomError(Exception): ...

        counter = 0

        def raiser() -> NoReturn:
            raise CustomError

        @throttle(on_try=on_try, duration=_DURATION, path=temp_file, raiser=raiser)
        def func() -> None:
            nonlocal counter
            counter += 1

        func()
        assert counter == 1
        assert temp_file.is_file()
        with raises(CustomError):
            func()
        assert counter == 1

    async def test_sync_func_on_pass_failing(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        @throttle(duration=_DURATION, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1
            raise CustomError

        for i in range(2):
            with raises(CustomError):
                func()
            assert counter == (i + 1)
            assert not temp_file.exists()

    async def test_sync_on_func_on_try_failing(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        @throttle(on_try=True, duration=_DURATION, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1
            raise CustomError

        with raises(CustomError):
            func()
        assert counter == 1
        assert temp_file.is_file()
        func()
        assert counter == 1
        await sleep(_MULTIPLE * _DURATION)
        with raises(CustomError):
            func()
        assert counter == 2
        assert temp_file.is_file()
        func()
        assert counter == 2

    @mark.parametrize("on_try", [param(False), param(True)])
    async def test_async_func_passing(self, *, on_try: bool, temp_file: Path) -> None:
        counter = 0

        @throttle(on_try=on_try, duration=_DURATION, path=temp_file)
        async def func() -> None:
            await sleep()
            nonlocal counter
            counter += 1

        for _ in range(2):
            await func()
            assert counter == 1
            assert temp_file.is_file()
        await sleep(_MULTIPLE * _DURATION)
        for _ in range(2):
            await func()
            assert counter == 2

    @mark.parametrize("on_try", [param(False), param(True)])
    async def test_async_func_with_raiser(
        self, *, on_try: bool, temp_file: Path
    ) -> None:
        class CustomError(Exception): ...

        counter = 0

        def raiser() -> NoReturn:
            raise CustomError

        @throttle(on_try=on_try, duration=_DURATION, path=temp_file, raiser=raiser)
        async def func() -> None:
            await sleep()
            nonlocal counter
            counter += 1

        await func()
        assert counter == 1
        assert temp_file.is_file()
        with raises(CustomError):
            await func()
        assert counter == 1

    async def test_async_func_on_pass_failing(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        @throttle(duration=_DURATION, path=temp_file)
        async def func() -> None:
            await sleep()
            nonlocal counter
            counter += 1
            raise CustomError

        for i in range(2):
            with raises(CustomError):
                await func()
            assert counter == (i + 1)
            assert not temp_file.exists()

    async def test_async_on_func_on_try_failing(self, *, temp_file: Path) -> None:
        class CustomError(Exception): ...

        counter = 0

        @throttle(on_try=True, duration=_DURATION, path=temp_file)
        async def func() -> None:
            await sleep()
            nonlocal counter
            counter += 1
            raise CustomError

        with raises(CustomError):
            await func()
        assert counter == 1
        assert temp_file.is_file()
        await func()
        assert counter == 1
        await sleep(_MULTIPLE * _DURATION)
        with raises(CustomError):
            await func()
        assert counter == 2
        assert temp_file.is_file()
        await func()
        assert counter == 2

    def test_env_var(self, *, temp_file: Path) -> None:
        counter = 0

        @throttle(duration=_DURATION, path=temp_file)
        def func() -> None:
            nonlocal counter
            counter += 1

        with temp_environ(THROTTLE="1"):
            for i in range(2):
                func()
                assert counter == (i + 1)
                assert temp_file.is_file()

    def test_signature(self) -> None:
        @throttle()
        def func(*, fix: bool) -> None:
            assert fix

        def other(*, fix: bool) -> None:
            assert fix

        assert signature(func) == signature(other)

    def test_error_marker_file(self, *, temp_path_not_exist: Path) -> None:
        temp_path_not_exist.mkdir()

        @throttle(duration=_DURATION, path=temp_path_not_exist)
        def func() -> None: ...

        with raises(_ThrottleMarkerFileError, match=r"Invalid marker file '.*'"):
            func()

    def test_error_parse_zoned_date_time(self, *, temp_file: Path) -> None:
        _ = temp_file.write_text("invalid")

        @throttle(duration=_DURATION, path=temp_file)
        def func() -> None: ...

        with raises(
            _ThrottleParseZonedDateTimeError,
            match=r"Unable to parse the contents 'invalid' of '.*' to a ZonedDateTime",
        ):
            func()
