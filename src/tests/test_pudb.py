from __future__ import annotations

from asyncio import sleep

from hypothesis import given
from pytest import raises

from utilities.hypothesis import text_ascii
from utilities.pudb import call_pudb


class TestCallPUDB:
    def test_sync(self) -> None:
        @call_pudb
        def func(x: int, y: int, /) -> float:
            return x / y

        with raises(ZeroDivisionError):
            _ = func(1, 0)

    async def test_async(self) -> None:
        @call_pudb
        async def func(x: int, y: int, /) -> float:
            await sleep(0.01)
            return x / y

        with raises(ZeroDivisionError):
            _ = await func(1, 0)

    @given(env_var=text_ascii(min_size=1))
    def test_custom_env_var(self, *, env_var: str) -> None:
        @call_pudb(env_var=env_var)
        def func(x: int, y: int, /) -> float:
            return x / y

        with raises(ZeroDivisionError):
            _ = func(1, 0)
