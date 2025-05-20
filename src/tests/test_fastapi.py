from __future__ import annotations

from asyncio import sleep
from re import search

from pytest import raises

from tests.conftest import SKIPIF_CI
from utilities.asyncio import EnhancedTaskGroup
from utilities.fastapi import PingReceiver


class TestPingReceiver:
    @SKIPIF_CI
    async def test_main(self) -> None:
        port = 5465
        receiver = PingReceiver(port=port)
        assert await PingReceiver.ping(port) is False
        await sleep(0.1)

        async def run_test() -> None:
            await sleep(0.1)
            result = await PingReceiver.ping(port)
            assert isinstance(result, str)
            assert search(
                r"pong @ \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}", result
            )

        with raises(ExceptionGroup):  # noqa: PT012
            async with EnhancedTaskGroup(timeout=1.0) as tg:
                _ = tg.create_task(receiver())
                _ = tg.create_task(run_test())
