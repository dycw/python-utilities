from __future__ import annotations

from asyncio import sleep
from re import search

from tests.conftest import SKIPIF_CI
from utilities.fastapi import PingReceiver


class TestPingReceiver:
    @SKIPIF_CI
    async def test_main(self) -> None:
        port = 5465
        assert await PingReceiver.ping(port) is False
        await sleep(0.1)
        async with PingReceiver(port=port):
            await sleep(0.1)
            result = await PingReceiver.ping(port)
            assert isinstance(result, str)
            assert search(
                r"pong @ \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}", result
            )
        await sleep(0.1)
        assert await PingReceiver.ping(port) is False
