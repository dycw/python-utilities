from __future__ import annotations

from asyncio import sleep

from tests.conftest import SKIPIF_CI
from utilities.fastapi import PingReceiver


class TestPingReceiver:
    @SKIPIF_CI
    async def test_main(self) -> None:
        port = 5465
        assert not await PingReceiver.ping(port)
        await sleep(0.1)
        async with PingReceiver(port=port):
            await sleep(0.1)
            assert await PingReceiver.ping(port)
        await sleep(0.1)
        assert not await PingReceiver.ping(port)
