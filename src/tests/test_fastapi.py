from __future__ import annotations

from asyncio import sleep

from utilities.fastapi import PingReceiver


class TestPingReceiver:
    async def test_main(self) -> None:
        port = 5465
        assert not await PingReceiver.ping(port)
        await sleep(0.1)
        async with PingReceiver(port=port):
            assert await PingReceiver.ping(port)
        await sleep(0.1)
        assert not await PingReceiver.ping(port)
