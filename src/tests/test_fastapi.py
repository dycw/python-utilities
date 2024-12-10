from __future__ import annotations

from asyncio import sleep

from httpx import AsyncClient, ConnectError
from pytest import raises

from utilities.fastapi import PingReceiver


class TestPingReceiver:
    async def test_ping_receiver(self) -> None:
        port = 5465
        url = f"http://localhost:{port}/ping"
        async with AsyncClient() as client:
            with raises(ConnectError):
                _ = await client.get(url)
            async with PingReceiver(port=port):
                await sleep(0.1)
                response = await client.get(url)
                assert response.status_code == 200
                assert response.text == '"pong"'
            await sleep(0.1)
            with raises(ConnectError):
                _ = await client.get(url)
