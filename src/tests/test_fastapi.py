from __future__ import annotations

from re import search
from typing import TYPE_CHECKING, ClassVar, Literal

from tests.conftest import SKIPIF_CI
from utilities.asyncio import sleep_td
from utilities.fastapi import yield_ping_receiver
from utilities.whenever import SECOND

if TYPE_CHECKING:
    from whenever import TimeDelta


class TestPingReceiver:
    delta: ClassVar[TimeDelta] = 0.1 * SECOND
    port: ClassVar[int] = 5465

    @SKIPIF_CI
    async def test_main(self) -> None:
        assert await self.ping() is False
        await sleep_td(self.delta)
        async with yield_ping_receiver(self.port, timeout=2 * self.delta):
            await sleep_td(self.delta)
            result = await self.ping()
            assert isinstance(result, str)
            assert search(
                r"pong @ \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}", result
            )
        await sleep_td(self.delta)
        assert await self.ping() is False

    async def ping(self) -> str | Literal[False]:
        """Ping the receiver."""
        from httpx import AsyncClient, ConnectError  # skipif-ci

        url = f"http://localhost:{self.port}/ping"  # skipif-ci
        try:  # skipif-ci
            async with AsyncClient() as client:
                response = await client.get(url)
        except ConnectError:  # skipif-ci
            return False
        return response.text if response.status_code == 200 else False  # skipif-ci
