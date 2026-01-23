from __future__ import annotations

from re import search
from typing import TYPE_CHECKING, ClassVar, Literal

from utilities.constants import SECOND
from utilities.core import async_sleep
from utilities.fastapi import yield_ping_receiver
from utilities.pytest import skipif_ci

if TYPE_CHECKING:
    from whenever import TimeDelta


_DURATION: TimeDelta = 0.05 * SECOND
_MULTIPLE: int = 10


class TestPingReceiver:
    port: ClassVar[int] = 5465

    @skipif_ci
    async def test_main(self) -> None:
        assert await self.ping() is False
        await async_sleep(_DURATION)
        async with yield_ping_receiver(self.port, timeout=_MULTIPLE * _DURATION):
            await async_sleep(_DURATION)
            result = await self.ping()
            assert isinstance(result, str)
            assert search(
                r"pong @ \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}", result
            )
        await async_sleep(_DURATION)
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
