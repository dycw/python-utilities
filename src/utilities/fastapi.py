from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any, override

from fastapi import FastAPI
from uvicorn import Config, Server

from utilities.asyncio import AsyncService
from utilities.datetime import SECOND, datetime_duration_to_float, get_now_local

if TYPE_CHECKING:
    from utilities.types import Duration


_LOCALHOST: str = "localhost"
_TIMEOUT: Duration = SECOND


class _PingerReceiverApp(FastAPI):
    """App for the ping pinger."""

    @override
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # skipif-ci

        @self.get("/ping")  # skipif-ci
        def ping() -> str:
            from utilities.whenever import serialize_zoned_datetime  # skipif-ci

            now = serialize_zoned_datetime(get_now_local())  # skipif-ci
            return f"pong @ {now}"  # skipif-ci

        _ = ping  # skipif-ci


@dataclass(kw_only=True)
class PingReceiver(AsyncService):
    """A ping receiver."""

    host: InitVar[str] = _LOCALHOST
    port: InitVar[int]
    _app: _PingerReceiverApp = field(
        default_factory=_PingerReceiverApp, init=False, repr=False
    )
    _server: Server = field(init=False, repr=False)

    @override
    def __post_init__(self, host: str, port: int, /) -> None:
        super().__post_init__()  # skipif-ci
        self._server = Server(Config(self._app, host=host, port=port))  # skipif-ci

    @classmethod
    async def ping(
        cls, port: int, /, *, host: str = _LOCALHOST, timeout: Duration = _TIMEOUT
    ) -> bool:
        """Ping the receiver."""
        from httpx import AsyncClient, ConnectError  # skipif-ci

        url = f"http://{host}:{port}/ping"  # skipif-ci
        timeout_use = datetime_duration_to_float(timeout)  # skipif-ci
        try:  # skipif-ci
            async with AsyncClient() as client:
                response = await client.get(url, timeout=timeout_use)
        except ConnectError:  # skipif-ci
            return False
        else:  # skipif-ci
            return response.status_code == 200

    @override
    async def _start_core(self) -> None:
        await self._server.serve()  # skipif-ci

    @override
    async def _stop_core(self) -> None:
        await self._server.shutdown()  # skipif-ci


__all__ = ["PingReceiver"]
