from __future__ import annotations

from asyncio import Task, create_task
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Self, override

from fastapi import FastAPI
from uvicorn import Config, Server

from utilities.asyncio import Looper
from utilities.datetime import SECOND, datetime_duration_to_float

if TYPE_CHECKING:
    from types import TracebackType

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
            from utilities.tzlocal import get_now_local  # skipif-ci
            from utilities.whenever import serialize_zoned_datetime  # skipif-ci

            now = serialize_zoned_datetime(get_now_local())  # skipif-ci
            return f"pong @ {now}"  # skipif-ci

        _ = ping  # skipif-ci


@dataclass(kw_only=True)
class PingReceiver(Looper[None]):
    """A ping receiver."""

    host: InitVar[str] = _LOCALHOST
    port: InitVar[int]
    _app: _PingerReceiverApp = field(
        default_factory=_PingerReceiverApp, init=False, repr=False
    )
    _server: Server = field(init=False, repr=False)
    _server_task: Task[None] | None = field(default=None, init=False, repr=False)

    @override
    def __post_init__(self, host: str, port: int, /) -> None:
        super().__post_init__()  # skipif-ci
        self._server = Server(Config(self._app, host=host, port=port))  # skipif-ci

    @override
    async def __aenter__(self) -> Self:
        _ = await super().__aenter__()  # skipif-ci
        async with self._lock:  # skipif-ci
            self._server_task = create_task(self._server.serve())
        return self  # skipif-ci

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await super().__aexit__(exc_type, exc_value, traceback)  # skipif-ci
        await self._server.shutdown()  # skipif-ci

    @classmethod
    async def ping(
        cls, port: int, /, *, host: str = _LOCALHOST, timeout: Duration = _TIMEOUT
    ) -> str | Literal[False]:
        """Ping the receiver."""
        from httpx import AsyncClient, ConnectError  # skipif-ci

        url = f"http://{host}:{port}/ping"  # skipif-ci
        timeout_use = datetime_duration_to_float(timeout)  # skipif-ci
        try:  # skipif-ci
            async with AsyncClient() as client:
                response = await client.get(url, timeout=timeout_use)
        except ConnectError:  # skipif-ci
            return False
        return response.text if response.status_code == 200 else False  # skipif-ci


__all__ = ["PingReceiver"]
