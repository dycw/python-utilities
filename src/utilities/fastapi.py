from __future__ import annotations

from asyncio import Task, create_task
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

from fastapi import FastAPI
from typing_extensions import override
from uvicorn import Config, Server

if TYPE_CHECKING:
    from types import TracebackType


_LOCALHOST = "localhost"


class _PingerReceiverApp(FastAPI):
    """App for the ping pinger."""

    @override
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # skipif-ci

        @self.get("/ping")  # skipif-ci
        def ping() -> str:
            return "pong"  # skipif-ci

        _ = ping  # skipif-ci


@dataclass(kw_only=True, slots=True)
class PingReceiver:
    """A ping receiver."""

    host: str = "localhost"
    port: int
    _app: _PingerReceiverApp = field(default_factory=_PingerReceiverApp)
    _config: Config = field(init=False)
    _server: Server = field(init=False)
    _task: Task[None] | None = None

    def __post_init__(self) -> None:
        self._config = Config(self._app, host=self.host, port=self.port)  # skipif-ci
        self._server = Server(self._config)  # skipif-ci

    @classmethod
    async def ping(cls, port: int, *, host: str = _LOCALHOST) -> bool:
        """Ping the receiver."""
        from httpx import AsyncClient, ConnectError  # skipif-ci

        url = f"http://{host}:{port}/ping"  # skipif-ci
        try:  # skipif-ci
            async with AsyncClient() as client:
                response = await client.get(url)
        except ConnectError:  # skipif-ci
            return False
        else:  # skipif-ci
            return response.status_code == 200

    async def __aenter__(self) -> Self:
        """Start the server."""
        self._task = create_task(self._server.serve())  # skipif-ci
        return self  # skipif-ci

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Stop the server."""
        _ = (exc_type, exc_value, traceback)  # skipif-ci
        self._server.should_exit = True  # skipif-ci


__all__ = ["PingReceiver"]
