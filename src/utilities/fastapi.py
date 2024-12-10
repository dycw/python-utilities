from __future__ import annotations

from asyncio import Task, create_task
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

from fastapi import FastAPI
from typing_extensions import override
from uvicorn import Config, Server

if TYPE_CHECKING:
    from types import TracebackType


class _PingerReceiverApp(FastAPI):
    """App for the ping pinger."""

    @override
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

        @self.get("/ping")
        def ping() -> str:
            return "pong"

        _ = ping


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
        self._config = Config(self._app, host=self.host, port=self.port)
        self._server = Server(self._config)

    async def __aenter__(self) -> Self:
        """Start the server."""
        self._task = create_task(self._server.serve())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Stop the server."""
        _ = (exc_type, exc_value, traceback)
        self._server.should_exit = True


__all__ = ["PingReceiver"]
