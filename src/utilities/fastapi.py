from __future__ import annotations

from asyncio import Task, create_task
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, override

from fastapi import FastAPI
from uvicorn import Config, Server

from utilities.asyncio import timeout_td
from utilities.whenever import get_now_local

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from utilities.types import Delta


_TASKS: list[Task[None]] = []


class _PingerReceiverApp(FastAPI):
    """App for the ping pinger."""

    @override
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # skipif-ci

        @self.get("/ping")  # skipif-ci
        def ping() -> str:
            return f"pong @ {get_now_local()}"  # skipif-ci

        _ = ping  # skipif-ci


@asynccontextmanager
async def yield_ping_receiver(
    port: int, /, *, host: str = "localhost", timeout: Delta | None = None
) -> AsyncIterator[None]:
    """Yield the ping receiver."""
    app = _PingerReceiverApp()  # skipif-ci
    server = Server(Config(app, host=host, port=port))  # skipif-ci
    task = create_task(server.serve())  # skipif-ci
    _TASKS.append(task)
    try:  # skipif-ci
        async with timeout_td(timeout):
            yield
    finally:  # skipif-ci
        await server.shutdown()


__all__ = ["yield_ping_receiver"]
