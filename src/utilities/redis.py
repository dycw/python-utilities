from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

import redis
import redis.asyncio

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


@contextmanager
def yield_client(
    *,
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    password: str | None = None,
) -> Iterator[redis.Redis]:
    """Yield a synchronous client."""
    client = redis.Redis(host=host, port=port, db=db, password=password)
    try:
        yield client
    finally:
        client.close()


@asynccontextmanager
async def yield_client_async(
    *,
    host: str = "localhost",
    port: int = 6379,
    db: str | int = 0,
    password: str | None = None,
) -> AsyncIterator[redis.asyncio.Redis]:
    """Yield an asynchronous client."""
    client = redis.asyncio.Redis(host=host, port=port, db=db, password=password)
    try:
        yield client
    finally:
        await client.aclose()


__all__ = ["yield_client", "yield_client_async"]
