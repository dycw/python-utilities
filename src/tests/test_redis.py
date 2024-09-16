from __future__ import annotations

import redis
import redis.asyncio
from hypothesis.strategies import sampled_from

from utilities.datetime import EPOCH_UTC, drop_microseconds
from utilities.hypothesis import zoned_datetimes
from utilities.redis import yield_client, yield_client_async
from utilities.zoneinfo import UTC, HongKong

valid_zoned_datetimes = zoned_datetimes(
    min_value=EPOCH_UTC, time_zone=sampled_from([HongKong, UTC]), valid=True
).map(drop_microseconds)
invalid_zoned_datetimes = (
    zoned_datetimes(
        max_value=EPOCH_UTC, time_zone=sampled_from([HongKong, UTC]), valid=True
    )
    .map(drop_microseconds)
    .filter(lambda t: t < EPOCH_UTC)
)


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
