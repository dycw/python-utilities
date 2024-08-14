from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

import redis
import redis.asyncio
from hypothesis import Phase, given, settings
from hypothesis.strategies import datetimes, integers, sampled_from
from redis.exceptions import ResponseError

from utilities.hypothesis import assume_does_not_raise, redis_clients, text_ascii
from utilities.redis import add_timestamp, yield_client, yield_client_async
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID


class TestAddTimestamp:
    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])),
        value=integers(-10, 10),
    )
    @settings(phases={Phase.generate})
    def test_sync(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: int,
    ) -> None:
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        ts = client.ts()
        with suppress(ResponseError):
            _ = ts.create(full_key, duplicate_policy="last")
        with assume_does_not_raise(
            ResponseError, match="must be a nonnegative integer"
        ):
            add_timestamp(ts, full_key, timestamp, value)
        _res_timestamp, res_value = ts.get(full_key)

        assert int(res_value) == value


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
