from __future__ import annotations

from typing import TYPE_CHECKING

import redis
import redis.asyncio
from hypothesis import Phase, assume, given, settings
from hypothesis.strategies import datetimes, floats, sampled_from
from redis.exceptions import ResponseError

from utilities.datetime import milliseconds_since_epoch_to_datetime
from utilities.hypothesis import assume_does_not_raise, longs, redis_clients, text_ascii
from utilities.redis import (
    time_series_add,
    time_series_get,
    yield_client,
    yield_client_async,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID


class TestTimeSeriesAdd:
    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])),
        value=longs(),
    )
    @settings(phases={Phase.generate})
    def test_main(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: int,
    ) -> None:
        _ = assume(timestamp.microsecond == 0)
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        ts = client.ts()
        if not client.exists(full_key):
            _ = ts.create(full_key, duplicate_policy="LAST")
        with assume_does_not_raise(
            ResponseError, match="must be a nonnegative integer"
        ):
            time_series_add(ts, full_key, timestamp, value)
        res_milliseconds, res_value = ts.get(full_key)
        res_timestamp = milliseconds_since_epoch_to_datetime(res_milliseconds)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert int(res_value) == value


class TestTimeSeriesGet:
    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])),
        value=longs() | floats(width=32),
    )
    @settings(phases={Phase.generate})
    def test_main(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp.microsecond == 0)
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        ts = client.ts()
        if not client.exists(full_key):
            _ = ts.create(full_key, duplicate_policy="LAST")
        with assume_does_not_raise(
            ResponseError, match="must be a nonnegative integer"
        ):
            time_series_add(ts, full_key, timestamp, value, duplicate_policy="LAST")
        res_timestamp, res_value = time_series_get(ts, full_key)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
