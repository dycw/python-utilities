from __future__ import annotations

from typing import TYPE_CHECKING

import redis
import redis.asyncio
from hypothesis import Phase, given, settings
from hypothesis.strategies import datetimes, floats, integers, sampled_from
from polars import DataFrame, Datetime, Float64, Utf8
from redis.exceptions import ResponseError

from utilities.datetime import (
    MillisecondsSinceEpochError,
    milliseconds_since_epoch,
    milliseconds_since_epoch_to_datetime,
)
from utilities.hypothesis import assume_does_not_raise, longs, redis_clients, text_ascii
from utilities.redis import (
    time_series_add,
    time_series_get,
    time_series_madd,
    yield_client,
    yield_client_async,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID
    from zoneinfo import ZoneInfo


class TestTimeSeriesAdd:
    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])),
        value=integers() | floats(),
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
        with assume_does_not_raise(MillisecondsSinceEpochError):
            _ = milliseconds_since_epoch(timestamp, strict=True)
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        ts = client.ts()
        if client.exists(full_key) == 0:
            _ = ts.create(full_key, duplicate_policy="LAST")
        with assume_does_not_raise(
            ResponseError, match="must be a nonnegative integer"
        ):
            time_series_add(ts, full_key, timestamp, value)
        res_milliseconds, res_value = ts.get(full_key)
        res_timestamp = milliseconds_since_epoch_to_datetime(res_milliseconds)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value


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
        with assume_does_not_raise(MillisecondsSinceEpochError):
            _ = milliseconds_since_epoch(timestamp, strict=True)
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        ts = client.ts()
        if client.exists(full_key) == 0:
            _ = ts.create(full_key, duplicate_policy="LAST")
        with assume_does_not_raise(
            ResponseError, match="must be a nonnegative integer"
        ):
            time_series_add(ts, full_key, timestamp, value, duplicate_policy="LAST")
        res_timestamp, res_value = time_series_get(ts, full_key)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value


class TestTimeSeriesMAdd:
    @given(
        client_pair=redis_clients(),
        key1=text_ascii(),
        key2=text_ascii(),
        datetime1=datetimes(),
        datetime2=datetimes(),
        time_zone=sampled_from([HONG_KONG, UTC]),
        value1=integers() | floats(),
        value2=integers() | floats(),
    )
    @settings(phases={Phase.generate})
    def test_main(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key1: str,
        key2: str,
        datetime1: dt.datetime,
        datetime2: dt.datetime,
        time_zone: ZoneInfo,
        value1: float,
        value2: float,
    ) -> None:
        timestamps = [d.replace(tzinfo=time_zone) for d in [datetime1, datetime2]]
        for timestamp in timestamps:
            with assume_does_not_raise(MillisecondsSinceEpochError):
                _ = milliseconds_since_epoch(timestamp, strict=True)
        client, uuid = client_pair
        full_keys = [f"{uuid}_{key}" for key in [key1, key2]]
        ts = client.ts()
        for full_key in full_keys:
            if client.exists(full_key) == 0:
                _ = ts.create(full_key, duplicate_policy="LAST")
        data = list(zip(full_keys, timestamps, [value1, value2], strict=True))
        with assume_does_not_raise(OverflowError):
            df = DataFrame(
                data,
                schema={
                    "key": Utf8,
                    "timestamp": Datetime(time_zone="UTC"),
                    "value": Float64,
                },
                orient="row",
            )
        time_series_madd(ts, df)
        for full_key, timestamp, value in data:
            res_timestamp, res_value = time_series_get(ts, full_key)
            assert res_timestamp == timestamp
            assert res_value == value


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
