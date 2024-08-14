from __future__ import annotations

from typing import TYPE_CHECKING

import redis
import redis.asyncio
from hypothesis import Phase, given, settings
from hypothesis.strategies import datetimes, floats, integers, sampled_from
from polars import Boolean, DataFrame, Float64, Utf8
from polars.testing import assert_frame_equal
from pytest import raises
from redis.exceptions import ResponseError

from utilities.datetime import MillisecondsSinceEpochError, milliseconds_since_epoch
from utilities.hypothesis import assume_does_not_raise, redis_clients, text_ascii
from utilities.polars import DatetimeUTC, check_polars_dataframe
from utilities.redis import (
    TimeSeriesMAddError,
    time_series_add,
    time_series_get,
    time_series_madd,
    time_series_range,
    yield_client,
    yield_client_async,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID
    from zoneinfo import ZoneInfo


class TestTimeSeriesAddAndGet:
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
        res_timestamp, res_value = time_series_get(ts, full_key)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value


class TestTimeSeriesMAddAndRange:
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
                schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Float64},
                orient="row",
            )
        time_series_madd(ts, df)
        result = time_series_range(ts, full_keys)
        check_polars_dataframe(
            result,
            height=2,
            schema_list={"key": Utf8, "timestamp": DatetimeUTC, "value": Float64},
        )
        assert_frame_equal(result, df)

    @given(client_pair=redis_clients())
    @settings(phases={Phase.generate})
    def test_error_key(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        ts = client.ts()
        df = DataFrame(
            schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
        )
        with raises(
            TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
        ):
            _ = time_series_madd(ts, df)

    @given(client_pair=redis_clients())
    @settings(phases={Phase.generate})
    def test_error_timestamp(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        ts = client.ts()
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        with raises(
            TimeSeriesMAddError,
            match="The 'timestamp' column must be Datetime; got Boolean",
        ):
            _ = time_series_madd(ts, df)

    @given(client_pair=redis_clients())
    @settings(phases={Phase.generate})
    def test_error_value(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        ts = client.ts()
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        with raises(
            TimeSeriesMAddError, match="The 'value' column must be numeric; got Boolean"
        ):
            _ = time_series_madd(ts, df)


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
