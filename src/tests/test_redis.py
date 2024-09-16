from __future__ import annotations

import redis
import redis.asyncio
from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    floats,
    sampled_from,
    tuples,
)
from polars import Boolean, DataFrame, DataType, Float64, Int64, Utf8
from polars.testing import assert_frame_equal
from pytest import raises
from redis.commands.timeseries import TimeSeries

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from utilities.datetime import EPOCH_UTC, drop_microseconds
from utilities.hypothesis import (
    int32s,
    lists_fixed_length,
    redis_cms,
    text_ascii,
    zoned_datetimes,
)
from utilities.polars import DatetimeUTC, check_polars_dataframe, zoned_datetime
from utilities.redis import (
    _TimeSeriesAddDataFrameKeyIsNotUtf8Error,
    _TimeSeriesAddDataFrameKeyMissingError,
    _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError,
    _TimeSeriesAddDataFrameTimestampMissingError,
    _TimeSeriesAddErrorAtUpsertError,
    _TimeSeriesAddInvalidTimestampError,
    _TimeSeriesAddInvalidValueError,
    _TimeSeriesMAddInvalidKeyError,
    _TimeSeriesMAddInvalidTimestampError,
    _TimeSeriesMAddInvalidValueError,
    _TimeSeriesMAddKeyIsNotUtf8Error,
    _TimeSeriesMAddKeyMissingError,
    _TimeSeriesMAddTimestampIsNotAZonedDatetimeError,
    _TimeSeriesMAddTimestampMissingError,
    _TimeSeriesMAddValueIsNotNumericError,
    _TimeSeriesMAddValueMissingError,
    _TimeSeriesRangeInvalidKeyError,
    _TimeSeriesRangeKeyWithInt64AndFloat64Error,
    _TimeSeriesRangeNoKeysRequestedError,
    _TimeSeriesReadDataFrameNoColumnsRequestedError,
    _TimeSeriesReadDataFrameNoKeysRequestedError,
    ensure_time_series_created,
    ensure_time_series_created_async,
    time_series_add,
    time_series_add_async,
    time_series_add_dataframe,
    time_series_add_dataframe_async,
    time_series_get,
    time_series_get_async,
    time_series_madd,
    time_series_madd_async,
    time_series_range,
    time_series_range_async,
    time_series_read_dataframe,
    time_series_read_dataframe_async,
    yield_client,
    yield_client_async,
    yield_time_series,
    yield_time_series_async,
)
from utilities.zoneinfo import UTC, HongKong

if TYPE_CHECKING:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from polars._typing import SchemaDict

    from utilities.types import Number


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


@SKIPIF_CI_AND_NOT_LINUX
class TestEnsureTimeSeriesCreated:
    @given(data=data())
    async def test_main(self, *, data: DataObject) -> None:
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis() as client:
                    assert client.exists(container.key) == 0
                    for _ in range(2):
                        ensure_time_series_created(client.ts(), container.key)
                    assert client.exists(container.key) == 1
                case redis.asyncio.Redis() as client:
                    assert await client.exists(container.key) == 0
                    for _ in range(2):
                        await ensure_time_series_created_async(
                            client.ts(), container.key
                        )
                    assert await client.exists(container.key) == 1


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesAddAndGet: ...


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesAddAndGet:
    @given(
        data=data(),
        timestamp=valid_zoned_datetimes,
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_main(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    result = time_series_add(
                        container.ts, container.key, timestamp, value
                    )
                case redis.asyncio.Redis():
                    result = await time_series_add_async(
                        container.ts, container.key, timestamp, value
                    )
            assert isinstance(result, int)
            match container.client:
                case redis.Redis():
                    res_timestamp, res_value = time_series_get(
                        container.ts, container.key
                    )
                case redis.asyncio.Redis():
                    res_timestamp, res_value = await time_series_get_async(
                        container.ts, container.key
                    )
            assert res_timestamp == timestamp.astimezone(UTC)
            assert res_value == value

    @given(
        data=data(),
        timestamp=valid_zoned_datetimes,
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_error_at_upsert(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        match = "Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got .*"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    _ = time_series_add(container.ts, container.key, timestamp, value)
                    with raises(_TimeSeriesAddErrorAtUpsertError, match=match):
                        _ = time_series_add(
                            container.ts, container.key, timestamp, value
                        )
                case redis.asyncio.Redis():
                    _ = await time_series_add_async(
                        container.ts, container.key, timestamp, value
                    )
                    with raises(_TimeSeriesAddErrorAtUpsertError, match=match):
                        _ = await time_series_add_async(
                            container.ts, container.key, timestamp, value
                        )

    @given(
        data=data(),
        timestamp=invalid_zoned_datetimes,
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_error_invalid_timestamp(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        match = "Timestamp must be at least the Epoch; got .*"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(_TimeSeriesAddInvalidTimestampError, match=match):
                        _ = time_series_add(
                            container.ts, container.key, timestamp, value
                        )
                case redis.asyncio.Redis():
                    with raises(_TimeSeriesAddInvalidTimestampError, match=match):
                        _ = await time_series_add_async(
                            container.ts, container.key, timestamp, value
                        )

    @given(
        data=data(),
        timestamp=valid_zoned_datetimes,
        value=sampled_from([inf, -inf, nan]),
    )
    async def test_error_invalid_value(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        match = "Invalid value; got .*"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(_TimeSeriesAddInvalidValueError, match=match):
                        _ = time_series_add(
                            container.ts, container.key, timestamp, value
                        )
                case redis.asyncio.Redis():
                    with raises(_TimeSeriesAddInvalidValueError, match=match):
                        _ = await time_series_add_async(
                            container.ts, container.key, timestamp, value
                        )


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesAddAndReadDataFrame:
    @given(
        data=data(),
        key_timestamp_values=lists_fixed_length(text_ascii(), 4, unique=True).map(
            tuple
        ),
        strategy_dtype1=sampled_from([
            (int32s(), Int64),
            (floats(allow_nan=False, allow_infinity=False), Float64),
        ]),
        strategy_dtype2=sampled_from([
            (int32s(), Int64),
            (floats(allow_nan=False, allow_infinity=False), Float64),
        ]),
        time_zone=sampled_from([HongKong, UTC]),
        series_names=lists_fixed_length(text_ascii(), 2, unique=True).map(tuple),
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        strategy_dtype1: tuple[SearchStrategy[Number], DataType],
        strategy_dtype2: tuple[SearchStrategy[Number], DataType],
        key_timestamp_values: tuple[str, str, str, str],
        series_names: tuple[str, str],
        time_zone: ZoneInfo,
    ) -> None:
        timestamp1, timestamp2 = data.draw(
            tuples(valid_zoned_datetimes, valid_zoned_datetimes)
        )
        strategy1, dtype1 = strategy_dtype1
        strategy2, dtype2 = strategy_dtype2
        value11, value21 = data.draw(tuples(strategy1, strategy1))
        value12, value22 = data.draw(tuples(strategy2, strategy2))
        key, timestamp, column1, column2 = key_timestamp_values
        columns = column1, column2
        schema = {
            key: Utf8,
            timestamp: zoned_datetime(time_zone=time_zone),
            column1: dtype1,
            column2: dtype2,
        }
        async with redis_cms(data) as container:
            key1, key2 = keys = cast(
                tuple[str, str], tuple(f"{container.key}_{id_}" for id_ in series_names)
            )
            df = DataFrame(
                [
                    (key1, timestamp1, value11, value12),
                    (key2, timestamp2, value21, value22),
                ],
                schema=schema,
                orient="row",
            )
            match container.client:
                case redis.Redis():
                    time_series_add_dataframe(
                        container.ts, df, key=key, timestamp=timestamp
                    )
                    result = time_series_read_dataframe(
                        container.ts,
                        keys,
                        columns,
                        output_key=key,
                        output_timestamp=timestamp,
                        output_time_zone=time_zone,
                    )
                case redis.asyncio.Redis():
                    await time_series_add_dataframe_async(
                        container.ts, df, key=key, timestamp=timestamp
                    )
                    result = await time_series_read_dataframe_async(
                        container.ts,
                        keys,
                        columns,
                        output_key=key,
                        output_timestamp=timestamp,
                        output_time_zone=time_zone,
                    )
            check_polars_dataframe(result, height=2, schema_list=schema)
        assert_frame_equal(result, df)

    @given(data=data())
    async def test_error_add_key_missing(self, *, data: DataObject) -> None:
        df = DataFrame()
        match = "DataFrame must have a 'key' column; got .*"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(_TimeSeriesAddDataFrameKeyMissingError, match=match):
                        _ = time_series_add_dataframe(container.ts, df)
                case redis.asyncio.Redis():
                    with raises(_TimeSeriesAddDataFrameKeyMissingError, match=match):
                        _ = await time_series_add_dataframe_async(container.ts, df)

    @given(data=data())
    async def test_error_add_timestamp_missing(self, *, data: DataObject) -> None:
        df = DataFrame(schema={"key": Utf8})
        match = "DataFrame must have a 'timestamp' column; got .*"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(
                        _TimeSeriesAddDataFrameTimestampMissingError, match=match
                    ):
                        _ = time_series_add_dataframe(container.ts, df)
                case redis.asyncio.Redis():
                    with raises(
                        _TimeSeriesAddDataFrameTimestampMissingError, match=match
                    ):
                        _ = await time_series_add_dataframe_async(container.ts, df)

    @given(data=data())
    async def test_error_add_key_is_not_utf8(self, *, data: DataObject) -> None:
        df = DataFrame(schema={"key": Boolean, "timestamp": DatetimeUTC})
        match = "The 'key' column must be Utf8; got Boolean"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(_TimeSeriesAddDataFrameKeyIsNotUtf8Error, match=match):
                        _ = time_series_add_dataframe(container.ts, df)
                case redis.asyncio.Redis():
                    with raises(_TimeSeriesAddDataFrameKeyIsNotUtf8Error, match=match):
                        _ = await time_series_add_dataframe_async(container.ts, df)

    @given(data=data())
    async def test_error_add_timestamp_is_not_a_zoned_datetime(
        self, *, data: DataObject
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean})
        match = "The 'timestamp' column must be a zoned Datetime; got Boolean"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(
                        _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError,
                        match=match,
                    ):
                        _ = time_series_add_dataframe(container.ts, df)
                case redis.asyncio.Redis():
                    with raises(
                        _TimeSeriesAddDataFrameTimestampIsNotAZonedDatetimeError,
                        match=match,
                    ):
                        _ = await time_series_add_dataframe_async(container.ts, df)

    @given(data=data())
    async def test_error_read_no_keys_requested(self, *, data: DataObject) -> None:
        match = "At least 1 key must be requested"
        async with redis_cms(data) as container:
            match container.client:
                case redis.Redis():
                    with raises(
                        _TimeSeriesReadDataFrameNoKeysRequestedError, match=match
                    ):
                        _ = time_series_read_dataframe(container.ts, [], [])
                case redis.asyncio.Redis():
                    with raises(
                        _TimeSeriesReadDataFrameNoKeysRequestedError, match=match
                    ):
                        _ = await time_series_read_dataframe_async(container.ts, [], [])

from utilities.redis import yield_client, yield_client_async


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
