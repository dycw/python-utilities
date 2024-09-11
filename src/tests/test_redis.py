from __future__ import annotations

from math import inf, nan
from typing import TYPE_CHECKING, ClassVar, Literal, cast

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
from polars import Boolean, DataFrame, Float64, Int64, Utf8
from polars.testing import assert_frame_equal
from pytest import mark, param, raises

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
    TimeSeriesAddDataFrameError,
    TimeSeriesMAddError,
    TimeSeriesRangeError,
    TimeSeriesReadDataFrameError,
    _TimeSeriesAddDataFrameKeyIsNotUtf8Error,
    _TimeSeriesAddDataFrameKeyMissingError,
    _TimeSeriesAddDataFrameTimestampMissingError,
    _TimeSeriesAddErrorAtUpsertError,
    _TimeSeriesAddInvalidTimestampError,
    _TimeSeriesAddInvalidValueError,
    _TimeSeriesMAddKeyMissingError,
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
    from dataclasses import dataclass
    from zoneinfo import ZoneInfo

    from polars import DataType
    from polars._typing import PolarsDataType, SchemaDict
    from redis.commands.timeseries import TimeSeries

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

    @given(data=data(), timestamp=valid_zoned_datetimes)
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
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
    schema: ClassVar[SchemaDict] = {
        "key": Utf8,
        "timestamp": DatetimeUTC,
        "value": Float64,
    }

    @given(
        data=data(),
        series_names=lists_fixed_length(text_ascii(), 2, unique=True).map(tuple),
        key_timestamp_values=lists_fixed_length(text_ascii(), 4, unique=True).map(
            tuple
        ),
        time_zone=sampled_from([HongKong, UTC]),
    )
    @mark.parametrize(
        ("strategy1", "dtype1"),
        [
            param(int32s(), Int64),
            param(floats(allow_nan=False, allow_infinity=False), Float64),
        ],
    )
    @mark.parametrize(
        ("strategy2", "dtype2"),
        [
            param(int32s(), Int64),
            param(floats(allow_nan=False, allow_infinity=False), Float64),
        ],
    )
    async def test_main(
        self,
        *,
        data: DataObject,
        series_names: tuple[str, str],
        strategy1: SearchStrategy[Number],
        strategy2: SearchStrategy[Number],
        key_timestamp_values: tuple[str, str, str, str],
        time_zone: ZoneInfo,
        dtype1: DataType,
        dtype2: DataType,
    ) -> None:
        timestamp1, timestamp2 = data.draw(
            tuples(valid_zoned_datetimes, valid_zoned_datetimes)
        )
        value11, value21 = data.draw(tuples(strategy1, strategy1))
        value12, value22 = data.draw(tuples(strategy2, strategy2))
        key, timestamp, column1, column2 = key_timestamp_values
        columns = (column1, column2)
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
                        container.ts,
                        df,
                        key=key,
                        timestamp=timestamp,
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


if 0:

    @SKIPIF_CI_AND_NOT_LINUX
    class TestTimeSeriesAddAndReadDataFrame:
        @given(yield_redis=redis_cms())
        def test_sync_error_add_timestamp_missing(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Utf8})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesAddDataFrameError,
                    match="DataFrame must have a 'timestamp' column; got .*",
                ),
            ):
                _ = time_series_add_dataframe(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_add_key_is_not_utf8(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Boolean, "timestamp": DatetimeUTC})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesAddDataFrameError,
                    match="The 'key' column must be Utf8; got Boolean",
                ),
            ):
                _ = time_series_add_dataframe(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_timestamp_is_not_a_zoned_datetime(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": Boolean})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesAddDataFrameError,
                    match="The 'timestamp' column must be a zoned Datetime; got Boolean",
                ),
            ):
                _ = time_series_add_dataframe(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_read_no_keys_requested(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesReadDataFrameError,
                    match="At least 1 key must be requested",
                ),
            ):
                _ = time_series_read_dataframe(redis.ts, [], [])

        @given(yield_redis=redis_cms())
        def test_sync_error_read_no_columns_requested(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesReadDataFrameError,
                    match="At least 1 column must be requested",
                ),
            ):
                _ = time_series_read_dataframe(redis.ts, container.key, [])

        @given(data=data())
        async def test_async_error_madd_timestamp_is_not_a_zoned_datetime(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": Boolean})
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesAddDataFrameError,
                    match="The 'timestamp' column must be a zoned Datetime; got Boolean",
                ):
                    _ = await time_series_add_dataframe_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_read_no_keys_requested(
            self, *, data: DataObject
        ) -> None:
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesReadDataFrameError,
                    match="At least 1 key must be requested",
                ):
                    _ = await time_series_read_dataframe_async(redis.ts, [], [])

        @given(data=data())
        async def test_async_error_read_no_columns_requested(
            self, *, data: DataObject
        ) -> None:
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesReadDataFrameError,
                    match="At least 1 column must be requested",
                ):
                    _ = await time_series_read_dataframe_async(
                        redis.ts, container.key, []
                    )

    @dataclass(frozen=True, kw_only=True)
    class _TestTimeSeriesMAddAndRangePrepare:
        keys: tuple[str, str]
        triples: list[tuple[str, dt.datetime, Number]]
        key: str
        timestamp: str
        value: str
        values_or_df: list[tuple[str, dt.datetime, Number]] | DataFrame
        schema: SchemaDict

    @SKIPIF_CI_AND_NOT_LINUX
    class TestTimeSeriesMAddAndRange:
        int_schema: ClassVar[SchemaDict] = {
            "key": Utf8,
            "timestamp": DatetimeUTC,
            "value": Int64,
        }
        float_schema: ClassVar[SchemaDict] = {
            "key": Utf8,
            "timestamp": DatetimeUTC,
            "value": Float64,
        }

        @given(
            data=data(),
            yield_redis=redis_cms(),
            series_names=lists_fixed_length(text_ascii(), 2, unique=True).map(tuple),
            time_zone=sampled_from([HongKong, UTC]),
            key_timestamp_value=lists_fixed_length(text_ascii(), 3, unique=True).map(
                tuple
            ),
        )
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        @mark.parametrize(
            ("strategy", "dtype"),
            [
                param(int32s(), Int64),
                param(floats(allow_nan=False, allow_infinity=False), Float64),
            ],
        )
        def test_sync(
            self,
            *,
            data: DataObject,
            yield_redis: YieldRedisContainer,
            series_names: tuple[str, str],
            time_zone: ZoneInfo,
            key_timestamp_value: tuple[str, str, str],
            case: Literal["values", "DataFrame"],
            strategy: SearchStrategy[Number],
            dtype: PolarsDataType,
        ) -> None:
            with yield_redis() as container:
                prepared = self._prepare_main_test(
                    data,
                    container.key,
                    series_names,
                    time_zone,
                    key_timestamp_value,
                    case,
                    strategy,
                    dtype,
                )
                res_madd = time_series_madd(
                    redis.ts,
                    prepared.values_or_df,
                    key=prepared.key,
                    timestamp=prepared.timestamp,
                    value=prepared.value,
                )
                assert isinstance(res_madd, list)
                for i in res_madd:
                    assert isinstance(i, int)
                res_range = time_series_range(
                    redis.ts,
                    prepared.keys,
                    output_key=prepared.key,
                    output_timestamp=prepared.timestamp,
                    output_time_zone=time_zone,
                    output_value=prepared.value,
                )
                check_polars_dataframe(res_range, height=2, schema_list=prepared.schema)
                assert res_range.rows() == prepared.triples

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_key_missing(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame()
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'key' column; got .*",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_timestamp_missing(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Utf8})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'timestamp' column; got .*",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_value_missing(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'value' column; got .*",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_key_is_not_utf8(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(
                schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
            )
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="The 'key' column must be Utf8; got Boolean",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_timestamp_is_not_a_zoned_datetime(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="The 'timestamp' column must be a zoned Datetime; got Boolean",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(yield_redis=redis_cms())
        def test_sync_error_madd_value_is_not_numeric(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            df = DataFrame(
                schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean}
            )
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesMAddError,
                    match="The 'value' column must be numeric; got Boolean",
                ),
            ):
                _ = time_series_madd(redis.ts, df)

        @given(data=data(), yield_redis=redis_cms())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        def test_sync_error_madd_invalid_key(
            self,
            *,
            data: DataObject,
            yield_redis: YieldRedisContainer,
            case: Literal["values", "DataFrame"],
        ) -> None:
            with yield_redis() as container:
                values_or_df = self._prepare_test_error_madd_invalid_key(
                    data, container.key, case
                )
                with raises(TimeSeriesMAddError, match="The key '.*' must exist"):
                    _ = time_series_madd(
                        redis.ts, values_or_df, assume_time_series_exist=True
                    )

        @given(data=data(), yield_redis=redis_cms())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        def test_sync_error_madd_invalid_timestamp(
            self,
            *,
            data: DataObject,
            yield_redis: YieldRedisContainer,
            case: Literal["values", "DataFrame"],
        ) -> None:
            with yield_redis() as container:
                values_or_df = self._prepare_test_error_madd_invalid_timestamp(
                    data, container.key, case
                )
                with raises(
                    TimeSeriesMAddError,
                    match="Timestamps must be at least the Epoch; got .*",
                ):
                    _ = time_series_madd(redis.ts, values_or_df)

        @given(data=data(), yield_redis=redis_cms())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
        def test_sync_error_madd_invalid_value(
            self,
            *,
            data: DataObject,
            yield_redis: YieldRedisContainer,
            case: Literal["values", "DataFrame"],
            value: float,
        ) -> None:
            with yield_redis() as container:
                values_or_df = self._prepare_test_error_madd_invalid_value(
                    data, container.key, case, value
                )
                with raises(TimeSeriesMAddError, match="The value .* is invalid"):
                    _ = time_series_madd(redis.ts, values_or_df)

        @given(yield_redis=redis_cms())
        def test_sync_error_range_no_keys_requested(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            with (
                yield_redis() as container,
                raises(
                    TimeSeriesRangeError,
                    match="At least 1 key must be requested; got .*",
                ),
            ):
                _ = time_series_range(redis.ts, [])

        @given(yield_redis=redis_cms())
        def test_sync_error_range_invalid_key(
            self, *, yield_redis: YieldRedisContainer
        ) -> None:
            with (
                yield_redis() as container,
                raises(TimeSeriesRangeError, match="The key '.*' must exist"),
            ):
                _ = time_series_range(redis.ts, container.key)

        @given(data=data(), yield_redis=redis_cms())
        def test_sync_error_range_key_with_int64_and_float64(
            self, *, data: DataObject, yield_redis: YieldRedisContainer
        ) -> None:
            with yield_redis() as container:
                values = self._prepare_test_error_range_key_with_int64_and_float64(
                    data, container.key
                )
                for vals in values:
                    _ = time_series_madd(redis.ts, vals)
                with raises(
                    TimeSeriesRangeError,
                    match="The key '.*' contains both Int64 and Float64 data",
                ):
                    _ = time_series_range(redis.ts, container.key)

        @given(
            data=data(),
            series_names=lists_fixed_length(text_ascii(), 2, unique=True).map(tuple),
            time_zone=sampled_from([HongKong, UTC]),
            key_timestamp_value=lists_fixed_length(text_ascii(), 3, unique=True).map(
                tuple
            ),
        )
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        @mark.parametrize(
            ("strategy", "dtype"),
            [
                param(int32s(), Int64),
                param(floats(allow_nan=False, allow_infinity=False), Float64),
            ],
        )
        async def test_async(
            self,
            *,
            data: DataObject,
            series_names: tuple[str, str],
            time_zone: ZoneInfo,
            key_timestamp_value: tuple[str, str, str],
            case: Literal["values", "DataFrame"],
            strategy: SearchStrategy[Number],
            dtype: PolarsDataType,
        ) -> None:
            async with redis_cms_async(data) as container:
                prepared = self._prepare_main_test(
                    data,
                    container.key,
                    series_names,
                    time_zone,
                    key_timestamp_value,
                    case,
                    strategy,
                    dtype,
                )
                res_madd = await time_series_madd_async(
                    redis.ts,
                    prepared.values_or_df,
                    key=prepared.key,
                    timestamp=prepared.timestamp,
                    value=prepared.value,
                )
                assert isinstance(res_madd, list)
                for i in res_madd:
                    assert isinstance(i, int)
                res_range = await time_series_range_async(
                    redis.ts,
                    prepared.keys,
                    output_key=prepared.key,
                    output_timestamp=prepared.timestamp,
                    output_time_zone=time_zone,
                    output_value=prepared.value,
                )
                check_polars_dataframe(res_range, height=2, schema_list=prepared.schema)
                assert res_range.rows() == prepared.triples

        @given(data=data())
        async def test_async_error_madd_key_missing(self, *, data: DataObject) -> None:
            df = DataFrame()
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'key' column; got .*",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_madd_timestamp_missing(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(schema={"key": Utf8})
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'timestamp' column; got .*",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_madd_value_missing(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC})
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="DataFrame must have a 'value' column; got .*",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_madd_key_is_not_utf8(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(
                schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
            )
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="The 'key' column must be Utf8; got Boolean",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_madd_timestamp_is_not_a_zoned_datetime(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="The 'timestamp' column must be a zoned Datetime; got Boolean",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        async def test_async_error_madd_value_is_not_numeric(
            self, *, data: DataObject
        ) -> None:
            df = DataFrame(
                schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean}
            )
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesMAddError,
                    match="The 'value' column must be numeric; got Boolean",
                ):
                    _ = await time_series_madd_async(redis.ts, df)

        @given(data=data())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        async def test_async_error_madd_invalid_key(
            self, *, data: DataObject, case: Literal["values", "DataFrame"]
        ) -> None:
            async with redis_cms_async(data) as container:
                values_or_df = self._prepare_test_error_madd_invalid_key(
                    data, container.key, case
                )
                with raises(TimeSeriesMAddError, match="The key '.*' must exist"):
                    _ = await time_series_madd_async(
                        redis.ts, values_or_df, assume_time_series_exist=True
                    )

        @given(data=data())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        async def test_async_error_madd_invalid_timestamp(
            self, *, data: DataObject, case: Literal["values", "DataFrame"]
        ) -> None:
            async with redis_cms_async(data) as container:
                values_or_df = self._prepare_test_error_madd_invalid_timestamp(
                    data, container.key, case
                )
                with raises(
                    TimeSeriesMAddError,
                    match="Timestamps must be at least the Epoch; got .*",
                ):
                    _ = await time_series_madd_async(redis.ts, values_or_df)

        @given(data=data())
        @mark.parametrize("case", [param("values"), param("DataFrame")])
        @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
        async def test_async_error_madd_invalid_value(
            self,
            *,
            data: DataObject,
            case: Literal["values", "DataFrame"],
            value: float,
        ) -> None:
            async with redis_cms_async(data) as container:
                values_or_df = self._prepare_test_error_madd_invalid_value(
                    data, container.key, case, value
                )
                with raises(TimeSeriesMAddError, match="The value .* is invalid"):
                    _ = await time_series_madd_async(redis.ts, values_or_df)

        @given(data=data())
        async def test_async_error_range_no_keys_requested(
            self, *, data: DataObject
        ) -> None:
            async with redis_cms_async(data) as container:
                with raises(
                    TimeSeriesRangeError,
                    match="At least 1 key must be requested; got .*",
                ):
                    _ = await time_series_range_async(redis.ts, [])

        @given(data=data())
        async def test_async_error_range_invalid_key(self, *, data: DataObject) -> None:
            async with redis_cms_async(data) as container:
                with raises(TimeSeriesRangeError, match="The key '.*' must exist"):
                    _ = await time_series_range_async(redis.ts, container.key)

        @given(data=data())
        async def test_async_error_range_key_with_int64_and_float64(
            self, *, data: DataObject
        ) -> None:
            async with redis_cms_async(data) as container:
                values = self._prepare_test_error_range_key_with_int64_and_float64(
                    data, container.key
                )
                for vals in values:
                    _ = await time_series_madd_async(redis.ts, vals)
                with raises(
                    TimeSeriesRangeError,
                    match="The key '.*' contains both Int64 and Float64 data",
                ):
                    _ = await time_series_range_async(redis.ts, container.key)

        def _prepare_main_test(
            self,
            data: DataObject,
            container_key: str,
            series_names: tuple[str, str],
            time_zone: ZoneInfo,
            key_timestamp_value: tuple[str, str, str],
            case: Literal["values", "DataFrame"],
            strategy: SearchStrategy[Number],
            dtype: PolarsDataType,
            /,
        ) -> _TestTimeSeriesMAddAndRangePrepare:
            keys = cast(
                tuple[str, str],
                tuple(f"{container.key}_{case}_{name}" for name in series_names),
            )
            timestamps = data.draw(tuples(valid_zoned_datetimes, valid_zoned_datetimes))
            values = data.draw(tuples(strategy, strategy))
            triples = list(zip(keys, timestamps, values, strict=True))
            key, timestamp, value = key_timestamp_value
            schema = {
                key: Utf8,
                timestamp: zoned_datetime(time_zone=time_zone),
                value: dtype,
            }
            match case:
                case "values":
                    values_or_df = triples
                case "DataFrame":
                    values_or_df = DataFrame(triples, schema=schema, orient="row")
            return _TestTimeSeriesMAddAndRangePrepare(
                keys=keys,
                triples=triples,
                key=key,
                timestamp=timestamp,
                value=value,
                values_or_df=values_or_df,
                schema=schema,
            )

        def _prepare_test_error_madd_invalid_key(
            self, data: DataObject, key: str, case: Literal["values", "DataFrame"], /
        ) -> list[tuple[str, dt.datetime, int]] | DataFrame:
            timestamp = data.draw(valid_zoned_datetimes)
            value = data.draw(int32s())
            values = [(f"{key}_{case}", timestamp, value)]
            match case:
                case "values":
                    return values
                case "DataFrame":
                    return DataFrame(values, schema=self.int_schema, orient="row")

        def _prepare_test_error_madd_invalid_timestamp(
            self, data: DataObject, key: str, case: Literal["values", "DataFrame"], /
        ) -> list[tuple[str, dt.datetime, int]] | DataFrame:
            timestamp = data.draw(invalid_zoned_datetimes)
            _ = assume(timestamp < EPOCH_UTC)
            value = data.draw(int32s())
            values = [(f"{key}_{case}", timestamp, value)]
            match case:
                case "values":
                    return values
                case "DataFrame":
                    return DataFrame(values, schema=self.int_schema, orient="row")

        def _prepare_test_error_madd_invalid_value(
            self,
            data: DataObject,
            key: str,
            case: Literal["values", "DataFrame"],
            value: float,
            /,
        ) -> list[tuple[str, dt.datetime, float]] | DataFrame:
            timestamp = data.draw(valid_zoned_datetimes)
            values = [(f"{key}_{case}", timestamp, value)]
            match case:
                case "values":
                    return values
                case "DataFrame":
                    return DataFrame(values, schema=self.float_schema, orient="row")

        def _prepare_test_error_range_key_with_int64_and_float64(
            self, data: DataObject, key: str, /
        ) -> tuple[
            list[tuple[str, dt.datetime, int]], list[tuple[str, dt.datetime, float]]
        ]:
            timestamp = data.draw(valid_zoned_datetimes)
            value = data.draw(int32s())
            return [(key, timestamp, value)], [(key, timestamp, float(value))]

    class TestYieldClient:
        def test_sync(self) -> None:
            with yield_client() as client:
                assert isinstance(client, redis.Redis)

        async def test_async(self) -> None:
            async with yield_client_async() as client:
                assert isinstance(client, redis.asyncio.Redis)

    class TestYieldTimeSeries:
        def test_sync(self) -> None:
            with yield_time_series() as ts:
                assert isinstance(ts, TimeSeries)

        async def test_async(self) -> None:
            async with yield_time_series_async() as ts:
                assert isinstance(ts, TimeSeries)
