from __future__ import annotations

from dataclasses import dataclass
from math import inf, nan
from typing import TYPE_CHECKING, ClassVar, Literal, TypeVar, cast

import redis
import redis.asyncio
from hypothesis import HealthCheck, assume, given, settings
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
from pytest import mark, param, raises
from redis.commands.timeseries import TimeSeries

from tests.conftest import SKIPIF_CI_AND_NOT_LINUX
from utilities.datetime import EPOCH_UTC, drop_microseconds
from utilities.hypothesis import (
    YieldRedisContainer,
    int32s,
    lists_fixed_length,
    redis_cms,
    redis_cms_async,
    text_ascii,
    zoned_datetimes,
)
from utilities.polars import DatetimeUTC, check_polars_dataframe, zoned_datetime
from utilities.redis import (
    TimeSeriesAddDataFrameError,
    TimeSeriesAddError,
    TimeSeriesMAddError,
    TimeSeriesRangeError,
    TimeSeriesReadDataFrameError,
    ensure_time_series_created,
    ensure_time_series_created_async,
    time_series_add,
    time_series_add_async,
    time_series_add_dataframe,
    time_series_get,
    time_series_get_async,
    time_series_madd,
    time_series_madd_async,
    time_series_range,
    time_series_range_async,
    time_series_read_dataframe,
    yield_client,
    yield_client_async,
    yield_time_series,
    yield_time_series_async,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from polars._typing import PolarsDataType, SchemaDict

    from utilities.types import Number


_T = TypeVar("_T")


@SKIPIF_CI_AND_NOT_LINUX
class TestEnsureTimeSeriesCreated:
    @given(yield_redis=redis_cms())
    def test_sync(self, *, yield_redis: YieldRedisContainer) -> None:
        with yield_redis() as redis:
            assert redis.client.exists(redis.key) == 0
            for _ in range(2):
                ensure_time_series_created(redis.client.ts(), redis.key)
            assert redis.client.exists(redis.key) == 1

    @given(data=data())
    async def test_async(self, *, data: DataObject) -> None:
        async with redis_cms_async(data) as redis:
            assert await redis.client.exists(redis.key) == 0
            for _ in range(2):
                await ensure_time_series_created_async(redis.client.ts(), redis.key)
            assert await redis.client.exists(redis.key) == 1


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesAddAndGet:
    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_sync(
        self, *, yield_redis: YieldRedisContainer, timestamp: dt.datetime, value: float
    ) -> None:
        with yield_redis() as redis:
            result = time_series_add(
                redis.ts, redis.key, timestamp, value, duplicate_policy="last"
            )
            assert isinstance(result, int)
            res_timestamp, res_value = time_series_get(redis.ts, redis.key)
            assert res_timestamp == timestamp.astimezone(UTC)
            assert res_value == value

    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_sync_error_at_upsert(
        self, *, yield_redis: YieldRedisContainer, timestamp: dt.datetime, value: float
    ) -> None:
        with yield_redis() as redis:
            _ = time_series_add(redis.ts, redis.key, timestamp, value)
            with raises(
                TimeSeriesAddError,
                match="Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got .*",
            ):
                _ = time_series_add(redis.ts, redis.key, timestamp, value)

    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            max_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_sync_invalid_timestamp(
        self, *, yield_redis: YieldRedisContainer, timestamp: dt.datetime, value: float
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        with (
            yield_redis() as redis,
            raises(
                TimeSeriesAddError, match="Timestamp must be at least the Epoch; got .*"
            ),
        ):
            _ = time_series_add(
                redis.ts, redis.key, timestamp, value, duplicate_policy="last"
            )

    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
    )
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_sync_invalid_value(
        self, *, yield_redis: YieldRedisContainer, timestamp: dt.datetime, value: float
    ) -> None:
        with (
            yield_redis() as redis,
            raises(TimeSeriesAddError, match="Invalid value; got .*"),
        ):
            _ = time_series_add(
                redis.ts, redis.key, timestamp, value, duplicate_policy="last"
            )

    @given(
        data=data(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_async(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        async with redis_cms_async(data) as redis:
            result = await time_series_add_async(
                redis.ts, redis.key, timestamp, value, duplicate_policy="last"
            )
            assert isinstance(result, int)
            res_timestamp, res_value = await time_series_get_async(redis.ts, redis.key)
            assert res_timestamp == timestamp.astimezone(UTC)
            assert res_value == value

    @given(
        data=data(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_async_error_at_upsert(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        async with redis_cms_async(data) as redis:
            _ = await time_series_add_async(redis.ts, redis.key, timestamp, value)
            with raises(
                TimeSeriesAddError,
                match="Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got .*",
            ):
                _ = await time_series_add_async(redis.ts, redis.key, timestamp, value)

    @given(
        data=data(),
        timestamp=zoned_datetimes(
            max_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    async def test_async_invalid_timestamp(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesAddError, match="Timestamp must be at least the Epoch; got .*"
            ):
                _ = await time_series_add_async(
                    redis.ts, redis.key, timestamp, value, duplicate_policy="last"
                )

    @given(
        data=data(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
    )
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    async def test_async_invalid_value(
        self, *, data: DataObject, timestamp: dt.datetime, value: float
    ) -> None:
        async with redis_cms_async(data) as redis:
            with raises(TimeSeriesAddError, match="Invalid value; got .*"):
                _ = await time_series_add_async(
                    redis.ts, redis.key, timestamp, value, duplicate_policy="last"
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
        yield_redis=redis_cms(),
        keys=lists_fixed_length(text_ascii(), 6, unique=True),
        time_zone=sampled_from([HONG_KONG, UTC]),
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
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_main(
        self,
        *,
        data: DataObject,
        yield_redis: YieldRedisContainer,
        keys: list[str],
        time_zone: ZoneInfo,
        strategy1: SearchStrategy[Number],
        dtype1: DataType,
        strategy2: SearchStrategy[Number],
        dtype2: DataType,
    ) -> None:
        key, id1, id2, timestamp, key_value1, key_value2 = keys
        ts, uuid = ts_pair
        full_id1, full_id2 = (f"{uuid}_{id_}" for id_ in [id1, id2])
        (datetimes(min_value=EPOCH_UTC).map(drop_microseconds),)
        (datetimes(min_value=EPOCH_UTC).map(drop_microseconds),)
        value11, value21 = data.draw(tuples(strategy1, strategy1))
        value12, value22 = data.draw(tuples(strategy2, strategy2))
        schema = {
            key: Utf8,
            timestamp: zoned_datetime(time_zone=time_zone),
            key_value1: dtype1,
            key_value2: dtype2,
        }
        df = DataFrame(
            [
                (full_id1, timestamp1, value11, value12),
                (full_id2, timestamp2, value21, value22),
            ],
            schema=schema,
            orient="row",
        )
        time_series_add_dataframe(
            ts, df, key=key, timestamp=timestamp, duplicate_policy="last"
        )
        result = time_series_read_dataframe(
            ts,
            [full_id1, full_id2],
            [key_value1, key_value2],
            output_key=key,
            output_timestamp=timestamp,
            output_time_zone=time_zone,
        )
        check_polars_dataframe(result, height=2, schema_list=schema)
        assert_frame_equal(result, df)

    @given(yield_redis=redis_cms())
    def test_error_add_key_missing(self, *, yield_redis: YieldRedisContainer) -> None:
        df = DataFrame()
        with raises(
            TimeSeriesAddDataFrameError,
            match="DataFrame must have a 'key' column; got .*",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(yield_redis=redis_cms())
    def test_error_add_timestamp_missing(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        df = DataFrame(schema={"key": Utf8})
        with raises(
            TimeSeriesAddDataFrameError,
            match="DataFrame must have a 'timestamp' column; got .*",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(yield_redis=redis_cms())
    def test_error_add_key_is_not_utf8(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        df = DataFrame(schema={"key": Boolean, "timestamp": DatetimeUTC})
        with raises(
            TimeSeriesAddDataFrameError,
            match="The 'key' column must be Utf8; got Boolean",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(yield_redis=redis_cms())
    def test_error_madd_timestamp_is_not_a_zoned_datetime(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean})
        with raises(
            TimeSeriesAddDataFrameError,
            match="The 'timestamp' column must be a zoned Datetime; got Boolean",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(yield_redis=redis_cms())
    def test_error_read_no_keys_requested(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        with raises(
            TimeSeriesReadDataFrameError, match="At least 1 key must be requested"
        ):
            _ = time_series_read_dataframe(ts_pair[0], [], [])

    @given(yield_redis=redis_cms())
    def test_error_read_no_columns_requested(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        ts, uuid = ts_pair
        with raises(
            TimeSeriesReadDataFrameError, match="At least 1 column must be requested"
        ):
            _ = time_series_read_dataframe(ts, f"{uuid}_{key}", [])


@dataclass(frozen=True, kw_only=True)
class _TestTimeSeriesMAddAndRangePrepare:
    keys: tuple[str, str]
    triples: list[tuple[str, dt.datetime, Number]]
    key: str
    timestamp: str
    value: str
    values_or_df: list[tuple[str, dt.datetime, Number]] | DataFrame
    schema: SchemaDict


@mark.only
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
        time_zone=sampled_from([HONG_KONG, UTC]),
        key_timestamp_value=lists_fixed_length(text_ascii(), 3, unique=True).map(tuple),
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
        with yield_redis() as redis:
            prepared = self._prepare_main_test(
                data,
                redis.key,
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
                duplicate_policy="last",
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
            yield_redis() as redis,
            raises(
                TimeSeriesMAddError, match="DataFrame must have a 'key' column; got .*"
            ),
        ):
            _ = time_series_madd(redis.ts, df)

    @given(yield_redis=redis_cms())
    def test_sync_error_madd_timestamp_missing(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        df = DataFrame(schema={"key": Utf8})
        with (
            yield_redis() as redis,
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
            yield_redis() as redis,
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
            yield_redis() as redis,
            raises(
                TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
            ),
        ):
            _ = time_series_madd(redis.ts, df)

    @given(yield_redis=redis_cms())
    def test_sync_error_madd_timestamp_is_not_a_zoned_datetime(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        with (
            yield_redis() as redis,
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
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        with (
            yield_redis() as redis,
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
        with yield_redis() as redis:
            values_or_df = self._prepare_test_error_madd_invalid_key(
                data, redis.key, case
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
        with yield_redis() as redis:
            values_or_df = self._prepare_test_error_madd_invalid_timestamp(
                data, redis.key, case
            )
            with raises(
                TimeSeriesMAddError,
                match="Timestamps must be at least the Epoch; got .*",
            ):
                _ = time_series_madd(redis.ts, values_or_df)

    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_error_madd_invalid_value(
        self,
        *,
        yield_redis: YieldRedisContainer,
        case: Literal["values", "DataFrame"],
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        with yield_redis() as redis:
            data = [(f"{redis.key}_{case}", timestamp, value)]
            match case:
                case "values":
                    values_or_df = data
                case "DataFrame":
                    values_or_df = DataFrame(
                        data, schema=self.float_schema, orient="row"
                    )
            with raises(TimeSeriesMAddError, match="The value .* is invalid"):
                _ = time_series_madd(redis.ts, values_or_df)

    @given(yield_redis=redis_cms())
    def test_error_range_no_keys_requested(
        self, *, yield_redis: YieldRedisContainer
    ) -> None:
        with (
            yield_redis() as redis,
            raises(
                TimeSeriesRangeError, match="At least 1 key must be requested; got .*"
            ),
        ):
            _ = time_series_range(redis.ts, [])

    @given(yield_redis=redis_cms())
    def test_error_range_invalid_key(self, *, yield_redis: YieldRedisContainer) -> None:
        with (
            yield_redis() as redis,
            raises(TimeSeriesRangeError, match="The key '.*' must exist"),
        ):
            _ = time_series_range(redis.ts, redis.key)

    @given(
        yield_redis=redis_cms(),
        timestamp=zoned_datetimes(
            min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=int32s(),
    )
    def test_error_range_key_with_int64_and_float64(
        self, *, yield_redis: YieldRedisContainer, timestamp: dt.datetime, value: int
    ) -> None:
        with yield_redis() as redis:
            _ = time_series_madd(
                redis.ts, [(redis.key, timestamp, value)], duplicate_policy="last"
            )
            _ = time_series_madd(
                redis.ts,
                [(redis.key, timestamp, float(value))],
                duplicate_policy="last",
            )
            with raises(
                TimeSeriesRangeError,
                match="The key '.*' contains both Int64 and Float64 data",
            ):
                _ = time_series_range(redis.ts, redis.key)

    @given(
        data=data(),
        series_names=lists_fixed_length(text_ascii(), 2, unique=True).map(tuple),
        time_zone=sampled_from([HONG_KONG, UTC]),
        key_timestamp_value=lists_fixed_length(text_ascii(), 3, unique=True).map(tuple),
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
        async with redis_cms_async(data) as redis:
            prepared = self._prepare_main_test(
                data,
                redis.key,
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
                duplicate_policy="last",
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
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesMAddError, match="DataFrame must have a 'key' column; got .*"
            ):
                _ = await time_series_madd_async(redis.ts, df)

    @given(data=data())
    async def test_async_error_madd_timestamp_missing(
        self, *, data: DataObject
    ) -> None:
        df = DataFrame(schema={"key": Utf8})
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesMAddError,
                match="DataFrame must have a 'timestamp' column; got .*",
            ):
                _ = await time_series_madd_async(redis.ts, df)

    @given(data=data())
    async def test_async_error_madd_value_missing(self, *, data: DataObject) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC})
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesMAddError,
                match="DataFrame must have a 'value' column; got .*",
            ):
                _ = await time_series_madd_async(redis.ts, df)

    @given(data=data())
    async def test_async_error_madd_key_is_not_utf8(self, *, data: DataObject) -> None:
        df = DataFrame(
            schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
        )
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
            ):
                _ = await time_series_madd_async(redis.ts, df)

    @given(data=data())
    async def test_async_error_madd_timestamp_is_not_a_zoned_datetime(
        self, *, data: DataObject
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        async with redis_cms_async(data) as redis:
            with raises(
                TimeSeriesMAddError,
                match="The 'timestamp' column must be a zoned Datetime; got Boolean",
            ):
                _ = await time_series_madd_async(redis.ts, df)

    @given(data=data())
    async def test_async_error_madd_value_is_not_numeric(
        self, *, data: DataObject
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        async with redis_cms_async(data) as redis:
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
        async with redis_cms_async(data) as redis:
            values_or_df = self._prepare_test_error_madd_invalid_key(
                data, redis.key, case
            )
            with raises(TimeSeriesMAddError, match="The key '.*' must exist"):
                _ = await time_series_madd_async(
                    redis.ts, values_or_df, assume_time_series_exist=True
                )

    @given(data=data())
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    async def test_async_error_madd_invalid_timestamp(
        self,
        *,
        data: DataObject,
        case: Literal["values", "DataFrame"],
    ) -> None:
        async with redis_cms_async(data) as redis:
            values_or_df = self._prepare_test_error_madd_invalid_timestamp(
                data, redis.key, case
            )
            with raises(
                TimeSeriesMAddError,
                match="Timestamps must be at least the Epoch; got .*",
            ):
                _ = await time_series_madd_async(redis.ts, values_or_df)

    def _prepare_main_test(
        self,
        data: DataObject,
        redis_key: str,
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
            tuple(f"{redis_key}_{case}_{name}" for name in series_names),
        )
        st_datetimes = zoned_datetimes(min_value=EPOCH_UTC, time_zone=time_zone).map(
            drop_microseconds
        )
        timestamps = data.draw(tuples(st_datetimes, st_datetimes))
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
        timestamp = data.draw(
            zoned_datetimes(
                min_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
            ).map(drop_microseconds)
        )
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
        timestamp = data.draw(
            zoned_datetimes(
                max_value=EPOCH_UTC, time_zone=sampled_from([HONG_KONG, UTC])
            ).map(drop_microseconds)
        )
        _ = assume(timestamp < EPOCH_UTC)
        value = data.draw(int32s())
        values = [(f"{key}_{case}", timestamp, value)]
        match case:
            case "values":
                return values
            case "DataFrame":
                return DataFrame(values, schema=self.int_schema, orient="row")


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
