from __future__ import annotations

from math import inf, nan
from typing import TYPE_CHECKING, ClassVar, Literal

import redis
import redis.asyncio
from hypothesis import HealthCheck, Phase, assume, given, reproduce_failure, settings
from hypothesis.strategies import datetimes, floats, sampled_from
from polars import Boolean, DataFrame, Datetime, Float64, Utf8
from pytest import mark, param, raises
from redis.commands.timeseries import TimeSeries

from tests.conftest import FLAKY, SKIPIF_CI_AND_NOT_LINUX
from utilities.datetime import EPOCH_NAIVE, EPOCH_UTC, drop_microseconds
from utilities.hypothesis import (
    assume_does_not_raise,
    datetimes_utc,
    longs,
    redis_clients,
    redis_time_series,
    text_ascii,
)
from utilities.iterables import CheckDuplicatesError, check_duplicates
from utilities.polars import DatetimeUTC, check_polars_dataframe
from utilities.redis import (
    TimeSeriesAddError,
    TimeSeriesMAddError,
    ensure_time_series_created,
    time_series_add,
    time_series_get,
    time_series_madd,
    time_series_range,
    yield_client,
    yield_client_async,
    yield_time_series,
    yield_time_series_async,
)
from utilities.zoneinfo import HONG_KONG, UTC, get_time_zone_name

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID
    from zoneinfo import ZoneInfo

    from polars._typing import SchemaDict


@SKIPIF_CI_AND_NOT_LINUX
class TestEnsureTimeSeriesCreated:
    @FLAKY
    @given(client_pair=redis_clients(), key=text_ascii())
    def test_main(self, *, client_pair: tuple[redis.Redis, UUID], key: str) -> None:
        client, uuid = client_pair
        full_key = f"{uuid}_{key}"
        assert client.exists(full_key) == 0
        for _ in range(2):
            ensure_time_series_created(client.ts(), full_key)
        assert client.exists(full_key) == 1


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesAddAndGet:
    @FLAKY
    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes(
            min_value=EPOCH_NAIVE, timezones=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_main(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp.fold == 0)
        ts, uuid = ts_pair
        full_key = f"{uuid}_{key}"
        res_add = time_series_add(
            ts, full_key, timestamp, value, duplicate_policy="last"
        )
        assert isinstance(res_add, int)
        res_timestamp, res_value = time_series_get(ts, full_key)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes_utc(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_error_at_upsert(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        with raises(  # noqa: PT012
            TimeSeriesAddError,
            match="Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got .*",
        ):
            for _ in range(2):
                _ = time_series_add(ts, f"{uuid}_{key}", timestamp, value)

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes_utc(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_invalid_timestamp(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        ts, uuid = ts_pair
        with raises(
            TimeSeriesAddError, match="Timestamp must be at least the Epoch; got .*"
        ):
            _ = time_series_add(
                ts, f"{uuid}_{key}", timestamp, value, duplicate_policy="last"
            )

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])).map(
            drop_microseconds
        ),
    )
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_invalid_value(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        with raises(TimeSeriesAddError, match="Invalid value; got .*"):
            _ = time_series_add(
                ts, f"{uuid}_{key}", timestamp, value, duplicate_policy="last"
            )


@SKIPIF_CI_AND_NOT_LINUX
class TestTimeSeriesMAddAndRange:
    schema: ClassVar[SchemaDict] = {
        "key": Utf8,
        "timestamp": DatetimeUTC,
        "value": Float64,
    }

    @given(
        ts_pair=redis_time_series(),
        key1=text_ascii(),
        key2=text_ascii(),
        datetime1=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        datetime2=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value1=longs() | floats(allow_nan=False, allow_infinity=False),
        value2=longs() | floats(allow_nan=False, allow_infinity=False),
        input_output_key=text_ascii(),
        input_output_timestamp=text_ascii(),
        input_time_zone=sampled_from([HONG_KONG, UTC]),
        output_time_zone=sampled_from([HONG_KONG, UTC]),
        input_output_value=text_ascii(),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    @reproduce_failure(
        "6.111.0",
        b"AABO0/eN18+MiwQDASMEOyYFHQEdBwUiAAQhAyQAARcAHQE+JToACoUAABuQBh0IBCUF170AAH4BAH////8AIQECBgUCAACn+gAAWAcFHQEuBwABDgY0MgU8EQcHAAEABDACNBcBBAIGBAIDFAYkBAMHBwM=",
    )
    @settings(
        phases={Phase.generate}, suppress_health_check={HealthCheck.filter_too_much}
    )
    def test_main(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        key1: str,
        key2: str,
        datetime1: dt.datetime,
        datetime2: dt.datetime,
        value1: float,
        value2: float,
        input_output_key: str,
        input_output_timestamp: str,
        input_output_time_zone: ZoneInfo,
        input_output_value: str,
    ) -> None:
        with assume_does_not_raise(CheckDuplicatesError):
            check_duplicates([
                key1,
                key2,
                input_output_key,
                input_output_timestamp,
                input_output_value,
            ])
        timestamps = [
            d.replace(tzinfo=input_output_time_zone) for d in [datetime1, datetime2]
        ]
        for timestamp in timestamps:
            _ = assume(timestamp.fold == 0)
        ts, uuid = ts_pair
        full_keys = [f"{uuid}_{case}_{key}" for key in [key1, key2]]
        data = list(zip(full_keys, timestamps, [value1, value2], strict=True))
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(
                    data,
                    schema={
                        input_output_key: Utf8,
                        input_output_timestamp: Datetime(
                            time_zone=get_time_zone_name(input_output_time_zone)
                        ),
                        input_output_value: Float64,
                    },
                    orient="row",
                )
        res_madd = time_series_madd(
            ts,
            values_or_df,
            key=input_output_key,
            timestamp=input_output_timestamp,
            value=input_output_value,
            duplicate_policy="last",
        )
        assert isinstance(res_madd, list)
        for i in res_madd:
            assert isinstance(i, int)
        res_range = time_series_range(
            ts,
            full_keys,
            output_key=input_output_key,
            output_timestamp=input_output_timestamp,
            output_time_zone=output_time_zone,
            output_value=input_output_value,
        )
        check_polars_dataframe(
            res_range,
            height=2,
            schema_list={
                input_output_key: Utf8,
                input_output_timestamp: Datetime(
                    time_zone=get_time_zone_name(output_time_zone)
                ),
                input_output_value: Float64,
            },
        )
        assert res_range.rows() == data

    @FLAKY
    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes_utc(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_invalid_key(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        data = [(f"{uuid}_{case}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(TimeSeriesMAddError, match="Invalid key; got '.*'"):
            _ = time_series_madd(ts, values_or_df, assume_time_series_exist=True)

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes_utc(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_invalid_timestamp(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        ts, uuid = ts_pair
        data = [(f"{uuid}_{case}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(
            TimeSeriesMAddError, match="Timestamps must be at least the Epoch; got .*"
        ):
            _ = time_series_madd(ts, values_or_df)

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])).map(
            drop_microseconds
        ),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_invalid_value(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        data = [(f"{uuid}_{case}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(TimeSeriesMAddError, match=r"Invalid value; got .*"):
            _ = time_series_madd(ts, values_or_df)

    @given(ts_pair=redis_time_series())
    def test_df_error_key(self, *, ts_pair: tuple[TimeSeries, UUID]) -> None:
        df = DataFrame(
            schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
        )
        with raises(
            TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
        ):
            _ = time_series_madd(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_df_error_timestamp(self, *, ts_pair: tuple[TimeSeries, UUID]) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        with raises(
            TimeSeriesMAddError,
            match="The 'timestamp' column must be a zoned Datetime; got Boolean",
        ):
            _ = time_series_madd(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_df_error_value(self, *, ts_pair: tuple[TimeSeries, UUID]) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        with raises(
            TimeSeriesMAddError, match="The 'value' column must be numeric; got Boolean"
        ):
            _ = time_series_madd(ts_pair[0], df)

    @given(ts_pair=redis_time_series(), key=text_ascii())
    def test_non_existent_key(
        self, *, ts_pair: tuple[TimeSeries, UUID], key: str
    ) -> None:
        ts, uuid = ts_pair
        df = time_series_range(ts, f"{uuid}_{key}")
        check_polars_dataframe(df, height=0, schema_list=self.schema)


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
