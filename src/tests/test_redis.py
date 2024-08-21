from __future__ import annotations

from math import inf, nan
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import redis
import redis.asyncio
from hypothesis import HealthCheck, assume, given, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    data,
    datetimes,
    floats,
    sampled_from,
    tuples,
)
from polars import Boolean, DataFrame, DataType, Float64, Int64, Utf8
from polars.testing import assert_frame_equal
from pytest import mark, param, raises
from redis.commands.timeseries import TimeSeries

from tests.conftest import FLAKY, SKIPIF_CI_AND_NOT_LINUX
from utilities.datetime import EPOCH_NAIVE, EPOCH_UTC, drop_microseconds
from utilities.hypothesis import (
    int32s,
    lists_fixed_length,
    redis_clients,
    redis_time_series,
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
    time_series_add,
    time_series_add_dataframe,
    time_series_get,
    time_series_madd,
    time_series_range,
    time_series_read_dataframe,
    yield_client,
    yield_client_async,
    yield_time_series,
    yield_time_series_async,
)
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    import datetime as dt
    from uuid import UUID
    from zoneinfo import ZoneInfo

    from polars._typing import PolarsDataType, SchemaDict


def _clean_datetime(
    datetime: dt.datetime, /, *, time_zone: ZoneInfo | None = None
) -> dt.datetime:
    _ = assume(datetime.fold == 0)
    if time_zone is not None:
        datetime = datetime.replace(tzinfo=time_zone)
    return max(datetime, EPOCH_UTC.astimezone(datetime.tzinfo))


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
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_main(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        full_key = f"{uuid}_{key}"
        timestamp = _clean_datetime(timestamp)
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
        timestamp=zoned_datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
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
        timestamp=zoned_datetimes(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=int32s() | floats(allow_nan=False, allow_infinity=False),
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
        timestamp=datetimes(
            min_value=EPOCH_NAIVE, timezones=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
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
                ts,
                f"{uuid}_{key}",
                _clean_datetime(timestamp),
                value,
                duplicate_policy="last",
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
        ts_pair=redis_time_series(),
        keys=lists_fixed_length(text_ascii(), 6, unique=True),
        datetime1=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        datetime2=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
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
        ts_pair: tuple[TimeSeries, UUID],
        keys: list[str],
        datetime1: dt.datetime,
        datetime2: dt.datetime,
        time_zone: ZoneInfo,
        strategy1: SearchStrategy[Any],
        dtype1: DataType,
        strategy2: SearchStrategy[Any],
        dtype2: DataType,
    ) -> None:
        key, id1, id2, timestamp, key_value1, key_value2 = keys
        ts, uuid = ts_pair
        full_id1, full_id2 = (f"{uuid}_{id_}" for id_ in [id1, id2])
        timestamp1, timestamp2 = (
            _clean_datetime(d, time_zone=time_zone) for d in [datetime1, datetime2]
        )
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

    @given(ts_pair=redis_time_series())
    def test_error_add_key_missing(self, *, ts_pair: tuple[TimeSeries, UUID]) -> None:
        df = DataFrame()
        with raises(
            TimeSeriesAddDataFrameError,
            match="DataFrame must have a 'key' column; got .*",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_add_timestamp_missing(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(schema={"key": Utf8})
        with raises(
            TimeSeriesAddDataFrameError,
            match="DataFrame must have a 'timestamp' column; got .*",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_add_key_is_not_utf8(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(schema={"key": Boolean, "timestamp": DatetimeUTC})
        with raises(
            TimeSeriesAddDataFrameError,
            match="The 'key' column must be Utf8; got Boolean",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_timestamp_is_not_a_zoned_datetime(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean})
        with raises(
            TimeSeriesAddDataFrameError,
            match="The 'timestamp' column must be a zoned Datetime; got Boolean",
        ):
            _ = time_series_add_dataframe(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_read_no_keys_requested(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        with raises(
            TimeSeriesReadDataFrameError, match="At least 1 key must be requested"
        ):
            _ = time_series_read_dataframe(ts_pair[0], [], [])

    @given(ts_pair=redis_time_series(), key=text_ascii())
    def test_error_read_no_columns_requested(
        self, *, ts_pair: tuple[TimeSeries, UUID], key: str
    ) -> None:
        ts, uuid = ts_pair
        with raises(
            TimeSeriesReadDataFrameError, match="At least 1 column must be requested"
        ):
            _ = time_series_read_dataframe(ts, f"{uuid}_{key}", [])


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

    @FLAKY
    @given(
        data=data(),
        ts_pair=redis_time_series(),
        keys=lists_fixed_length(text_ascii(), 5, unique=True),
        datetime1=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        datetime2=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        time_zone=sampled_from([HONG_KONG, UTC]),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    @mark.parametrize(
        ("strategy", "dtype"),
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
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        keys: list[str],
        datetime1: dt.datetime,
        datetime2: dt.datetime,
        time_zone: ZoneInfo,
        strategy: SearchStrategy[Any],
        dtype: PolarsDataType,
    ) -> None:
        key, key1, key2, timestamp, value = keys
        ts, uuid = ts_pair
        full_keys = [f"{uuid}_{case}_{key}" for key in [key1, key2]]
        timestamps = [
            _clean_datetime(d, time_zone=time_zone) for d in [datetime1, datetime2]
        ]
        values = data.draw(tuples(strategy, strategy))
        triples = list(zip(full_keys, timestamps, values, strict=True))
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
        res_madd = time_series_madd(
            ts,
            values_or_df,
            key=key,
            timestamp=timestamp,
            value=value,
            duplicate_policy="last",
        )
        assert isinstance(res_madd, list)
        for i in res_madd:
            assert isinstance(i, int)
        res_range = time_series_range(
            ts,
            full_keys,
            output_key=key,
            output_timestamp=timestamp,
            output_time_zone=time_zone,
            output_value=value,
        )
        check_polars_dataframe(res_range, height=2, schema_list=schema)
        assert res_range.rows() == triples

    @given(ts_pair=redis_time_series())
    def test_error_madd_key_missing(self, *, ts_pair: tuple[TimeSeries, UUID]) -> None:
        ts, _ = ts_pair
        df = DataFrame()
        with raises(
            TimeSeriesMAddError, match="DataFrame must have a 'key' column; got .*"
        ):
            _ = time_series_madd(ts, df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_timestamp_missing(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        ts, _ = ts_pair
        df = DataFrame(schema={"key": Utf8})
        with raises(
            TimeSeriesMAddError,
            match="DataFrame must have a 'timestamp' column; got .*",
        ):
            _ = time_series_madd(ts, df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_value_missing(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        ts, _ = ts_pair
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC})
        with raises(
            TimeSeriesMAddError, match="DataFrame must have a 'value' column; got .*"
        ):
            _ = time_series_madd(ts, df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_key_is_not_utf8(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(
            schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
        )
        with raises(
            TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
        ):
            _ = time_series_madd(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_timestamp_is_not_a_zoned_datetime(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        with raises(
            TimeSeriesMAddError,
            match="The 'timestamp' column must be a zoned Datetime; got Boolean",
        ):
            _ = time_series_madd(ts_pair[0], df)

    @given(ts_pair=redis_time_series())
    def test_error_madd_value_is_not_numeric(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        with raises(
            TimeSeriesMAddError, match="The 'value' column must be numeric; got Boolean"
        ):
            _ = time_series_madd(ts_pair[0], df)

    @FLAKY
    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=zoned_datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=int32s(),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_error_madd_invalid_key(
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
                values_or_df = DataFrame(data, schema=self.int_schema, orient="row")
        with raises(TimeSeriesMAddError, match="The key '.*' must exist"):
            _ = time_series_madd(ts, values_or_df, assume_time_series_exist=True)

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=zoned_datetimes(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=int32s(),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_error_madd_invalid_timestamp(
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
                values_or_df = DataFrame(data, schema=self.int_schema, orient="row")
        with raises(
            TimeSeriesMAddError, match="Timestamps must be at least the Epoch; got .*"
        ):
            _ = time_series_madd(ts, values_or_df)

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=datetimes(
            min_value=EPOCH_NAIVE, timezones=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_error_madd_invalid_value(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        ts, uuid = ts_pair
        data = [(f"{uuid}_{case}_{key}", _clean_datetime(timestamp), value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.float_schema, orient="row")
        with raises(TimeSeriesMAddError, match="The value .* is invalid"):
            _ = time_series_madd(ts, values_or_df)

    @given(ts_pair=redis_time_series())
    def test_error_range_no_keys_requested(
        self, *, ts_pair: tuple[TimeSeries, UUID]
    ) -> None:
        ts, _ = ts_pair
        with raises(
            TimeSeriesRangeError, match="At least 1 key must be requested; got .*"
        ):
            _ = time_series_range(ts, [])

    @given(ts_pair=redis_time_series(), key=text_ascii())
    def test_error_range_invalid_key(
        self, *, ts_pair: tuple[TimeSeries, UUID], key: str
    ) -> None:
        ts, uuid = ts_pair
        with raises(TimeSeriesRangeError, match="The key '.*' must exist"):
            _ = time_series_range(ts, f"{uuid}_{key}")

    @given(
        ts_pair=redis_time_series(),
        key=text_ascii(),
        timestamp=zoned_datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=int32s(),
    )
    def test_error_range_key_with_int64_and_float64(
        self,
        *,
        ts_pair: tuple[TimeSeries, UUID],
        key: str,
        timestamp: dt.datetime,
        value: int,
    ) -> None:
        ts, uuid = ts_pair
        _ = time_series_madd(
            ts, [(f"{uuid}_{key}", timestamp, value)], duplicate_policy="last"
        )
        _ = time_series_madd(
            ts, [(f"{uuid}_{key}", timestamp, float(value))], duplicate_policy="last"
        )
        with raises(
            TimeSeriesRangeError,
            match="The key '.*' contains both Int64 and Float64 data",
        ):
            _ = time_series_range(ts, f"{uuid}_{key}")


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
