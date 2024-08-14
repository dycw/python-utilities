from __future__ import annotations

from math import inf, nan
from typing import TYPE_CHECKING, ClassVar, Literal

import redis
import redis.asyncio
from hypothesis import assume, given
from hypothesis.strategies import datetimes, floats, sampled_from
from polars import Boolean, DataFrame, Float64, Utf8
from pytest import mark, param, raises

from tests.conftest import SKIPIF_CI
from utilities.datetime import EPOCH_NAIVE, EPOCH_UTC, drop_microseconds
from utilities.hypothesis import datetimes_utc, longs, redis_clients, text_ascii
from utilities.polars import DatetimeUTC, check_polars_dataframe
from utilities.redis import (
    TimeSeriesAddError,
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

    from polars._typing import SchemaDict


@SKIPIF_CI
class TestTimeSeriesAddAndGet:
    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(
            min_value=EPOCH_NAIVE, timezones=sampled_from([HONG_KONG, UTC])
        ).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_main(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        client, uuid = client_pair
        ts = client.ts()
        full_key = f"{uuid}_{key}"
        res_add = time_series_add(
            ts, full_key, timestamp, value, duplicate_policy="LAST"
        )
        assert isinstance(res_add, int)
        res_timestamp, res_value = time_series_get(ts, full_key)
        assert res_timestamp == timestamp.astimezone(UTC)
        assert res_value == value

    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes_utc(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_error_at_upsert(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        client, uuid = client_pair
        with raises(  # noqa: PT012
            TimeSeriesAddError,
            match="Error at upsert under DUPLICATE_POLICY == 'BLOCK'; got .*",
        ):
            for _ in range(2):
                _ = time_series_add(client.ts(), f"{uuid}_{key}", timestamp, value)

    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes_utc(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    def test_invalid_timestamp(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        client, uuid = client_pair
        with raises(
            TimeSeriesAddError, match="Timestamp must be at least the Epoch; got .*"
        ):
            _ = time_series_add(
                client.ts(), f"{uuid}_{key}", timestamp, value, duplicate_policy="LAST"
            )

    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes(timezones=sampled_from([HONG_KONG, UTC])).map(
            drop_microseconds
        ),
    )
    @mark.parametrize("value", [param(inf), param(-inf), param(nan)])
    def test_invalid_value(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        client, uuid = client_pair
        with raises(TimeSeriesAddError, match="Invalid value; got .*"):
            _ = time_series_add(
                client.ts(), f"{uuid}_{key}", timestamp, value, duplicate_policy="LAST"
            )


@SKIPIF_CI
class TestTimeSeriesMAddAndRange:
    schema: ClassVar[SchemaDict] = {
        "key": Utf8,
        "timestamp": DatetimeUTC,
        "value": Float64,
    }

    @given(
        client_pair=redis_clients(),
        key1=text_ascii(),
        key2=text_ascii(),
        datetime1=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        datetime2=datetimes(min_value=EPOCH_NAIVE).map(drop_microseconds),
        time_zone=sampled_from([HONG_KONG, UTC]),
        value1=longs() | floats(allow_nan=False, allow_infinity=False),
        value2=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_main(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        case: Literal["values", "DataFrame"],
        key1: str,
        key2: str,
        datetime1: dt.datetime,
        datetime2: dt.datetime,
        time_zone: ZoneInfo,
        value1: float,
        value2: float,
    ) -> None:
        _ = assume(key1 != key2)
        timestamps = [d.replace(tzinfo=time_zone) for d in [datetime1, datetime2]]
        for timestamp in timestamps:
            _ = assume(timestamp.fold == 0)
        client, uuid = client_pair
        full_keys = [f"{uuid}_{key}" for key in [key1, key2]]
        ts = client.ts()
        for full_key in full_keys:
            if client.exists(full_key) == 0:
                _ = ts.create(full_key, duplicate_policy="LAST")
        data = list(zip(full_keys, timestamps, [value1, value2], strict=True))
        match case:
            case "values":
                res_madd = time_series_madd(ts, data)
            case "DataFrame":
                df = DataFrame(data, schema=self.schema, orient="row")
                res_madd = time_series_madd(ts, df)
        assert isinstance(res_madd, list)
        for i in res_madd:
            assert isinstance(i, int)
        res_range = time_series_range(ts, full_keys)
        check_polars_dataframe(res_range, height=2, schema_list=self.schema)
        assert res_range.rows() == data

    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes_utc(min_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_invalid_key(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        client, uuid = client_pair
        data = [(f"{uuid}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(TimeSeriesMAddError, match="Invalid key; got '.*'"):
            _ = time_series_madd(client.ts(), values_or_df)

    @given(
        client_pair=redis_clients(),
        key=text_ascii(),
        timestamp=datetimes_utc(max_value=EPOCH_NAIVE).map(drop_microseconds),
        value=longs() | floats(allow_nan=False, allow_infinity=False),
    )
    @mark.parametrize("case", [param("values"), param("DataFrame")])
    def test_invalid_timestamp(
        self,
        *,
        client_pair: tuple[redis.Redis, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        _ = assume(timestamp < EPOCH_UTC)
        client, uuid = client_pair
        data = [(f"{uuid}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(
            TimeSeriesMAddError, match="Timestamps must be at least the Epoch; got .*"
        ):
            _ = time_series_madd(client.ts(), values_or_df)

    @given(
        client_pair=redis_clients(),
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
        client_pair: tuple[redis.Redis, UUID],
        case: Literal["values", "DataFrame"],
        key: str,
        timestamp: dt.datetime,
        value: float,
    ) -> None:
        client, uuid = client_pair
        data = [(f"{uuid}_{key}", timestamp, value)]
        match case:
            case "values":
                values_or_df = data
            case "DataFrame":
                values_or_df = DataFrame(data, schema=self.schema, orient="row")
        with raises(TimeSeriesMAddError, match=r"Invalid value; got .*"):
            _ = time_series_madd(client.ts(), values_or_df)

    @given(client_pair=redis_clients())
    def test_df_error_key(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        df = DataFrame(
            schema={"key": Boolean, "timestamp": DatetimeUTC, "value": Float64}
        )
        with raises(
            TimeSeriesMAddError, match="The 'key' column must be Utf8; got Boolean"
        ):
            _ = time_series_madd(client.ts(), df)

    @given(client_pair=redis_clients())
    def test_df_error_timestamp(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        df = DataFrame(schema={"key": Utf8, "timestamp": Boolean, "value": Float64})
        with raises(
            TimeSeriesMAddError,
            match="The 'timestamp' column must be Datetime; got Boolean",
        ):
            _ = time_series_madd(client.ts(), df)

    @given(client_pair=redis_clients())
    def test_df_error_value(self, *, client_pair: tuple[redis.Redis, UUID]) -> None:
        client, _ = client_pair
        df = DataFrame(schema={"key": Utf8, "timestamp": DatetimeUTC, "value": Boolean})
        with raises(
            TimeSeriesMAddError, match="The 'value' column must be numeric; got Boolean"
        ):
            _ = time_series_madd(client.ts(), df)


class TestYieldClient:
    def test_sync(self) -> None:
        with yield_client() as client:
            assert isinstance(client, redis.Redis)

    async def test_async(self) -> None:
        async with yield_client_async() as client:
            assert isinstance(client, redis.asyncio.Redis)
