from __future__ import annotations

import datetime as dt
from contextlib import suppress
from io import StringIO
from logging import DEBUG, StreamHandler, getLogger
from math import isinf, isnan
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import DataObject, builds, data, lists, sampled_from
from pytest import approx, mark, param, raises

from tests.test_operator import (
    DataClass1,
    DataClass2Inner,
    DataClass2Outer,
    DataClass3,
    SubFrozenSet,
    SubList,
    SubSet,
    SubTuple,
    TruthEnum,
    make_objects,
)
from utilities.dataclasses import asdict_without_defaults, is_dataclass_instance
from utilities.datetime import SECOND, get_now
from utilities.hypothesis import (
    assume_does_not_raise,
    settings_with_reduced_examples,
    text_printable,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.operator import _IsEqualUnsortableCollectionsError, is_equal
from utilities.orjson import (
    OrjsonFormatter,
    OrjsonLogRecord,
    _DeserializeNoObjectsError,
    _DeserializeObjectNotFoundError,
    _SerializeIntegerError,
    deserialize,
    serialize,
)
from utilities.types import is_string_mapping
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from utilities.dataclasses import Dataclass
    from utilities.types import StrMapping


# handler


class TestOrjsonFormatter:
    def test_main(self) -> None:
        buffer = StringIO()
        name = TestOrjsonFormatter.test_main.__qualname__
        logger = getLogger(name)
        logger.setLevel(DEBUG)
        handler = StreamHandler(buffer)

        def before(obj: Any, /) -> Any:
            if is_string_mapping(obj):
                return {k: v for k, v in obj.items() if not k.startswith("zoned")}
            return obj

        handler.setFormatter(OrjsonFormatter(before=before))
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        extra = {"a": 1, "b": 2}
        logger.debug("message", extra=extra)
        record = deserialize(buffer.getvalue().encode(), objects={OrjsonLogRecord})
        assert isinstance(record, OrjsonLogRecord)
        assert record.name == name
        assert record.message == "message"
        assert record.level == DEBUG
        assert record.path_name == Path(__file__)
        assert record.line_num == approx(100, rel=0.1)
        assert abs(record.datetime - get_now(time_zone="local")) <= SECOND
        assert record.func_name == TestOrjsonFormatter.test_main.__name__
        assert record.stack_info is None
        assert record.extra == extra


# serialize/deserialize


class TestSerializeAndDeserialize:
    @given(obj=make_objects())
    def test_main(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj))
        with assume_does_not_raise(_IsEqualUnsortableCollectionsError):
            assert is_equal(result, obj)

    @given(obj=make_objects(dataclass1=True))
    def test_dataclass(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClass1})
        assert result == obj

    @given(obj=make_objects(dataclass2=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_nested(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClass2Inner, DataClass2Outer})
        assert result == obj

    @given(obj=make_objects(dataclass3=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_lit(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClass3})
        assert result == obj

    @given(obj=builds(DataClass1))
    def test_dataclass_no_objects_error(self, *, obj: DataClass1) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeNoObjectsError,
            match="Objects required to deserialize '.*' from .*",
        ):
            _ = deserialize(ser)

    @given(obj=builds(DataClass1))
    def test_dataclass_empty_error(self, *, obj: DataClass1) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeObjectNotFoundError,
            match=r"Unable to find object to deserialize '.*' from .*",
        ):
            _ = deserialize(ser, objects=set())

    @given(obj=make_objects(enum=True))
    def test_enum(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={TruthEnum})
        assert result == obj

    @given(obj=make_objects(sub_frozenset=True))
    def test_sub_frozenset(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubFrozenSet})
        assert result == obj

    @given(obj=make_objects(sub_list=True))
    def test_sub_list(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubList})
        assert result == obj

    @given(obj=make_objects(sub_set=True))
    def test_sub_set(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubSet})
        assert result == obj

    @given(obj=make_objects(sub_tuple=True))
    def test_sub_tuple(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubTuple})
        assert result == obj

    @mark.parametrize(
        ("utc", "expected"),
        [
            param(UTC, b'"[dt]2000-01-01T00:00:00+00:00[UTC]"'),
            param(dt.UTC, b'"[dt]2000-01-01T00:00:00+00:00[dt.UTC]"'),
        ],
        ids=str,
    )
    def test_utc(self, *, utc: dt.tzinfo, expected: bytes) -> None:
        datetime = dt.datetime(2000, 1, 1, tzinfo=utc)
        ser = serialize(datetime)
        assert ser == expected
        result = deserialize(ser)
        assert result == datetime
        assert result.tzinfo is utc


class TestSerialize:
    @given(text=text_printable())
    def test_before(self, *, text: str) -> None:
        result = serialize(text, before=str.upper)
        expected = serialize(text.upper())
        assert result == expected

    def test_dataclass(self) -> None:
        obj = DataClass1()
        result = serialize(obj)
        expected = b'{"[dc|DataClass1]":{}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(x=0))
        result = serialize(obj)
        expected = b'{"[dc|DataClass2Outer]":{"inner":{"[dc|DataClass2Inner]":{}}}}'
        assert result == expected

    def test_dataclass_hook_main(self) -> None:
        obj = DataClass1()

        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        result = serialize(obj, dataclass_final_hook=hook)
        expected = b'{"[dc|DataClass1]":{}}'
        assert result == expected

    @given(x=sampled_from([MIN_INT64 - 1, MAX_INT64 + 1]))
    def test_pre_process(self, *, x: int) -> None:
        with raises(_SerializeIntegerError, match="Integer .* is out of range"):
            _ = serialize(x)

    @given(data=data())
    @settings_with_reduced_examples(suppress_health_check={HealthCheck.filter_too_much})
    def test_ib_trades(self, *, data: DataObject) -> None:
        from ib_async import (
            ComboLeg,
            CommissionReport,
            Contract,
            DeltaNeutralContract,
            Execution,
            Fill,
            Forex,
            Order,
            Trade,
        )

        forexes = builds(Forex)
        fills = builds(Fill, contract=forexes)
        trades = builds(Trade, fills=lists(fills))
        obj = data.draw(make_objects(extra_base=trades))

        def hook(cls: type[Any], mapping: StrMapping, /) -> Any:
            if issubclass(cls, Contract) and not issubclass(Contract, cls):
                mapping = {k: v for k, v in mapping.items() if k != "secType"}
            return mapping

        with assume_does_not_raise(_SerializeIntegerError):
            ser = serialize(obj, dataclass_final_hook=hook)
        result = deserialize(
            ser,
            objects={
                CommissionReport,
                ComboLeg,
                Contract,
                DeltaNeutralContract,
                Execution,
                Fill,
                Forex,
                Order,
                Trade,
            },
        )

        def unpack(obj: Any, /) -> Any:
            if isinstance(obj, list | tuple):
                return list(map(unpack, obj))
            if isinstance(obj, dict):
                return {k: unpack(v) for k, v in obj.items()}
            if is_dataclass_instance(obj):
                return unpack(asdict_without_defaults(obj))
            with suppress(TypeError):
                if isinf(obj) or isnan(obj):
                    return None
            return obj

        def eq(x: Any, y: Any) -> Any:
            if isinstance(x, list) and isinstance(y, list):
                return all(eq(x_i, y_i) for x_i, y_i in zip(x, y, strict=True))
            if isinstance(x, dict) and isinstance(y, dict):
                return (set(x) == set(y)) and all(eq(x[i], y[i]) for i in x)
            if is_dataclass_instance(x) and is_dataclass_instance(y):
                return eq(unpack(x), unpack(y))
            return x == y

        ur, uo = unpack(result), unpack(obj)
        assert eq(ur, uo)
