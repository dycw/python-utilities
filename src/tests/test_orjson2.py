from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from math import isinf, isnan, nan
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, given, reproduce_failure, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    builds,
    data,
    dates,
    datetimes,
    dictionaries,
    floats,
    lists,
    none,
    recursive,
    sampled_from,
    sets,
    tuples,
)
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
from pytest import raises

from utilities.dataclasses import asdict_without_defaults, is_dataclass_instance
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    text_ascii,
    text_printable,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.orjson2 import (
    _Deserialize2NoObjectsError,
    _Deserialize2ObjectEmptyError,
    _Serialize2IntegerError,
    deserialize2,
    serialize2,
)
from utilities.sentinel import sentinel

if TYPE_CHECKING:
    from utilities.dataclasses import Dataclass
    from utilities.types import StrMapping


# strategies


base = (
    booleans()
    | floats(allow_nan=False, allow_infinity=False)
    | int64s()
    | text_ascii().map(Path)
    | text_printable()
    | timedeltas_2w()
    | dates()
    | datetimes()
    | zoned_datetimes()
)


def extend(strategy: SearchStrategy[Any]) -> SearchStrategy[Any]:
    return lists(strategy) | dictionaries(text_ascii(), strategy)


objects = recursive(
    base, lambda children: lists(children) | dictionaries(text_printable(), children)
)


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass1:
    x: int | None = None


dataclass1s = builds(DataClass1)


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Inner:
    a: int | None = None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Outer:
    inner: DataClass2Inner


dataclass2s = builds(DataClass2Outer)


class TestSerializeAndDeserialize2:
    @given(obj=extend(base))
    def test_main(self, *, obj: Any) -> None:
        result = deserialize2(serialize2(obj))
        assert result == obj

    @given(obj=extend(base | dataclass1s))
    def test_dataclass(self, *, obj: Any) -> None:
        result = deserialize2(serialize2(obj), objects={DataClass1})
        assert result == obj

    @given(obj=extend(base | dataclass2s))
    def test_dataclass_nested(self, *, obj: Any) -> None:
        result = deserialize2(
            serialize2(obj), objects={DataClass2Inner, DataClass2Outer}
        )
        assert result == obj

    @given(obj=dataclass1s)
    def test_dataclass_no_objects_error(self, *, obj: DataClass1) -> None:
        ser = serialize2(obj)
        with raises(
            _Deserialize2NoObjectsError,
            match="Objects required to deserialize .* from .*",
        ):
            _ = deserialize2(ser)

    @given(obj=dataclass1s)
    def test_dataclass_empty_error(self, *, obj: DataClass1) -> None:
        ser = serialize2(obj)
        with raises(
            _Deserialize2ObjectEmptyError,
            match=r"Unable to find object '.*' to deserialize .* \(from .*\)",
        ):
            _ = deserialize2(ser, objects=set())


class TestSerialize2:
    def test_dataclass(self) -> None:
        obj = DataClass1(x=0)
        result = serialize2(obj)
        expected = b'{"[dc|DataClass1]":{"x":0}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(a=0))
        result = serialize2(obj)
        expected = (
            b'{"[dc|DataClass2Outer]":{"inner":{"[dc|DataClass2Inner]":{"a":0}}}}'
        )
        assert result == expected

    @given(
        obj=extend(dataclass1s.filter(lambda obj: (obj.x is not None) and (obj.x >= 0)))
    )
    def test_dataclass_hook_setup(self, *, obj: Any) -> None:
        ser = serialize2(obj)
        assert not search(b"-", ser)

    @given(obj=extend(dataclass1s.filter(lambda obj: obj.x is not None)))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_hook_main(self, *, obj: Any) -> None:
        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        ser = serialize2(obj, dataclass_hook=hook)
        assert not search(b"-", ser)

    @given(x=int64s())
    def test_dataclass_hook_on_list(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)

        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        result = serialize2(obj, dataclass_hook=hook)
        expected = serialize2(
            {"[dc|" + Example.__qualname__ + "]": {"x": x} if x >= 0 else {}},
            dataclass_hook=hook,
        )
        assert result == expected

    @given(x=sampled_from([MIN_INT64 - 1, MAX_INT64 + 1]))
    def test_pre_process(self, *, x: int) -> None:
        with raises(_Serialize2IntegerError, match="Integer .* is out of range"):
            _ = serialize2(x)

    @given(data=data())
    def test_ib(self, *, data: DataObject) -> None:
        def hook(cls: type[Any], mapping: StrMapping, /) -> Any:
            if issubclass(cls, Contract) and not issubclass(Contract, cls):
                mapping = {k: v for k, v in mapping.items() if k != "secType"}
            return mapping

        forexes = builds(Forex)
        orders = builds(Order)
        trades = builds(Trade, contract=forexes, order=orders)
        fills = builds(Fill)
        obj = data.draw(extend(forexes | orders | trades | fills))
        with assume_does_not_raise(_Serialize2IntegerError):
            ser = serialize2(obj, dataclass_hook=hook)
        result = deserialize2(
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
            if isinstance(obj, list):
                return list(map(unpack, obj))
            if isinstance(obj, dict):
                return {k: unpack(v) for k, v in obj.items()}
            if is_dataclass_instance(obj):
                return unpack(asdict_without_defaults(obj))
            with suppress(TypeError):
                if isnan(obj):
                    return None
            with suppress(TypeError):
                if isinf(obj):
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

    def test_fallback(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
