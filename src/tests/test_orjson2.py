from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from math import isinf, isnan
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
    dates,
    datetimes,
    dictionaries,
    floats,
    lists,
    sampled_from,
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
    _Serialize2TypeError,
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


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass1:
    x: int = 0


dataclass1s = builds(DataClass1)


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Inner:
    x: int = 0


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Outer:
    inner: DataClass2Inner


dataclass2s = builds(DataClass2Outer)


forexes = builds(Forex)
fills = builds(Fill, contract=forexes)
trades = builds(Trade, fills=lists(fills))


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
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_nested(self, *, obj: Any) -> None:
        with assume_does_not_raise(_Serialize2IntegerError):
            ser = serialize2(obj)
        result = deserialize2(ser, objects={DataClass2Inner, DataClass2Outer})
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
    @given(text=text_printable())
    def test_before(self, *, text: str) -> None:
        result = serialize2(text, before=str.upper)
        expected = serialize2(text.upper())
        assert result == expected

    def test_dataclass(self) -> None:
        obj = DataClass1()
        result = serialize2(obj)
        expected = b'{"[dc|DataClass1]":{}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(x=0))
        result = serialize2(obj)
        expected = b'{"[dc|DataClass2Outer]":{"inner":{"[dc|DataClass2Inner]":{}}}}'
        assert result == expected

    @given(obj=extend(dataclass1s.filter(lambda obj: obj.x >= 0)))
    def test_dataclass_hook_setup(self, *, obj: Any) -> None:
        ser = serialize2(obj)
        assert not search(b"-", ser)

    @given(obj=extend(dataclass1s))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_hook_main(self, *, obj: Any) -> None:
        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        ser = serialize2(obj, dataclass_final_hook=hook)
        assert not search(b"-", ser)

    @given(x=sampled_from([MIN_INT64 - 1, MAX_INT64 + 1]))
    def test_pre_process(self, *, x: int) -> None:
        with raises(_Serialize2IntegerError, match="Integer .* is out of range"):
            _ = serialize2(x)

    @given(obj=extend(trades))
    def test_ib_trades(self, *, obj: Any) -> None:
        def hook(cls: type[Any], mapping: StrMapping, /) -> Any:
            if issubclass(cls, Contract) and not issubclass(Contract, cls):
                mapping = {k: v for k, v in mapping.items() if k != "secType"}
            return mapping

        with assume_does_not_raise(_Serialize2IntegerError):
            ser = serialize2(obj, dataclass_final_hook=hook)
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

    def test_fallback(self) -> None:
        with raises(
            _Serialize2TypeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(
            _Serialize2TypeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize2(sentinel)
