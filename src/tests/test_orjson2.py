from __future__ import annotations

from dataclasses import dataclass
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import given, reproduce_failure
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
)
from ib_async import Contract, Fill, Forex, Order, Trade
from pytest import raises

from utilities.dataclasses import asdict_without_defaults, is_dataclass_instance
from utilities.hypothesis import (
    int64s,
    text_ascii,
    text_printable,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.orjson2 import (
    _Deserialize2NoObjectsError,
    _Deserialize2ObjectEmptyError,
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


dataclass1s = builds(DataClass1, x=int64s() | none())


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Inner:
    a: int | None = None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Outer:
    inner: DataClass2Inner


dataclass2s = builds(
    DataClass2Outer, inner=builds(DataClass2Inner, a=int64s() | none())
)


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

    @given(data=data())
    @reproduce_failure("6.119.3", b"AXicY2BkZiAbAAABKgAF")
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
        result = deserialize2(
            serialize2(obj, dataclass_hook=hook), objects={Trade, Order, Forex}
        )

        def unpack(obj: Any, /) -> Any:
            if isinstance(obj, list):
                return list(map(unpack, obj))
            if isinstance(obj, dict):
                return {k: unpack(v) for k, v in obj.items()}
            if is_dataclass_instance(obj):
                return asdict_without_defaults(obj)
            return obj

        assert unpack(result) == unpack(obj)

    def test_fallback(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
