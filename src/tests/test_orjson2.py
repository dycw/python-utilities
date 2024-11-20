from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from hypothesis import given, reproduce_failure, settings
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    builds,
    data,
    dates,
    dictionaries,
    floats,
    lists,
    none,
    recursive,
)
from hypothesis.strategies._internal.strategies import Ex
from pytest import mark, param, raises

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


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass1:
    x: int | None = None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Inner:
    a: int | None = None


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Outer:
    inner: DataClass2Inner


base = (
    booleans()
    | floats(allow_nan=False, allow_infinity=False)
    | int64s()
    | text_printable()
    | timedeltas_2w()
    | dates()
    | zoned_datetimes()
)


def extend(strategy: SearchStrategy[Any]) -> SearchStrategy[Any]:
    return lists(strategy) | dictionaries(text_printable(), strategy)


objects = recursive(
    base, lambda children: lists(children) | dictionaries(text_printable(), children)
)


class TestSerializeAndDeserialize2:
    @given(obj=extend(base))
    def test_main(self, *, obj: Any) -> None:
        result = deserialize2(serialize2(obj))
        assert result == obj

    @given(obj=extend(base | builds(DataClass1, x=int64s() | none())))
    def test_dataclass(self, *, obj: Any) -> None:
        result = deserialize2(serialize2(obj), objects={DataClass1})
        assert result == obj

    @given(
        obj=extend(
            base
            | builds(
                DataClass2Outer, inner=builds(DataClass2Inner, a=int64s() | none())
            )
        )
    )
    def test_dataclass_nested(self, *, obj: Any) -> None:
        result = deserialize2(
            serialize2(obj), objects={DataClass2Inner, DataClass2Outer}
        )
        assert result == obj

    @given(x=int64s() | none())
    def test_dataclass_no_objects_error(self, *, x: int | None) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)
        ser = serialize2(obj)
        with raises(
            _Deserialize2NoObjectsError,
            match="Objects required to deserialize .* from .*",
        ):
            _ = deserialize2(ser)

    @given(x=int64s() | none())
    def test_dataclass_empty_error(self, *, x: int | None) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)
        ser = serialize2(obj)
        with raises(
            _Deserialize2ObjectEmptyError,
            match=r"Unable to find object '.*' to deserialize .* \(from .*\)",
        ):
            _ = deserialize2(ser, objects=set())


class TestSerialize2:
    def test_dataclass(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = Example(x=0)
        result = serialize2(obj)
        expected = b'{"[dc|TestSerialize2.test_dataclass.<locals>.Example]":{"x":0}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Inner:
            a: int

        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Outer:
            x: Inner

        obj = Outer(x=Inner(a=0))
        result = serialize2(obj)
        expected = b'{"[dc|TestSerialize2.test_dataclass_nested.<locals>.Outer]":{"x":{"[dc|TestSerialize2.test_dataclass_nested.<locals>.Inner]":{"a":0}}}}'
        assert result == expected

    @given(x=int64s())
    def test_dataclass_hook(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)

        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        result = deserialize2(serialize2(obj, dataclass_hook=hook), objects={Example})
        expected = Example(x=x) if x >= 0 else Example()
        assert result == expected

    @given(x=int64s())
    @mark.skip
    def test_dataclass_hook_on_list(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)

        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        result = serialize2(obj, dataclass_hook=hook)
        expected = serialize2(
            {"[dc|" + Example.__qualname__ + "]": {"x": 0} if x >= 0 else {}},
            dataclass_hook=hook,
        )
        assert result == expected

    # def test_ib(self) -> None:
    #     from ib_async import Trade
    #
    #     x = [Trade]
    #
    #     result = deserialize2(serialize2(obj, dataclass_hook=hook), objects={Example})
    #     expected = [Example(x=x) if x >= 0 else Example()]
    #     assert result == expected

    def test_fallback(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
