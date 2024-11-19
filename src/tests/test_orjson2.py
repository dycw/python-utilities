from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    builds,
    data,
    dates,
    dictionaries,
    floats,
    lists,
    none,
)
from pytest import raises

from utilities.hypothesis import int64s, text_digits, text_printable, zoned_datetimes
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

_Object = bool | float | str | dt.date | dt.datetime
objects = (
    booleans()
    | floats(allow_nan=False, allow_infinity=False)
    | int64s()
    | text_printable()
    | dates()
    | zoned_datetimes()
)


class TestSerializeAndDeserialize2:
    @given(obj=objects)
    def test_main(self, *, obj: _Object) -> None:
        result = deserialize2(serialize2(obj))
        assert result == obj

    @given(
        objects=lists(objects)
        | dictionaries(text_printable(), objects)
        | lists(dictionaries(text_printable(), objects))
        | dictionaries(text_printable(), lists(objects))
    )
    def test_nested(
        self,
        *,
        objects: list[_Object]
        | dict[str, _Object]
        | list[dict[str, _Object]]
        | dict[str, list[_Object]],
    ) -> None:
        result = deserialize2(serialize2(objects))
        assert result == objects

    @given(x=text_digits())
    def test_str_of_digits(self, *, x: str) -> None:
        result = deserialize2(serialize2(x))
        assert result == x

    @given(x=int64s() | none())
    def test_dataclass(self, *, x: int | None) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example(x=x)
        ser = serialize2(obj)
        result = deserialize2(ser, objects={Example})
        assert result == obj

    @given(data=data())
    def test_dataclass_nested(self, *, data: DataObject) -> None:
        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Inner:
            a: int
            b: int = 0
            c: list[int] = field(default_factory=list)

        inner_st = builds(Inner, a=int64s(), b=int64s(), c=lists(int64s()))
        inner_default = data.draw(inner_st)

        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Outer:
            x: Inner
            y: Inner = inner_default
            z: list[Inner] = field(default_factory=list)

        obj = data.draw(builds(Outer, x=inner_st, y=inner_st, z=lists(inner_st)))
        result = deserialize2(serialize2(obj), objects={Outer, Inner})
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

    def test_fallback(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
