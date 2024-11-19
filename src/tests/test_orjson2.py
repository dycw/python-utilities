from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field, fields

from hypothesis import given, reproduce_failure
from hypothesis.strategies import (
    DataObject,
    booleans,
    builds,
    data,
    dates,
    dictionaries,
    floats,
    integers,
    lists,
    none,
    sampled_from,
)
from orjson import loads
from pytest import mark, param, raises

from utilities.hypothesis import int64s, text_ascii, zoned_datetimes
from utilities.orjson2 import deserialize2, serialize2
from utilities.sentinel import sentinel

_Object = bool | float | str | dt.date | dt.datetime
objects = (
    booleans()
    | floats(allow_nan=False, allow_infinity=False)
    | int64s()
    | text_ascii()
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
        | dictionaries(text_ascii(), objects)
        | lists(dictionaries(text_ascii(), objects))
        | dictionaries(text_ascii(), lists(objects))
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

    # @mark.only
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
        ser = serialize2(obj)
        # assert 0, ser
        result = deserialize2(ser, objects={Outer, Inner})
        assert result == obj

    def test_arbitrary_objects(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
