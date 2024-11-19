from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from hypothesis import given, reproduce_failure
from hypothesis.strategies import booleans, dates, dictionaries, floats, lists
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
    @reproduce_failure("6.119.3", b"AAQAAAAAAA==")
    @mark.only
    def test_main(self, *, obj: _Object) -> None:
        s = serialize2(obj)
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

    @mark.skip
    def test_dataclass(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int | None = None

        obj = Example()
        ser = serialize2(obj)
        expected = serialize2({Example.__qualname__: {}})
        assert ser == expected

    def test_arbitrary_objects(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
        result = serialize2(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize2(sentinel)
