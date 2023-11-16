from __future__ import annotations

from decimal import Decimal
from json import dumps
from math import isnan
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    binary,
    booleans,
    characters,
    complex_numbers,
    data,
    dates,
    datetimes,
    decimals,
    dictionaries,
    floats,
    fractions,
    frozensets,
    integers,
    ip_addresses,
    just,
    lists,
    none,
    sets,
    slices,
    text,
    timedeltas,
    times,
    tuples,
    uuids,
)
from pytest import mark, param, raises

from utilities.datetime import NOW_HKG, UTC
from utilities.hypothesis.hypothesis import assume_does_not_raise, temp_paths
from utilities.json import (
    _CLASS,
    _VALUE,
    InvalidTimeZoneError,
    JsonDeserializationError,
    JsonSerializationError,
    deserialize,
    serialize,
)


class TestSerialize:
    @given(data=data())
    @mark.parametrize(
        "strategy",
        [
            param(booleans()),
            param(characters()),
            param(dates()),
            param(datetimes(timezones=just(UTC) | none())),
            param(dictionaries(integers(), integers(), max_size=3)),
            param(fractions()),
            param(frozensets(integers(), max_size=3)),
            param(ip_addresses()),
            param(lists(integers(), max_size=3)),
            param(none()),
            param(sets(integers(), max_size=3)),
            param(temp_paths()),
            param(text()),
            param(timedeltas()),
            param(times()),
            param(uuids()),
        ],
    )
    def test_main(self, *, data: DataObject, strategy: SearchStrategy[Any]) -> None:
        x = data.draw(strategy)
        ser_x = serialize(x)
        assert deserialize(ser_x) == x
        y = data.draw(strategy)
        res = ser_x == serialize(y)
        expected = x == y
        assert res is expected

    @given(x=binary(), y=binary())
    def test_binary(self, *, x: bytes, y: bytes) -> None:
        with assume_does_not_raise(UnicodeDecodeError):
            ser_x = serialize(x)
        assert deserialize(ser_x) == x
        with assume_does_not_raise(UnicodeDecodeError):
            res = ser_x == serialize(y)
        expected = x == y
        assert res is expected

    @given(x=complex_numbers(), y=complex_numbers())
    def test_complex(self, *, x: complex, y: complex) -> None:
        ser_x = serialize(x)

        def eq(x: complex, y: complex, /) -> bool:
            return ((x.real == y.real) or (isnan(x.real) and isnan(y.real))) and (
                (x.imag == y.imag) or (isnan(x.imag) and isnan(y.imag))
            )

        assert eq(deserialize(ser_x), x)
        res = ser_x == serialize(y)
        expected = eq(x, y)
        assert res is expected

    @given(x=decimals(), y=decimals())
    def test_decimal(self, *, x: Decimal, y: Decimal) -> None:
        ser_x = serialize(x)

        def eq(x: Decimal, y: Decimal, /) -> bool:
            x_nan, y_nan = x.is_nan(), y.is_nan()
            if x_nan and y_nan:
                return (x.is_qnan() == y.is_qnan()) and (x.is_signed() == y.is_signed())
            return (x_nan == y_nan) and (x == y)

        assert eq(deserialize(ser_x), x)
        res = ser_x == serialize(y)
        expected = eq(x, y)
        assert res is expected

    @given(x=floats(), y=floats())
    def test_floats(self, *, x: float, y: float) -> None:
        ser_x = serialize(x)

        def eq(x: float, y: float, /) -> bool:
            return (x == y) or (isnan(x) and isnan(y))

        assert eq(deserialize(ser_x), x)
        res = ser_x == serialize(y)
        expected = eq(x, y)
        assert res is expected

    @given(data=data(), n=integers(0, 3))
    def test_slices(self, *, data: DataObject, n: int) -> None:
        x = data.draw(slices(n))
        ser_x = serialize(x)
        assert deserialize(ser_x) == x
        y = data.draw(slices(n))
        res = ser_x == serialize(y)
        expected = x == y
        assert res is expected

    @given(data=data(), n=integers(0, 3))
    def test_tuples(self, *, data: DataObject, n: int) -> None:
        elements = tuples(*(n * [integers()]))
        x = data.draw(elements)
        ser_x = serialize(x)
        assert deserialize(ser_x) == x
        y = data.draw(elements)
        res = ser_x == serialize(y)
        expected = x == y
        assert res is expected

    def test_timezone_error(self) -> None:
        with raises(InvalidTimeZoneError):
            _ = serialize(NOW_HKG)

    def test_serialization_error(self) -> None:
        class Example:
            pass

        with raises(JsonSerializationError):
            _ = serialize(Example())

    def test_deserializing_regular_dictionary(self) -> None:
        ser = dumps({"a": 1, "b": 2})
        _ = deserialize(ser)

    def test_deserialization_error(self) -> None:
        ser = dumps({_CLASS: "unknown", _VALUE: None})
        with raises(JsonDeserializationError):
            _ = deserialize(ser)
