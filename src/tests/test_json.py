from __future__ import annotations

from json import dumps
from math import isnan
from operator import eq, neg
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, given, settings
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
from typing_extensions import override

from utilities.datetime import NOW_HK
from utilities.hypothesis import (
    assume_does_not_raise,
    sqlite_engines,
    temp_paths,
    text_ascii,
)
from utilities.json import (
    _CLASS,
    _VALUE,
    JsonDeserializationError,
    JsonSerializationError,
    deserialize,
    serialize,
)
from utilities.sentinel import sentinel
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Callable
    from decimal import Decimal

    from sqlalchemy import Engine


class TestSerializeAndDeserialize:
    @given(data=data())
    @mark.parametrize(
        "elements",
        [
            param(booleans()),
            param(characters()),
            param(dates()),
            param(datetimes(timezones=just(UTC) | none())),
            param(fractions()),
            param(ip_addresses(v=4)),
            param(ip_addresses(v=6)),
            param(lists(integers(), max_size=3)),
            param(none()),
            param(temp_paths()),
            param(text()),
            param(timedeltas()),
            param(times()),
            param(uuids()),
        ],
    )
    def test_main(self, *, data: DataObject, elements: SearchStrategy[Any]) -> None:
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y)

    @given(x=binary(), y=binary())
    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    def test_binary(self, *, x: bytes, y: bytes) -> None:
        with assume_does_not_raise(UnicodeDecodeError):
            _ = list(map(serialize, [x, y]))
        self._assert_standard(x, y)

    @given(x=complex_numbers(), y=complex_numbers())
    def test_complex(self, *, x: complex, y: complex) -> None:
        def eq(x: complex, y: complex, /) -> bool:
            return ((x.real == y.real) or (isnan(x.real) and isnan(y.real))) and (
                (x.imag == y.imag) or (isnan(x.imag) and isnan(y.imag))
            )

        self._assert_standard(x, y, eq=eq)

    @given(x=decimals(), y=decimals())
    def test_decimal(self, *, x: Decimal, y: Decimal) -> None:
        def eq(x: Decimal, y: Decimal, /) -> bool:
            x_nan, y_nan = x.is_nan(), y.is_nan()
            if x_nan and y_nan:
                return (x.is_qnan() == y.is_qnan()) and (x.is_signed() == y.is_signed())
            return (x_nan == y_nan) and (x == y)

        self._assert_standard(x, y, eq=eq)

    @given(data=data(), n=integers(0, 10))
    def test_dicts_sortable(self, *, data: DataObject, n: int) -> None:
        elements = dictionaries(
            integers(0, 2 * n), integers(0, 2 * n), min_size=n, max_size=n
        )
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y)

    @given(data=data(), n=integers(2, 10))
    def test_dicts_unsortable(self, *, data: DataObject, n: int) -> None:
        elements = dictionaries(
            integers(0, 2 * n) | text_ascii(min_size=1, max_size=1),
            integers(0, 2 * n),
            min_size=n,
            max_size=n,
        )
        x, y = data.draw(tuples(elements, elements))
        self._assert_unsortable_collection(x, y)

    @given(x=sqlite_engines(), y=sqlite_engines())
    def test_engines(self, *, x: Engine, y: Engine) -> None:
        def eq(x: Engine, y: Engine, /) -> bool:
            return x.url == y.url

        self._assert_standard(x, y, eq=eq)

    @given(x=floats(), y=floats())
    def test_floats(self, *, x: float, y: float) -> None:
        def eq(x: float, y: float, /) -> bool:
            return (x == y) or (isnan(x) and isnan(y))

        self._assert_standard(x, y, eq=eq)

    @given(data=data(), n=integers(0, 3))
    def test_slices(self, *, data: DataObject, n: int) -> None:
        elements = slices(n)
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y, eq=eq)

    @given(data=data(), n=integers(0, 10))
    @mark.parametrize("strategy", [param(frozensets), param(sets)])
    def test_sets_sortable(
        self, *, data: DataObject, n: int, strategy: Callable[..., SearchStrategy[int]]
    ) -> None:
        elements = strategy(integers(0, 2 * n), min_size=n, max_size=n)
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y, eq=eq)

    @given(data=data(), n=integers(2, 10))
    @mark.parametrize("strategy", [param(frozensets), param(sets)])
    def test_sets_unsortable(
        self,
        *,
        data: DataObject,
        n: int,
        strategy: Callable[..., SearchStrategy[int | str]],
    ) -> None:
        elements = strategy(
            integers(0, 2 * n) | text_ascii(min_size=1, max_size=1),
            min_size=n,
            max_size=n,
        )
        x, y = data.draw(tuples(elements, elements))
        self._assert_unsortable_collection(x, y)

    @given(data=data(), n=integers(0, 3))
    def test_tuples(self, *, data: DataObject, n: int) -> None:
        elements = tuples(*(n * [integers(0, 2 * n)]))
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y, eq=eq)

    @given(m=integers(), n=integers())
    def test_extra(self, *, m: int, n: int) -> None:
        class Example:
            def __init__(self, n: int, /) -> None:
                super().__init__()
                self.n = n

            @override
            def __eq__(self, other: object) -> bool:
                return isinstance(other, Example) and (self.n == other.n)

        def f_ser(obj: Example, /) -> int:
            return obj.n

        extra_ser = {Example: ("example", f_ser)}

        def f_des(n: int, /) -> Example:
            return Example(n)

        extra_des = {"example": f_des}
        x = Example(m)
        ser_x = serialize(x, extra=extra_ser)
        assert deserialize(ser_x, extra=extra_des) == x
        y = Example(n)
        res = ser_x == serialize(y, extra=extra_ser)
        expected = x == y
        assert res is expected

    def test_error_timezone(self) -> None:
        with raises(JsonSerializationError, match="Invalid timezone: Asia/Hong_Kong"):
            _ = serialize(NOW_HK)

    def test_error(self) -> None:
        with raises(JsonSerializationError, match=r"Unsupported type: Sentinel"):
            _ = serialize(sentinel)

    def _assert_standard(
        self, x: Any, y: Any, /, *, eq: Callable[[Any, Any], bool] = eq
    ) -> None:
        ser_x = serialize(x)
        assert eq(deserialize(ser_x), x)
        res = ser_x == serialize(y)
        expected = eq(x, y)
        assert res is expected

    def _assert_unsortable_collection(self, x: Any, y: Any, /) -> None:
        ser_x = serialize(x)
        assert deserialize(ser_x) == x
        ser_y = serialize(y)
        if ser_x == ser_y:
            assert x == y


class TestSerialize:
    def test_error_despite_extra(self) -> None:
        class Example1: ...

        x = Example1()

        class Example2: ...

        extra = {Example2: ("example", neg)}
        with raises(JsonSerializationError, match="Unsupported type: Example1"):
            _ = serialize(x, extra=extra)


class TestDeserialization:
    @given(obj=dictionaries(text_ascii(), integers()))
    def test_regular_dictionary(self, *, obj: dict[str, int]) -> None:
        ser = dumps(obj)
        assert deserialize(ser) == obj

    def test_error_unknown_class(self) -> None:
        ser = dumps({_CLASS: "unknown", _VALUE: None})
        with raises(
            JsonDeserializationError, match="Unsupported type: unknown; value was None"
        ):
            _ = deserialize(ser)

    @given(n=integers())
    def test_error_despite_extra(self, *, n: int) -> None:
        class Example:
            def __init__(self, n: int, /) -> None:
                super().__init__()
                self.n = n

        def f_ser(obj: Example, /) -> int:
            return obj.n

        x = Example(n)
        extra_ser = {Example: ("example", f_ser)}
        ser = serialize(x, extra=extra_ser)
        extra_des = {"wrong": neg}
        with raises(
            JsonDeserializationError,
            match="Unsupported type: example; extras were {'wrong'}",
        ):
            _ = deserialize(ser, extra=extra_des)
