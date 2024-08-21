from __future__ import annotations

import datetime as dt
from json import dumps
from math import isnan
from operator import eq, neg
from typing import TYPE_CHECKING, Any

from tests.scripts import test_pypi_server
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    slices,
    sqlite_engines,
    temp_paths,
    text_ascii,
    timedeltas_2w,
)
from utilities.math import MAX_INT32, MAX_INT64, MIN_INT32, MIN_INT64
from utilities.sentinel import sentinel
from utilities.zoneinfo import HONG_KONG, UTC

if TYPE_CHECKING:
    from collections.abc import Callable
    from decimal import Decimal

    from sqlalchemy import Engine
from hypothesis import HealthCheck, given, reproduce_failure, settings
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
    lists,
    none,
    sampled_from,
    sets,
    text,
    times,
    tuples,
    uuids,
)
from pytest import mark, param, raises
from typing_extensions import override

from utilities.orjson import deserialize, serialize

if TYPE_CHECKING:
    from collections.abc import Callable


class TestSerializeAndDeserialize:
    @given(data=data())
    @mark.parametrize(
        ("elements", "two_way"),
        [
            param(booleans(), True),
            param(dates(), True),
            param(datetimes(), True),
            param(datetimes(timezones=sampled_from([HONG_KONG, UTC, dt.UTC])), True),
            param(dictionaries(text_ascii(), int64s(), max_size=3), True),
            param(fractions(min_value=10, max_value=10), True),
            param(ip_addresses(v=4), True),
            param(ip_addresses(v=6), True),
            param(lists(int64s(), max_size=3), True),
            param(none(), True),
            param(temp_paths(), True),
            param(text(), True),
            param(timedeltas_2w(), True),
            param(times(), True),
            param(uuids(), False),
        ],
    )
    def test_main(
        self, *, data: DataObject, elements: SearchStrategy[Any], two_way: bool
    ) -> None:
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y, two_way=two_way)

    @given(x=binary(), y=binary())
    @settings(suppress_health_check=[HealthCheck.filter_too_much])
    def test_binary(self, *, x: bytes, y: bytes) -> None:
        with assume_does_not_raise(UnicodeDecodeError):
            _ = x.decode()
            _ = y.decode()
        self._assert_standard(x, y, two_way=True)

    @given(
        x=complex_numbers(allow_infinity=False, allow_nan=False),
        y=complex_numbers(allow_infinity=False, allow_nan=False),
    )
    def test_complex(self, *, x: complex, y: complex) -> None:
        def eq(x: complex, y: complex, /) -> bool:
            return ((x.real == y.real) or (x.real == y.real == 0.0)) and (
                (x.imag == y.imag) or (x.imag == y.imag == 0.0)
            )

        self._assert_standard(x, y, eq=eq, two_way=True)

    @given(x=decimals(), y=decimals())
    def test_decimal(self, *, x: Decimal, y: Decimal) -> None:
        def eq(x: Decimal, y: Decimal, /) -> bool:
            x_nan, y_nan = x.is_nan(), y.is_nan()
            if x_nan and y_nan:
                return (x.is_qnan() == y.is_qnan()) and (x.is_signed() == y.is_signed())
            return (x_nan == y_nan) and (x == y)

        self._assert_standard(x, y, eq=eq)

    # @given(data=data(), n=integers(0, 10))
    # def test_dicts_sortable(self, *, data: DataObject, n: int) -> None:
    #     elements = dictionaries(
    #         text_ascii(), integers(0, 2 * n), min_size=n, max_size=n
    #     )
    #     x, y = data.draw(tuples(elements, elements))
    #     self._assert_standard(x, y)
    #
    # @given(data=data(), n=integers(2, 10))
    # def test_dicts_unsortable(self, *, data: DataObject, n: int) -> None:
    #     elements = dictionaries(
    #         integers(0, 2 * n) | text_ascii(min_size=1, max_size=1),
    #         integers(0, 2 * n),
    #         min_size=n,
    #         max_size=n,
    #     )
    #     x, y = data.draw(tuples(elements, elements))
    #     self._assert_unsortable_collection(x, y)

    @given(x=sqlite_engines(), y=sqlite_engines())
    def test_engines(self, *, x: Engine, y: Engine) -> None:
        def eq(x: Engine, y: Engine, /) -> bool:
            return x.url == y.url

        self._assert_standard(x, y, eq=eq)

    @given(
        x=floats(allow_nan=False, allow_infinity=False),
        y=floats(allow_nan=False, allow_infinity=False),
    )
    def test_floats(self, *, x: float, y: float) -> None:
        def eq(x: float, y: float, /) -> bool:
            return (x == y) or (x == y == 0.0)

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

    @given(data=data(), n=integers(0, 10))
    def test_slices(self, *, data: DataObject, n: int) -> None:
        elements = slices(n)
        x, y = data.draw(tuples(elements, elements))
        self._assert_standard(x, y, eq=eq)

    # @given(data=data(), n=integers(0, 3))
    # @mark.only
    # def test_tuples(self, *, data: DataObject, n: int) -> None:
    #     elements = tuples(*(n * [int64s()]))
    #     x, y = data.draw(tuples(elements, elements))
    #     self._assert_standard(x, y, eq=eq)

    def test_error(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize(sentinel)

    def _assert_standard(
        self,
        x: Any,
        y: Any,
        /,
        *,
        two_way: bool = False,
        eq: Callable[[Any, Any], bool] = eq,
    ) -> None:
        ser = serialize(x)
        if two_way:
            deser = deserialize(ser)
            assert eq(deser, x)
        res = ser == serialize(y)
        expected = eq(x, y)
        assert res is expected

    def _assert_unsortable_collection(self, x: Any, y: Any, /) -> None:
        ser_x = serialize(x)
        assert deserialize(ser_x) == x
        ser_y = serialize(y)
        if ser_x == ser_y:
            assert x == y
