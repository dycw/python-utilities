import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from fractions import Fraction
from operator import eq
from typing import Any

from dacite import from_dict
from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    binary,
    booleans,
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
from sqlalchemy import Engine

from utilities.hypothesis import (
    int64s,
    slices,
    sqlite_engines,
    temp_paths,
    text_ascii,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.orjson import deserialize, serialize
from utilities.sentinel import sentinel
from utilities.zoneinfo import HONG_KONG, UTC


def _filter_binary(obj: bytes, /) -> bool:
    try:
        _ = obj.decode()
    except UnicodeDecodeError:
        return False
    return True


def _filter_fraction(obj: Fraction, /) -> bool:
    return (MIN_INT64 <= obj.numerator <= MAX_INT64) and (
        MIN_INT64 <= obj.denominator <= MAX_INT64
    )


def _map_abs(obj: Any, /) -> Any:
    return abs(obj) if obj == 0.0 else obj


def _map_complex(obj: complex, /) -> complex:
    return complex(_map_abs(obj.real), _map_abs(obj.imag))


class TestSerializeAndDeserialize:
    @given(data=data())
    @mark.parametrize(
        ("elements", "two_way"),
        [
            param(binary().filter(_filter_binary), True),
            param(booleans(), True),
            param(
                complex_numbers(allow_infinity=False, allow_nan=False).map(
                    _map_complex
                ),
                True,
            ),
            param(dates(), True),
            param(datetimes(), True),
            param(
                zoned_datetimes(time_zone=sampled_from([HONG_KONG, UTC, dt.UTC])), True
            ),
            param(decimals(allow_nan=False, allow_infinity=False).map(_map_abs), True),
            param(dictionaries(text_ascii(), int64s(), max_size=3), True),
            param(floats(allow_nan=False, allow_infinity=False).map(_map_abs), True),
            param(fractions().filter(_filter_fraction), True),
            param(ip_addresses(v=4), True),
            param(ip_addresses(v=6), True),
            param(lists(int64s(), max_size=3), True),
            param(none(), True),
            param(slices(integers(0, 10)), True),
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
        self._run_tests(x, y, two_way=two_way, eq_obj_implies_eq_ser=True)

    @given(
        date=dates(),
        int_=int64s(),
        local_datetime=datetimes(),
        text=text_ascii(),
        zoned_datetime=zoned_datetimes(
            time_zone=sampled_from([HONG_KONG, UTC, dt.UTC])
        ),
    )
    def test_dataclasses(
        self,
        *,
        date: dt.date,
        int_: int,
        local_datetime: dt.datetime,
        text: str,
        zoned_datetime: dt.datetime,
    ) -> None:
        @dataclass(kw_only=True)
        class Inner:
            date: dt.date
            int_: int
            local_datetime: dt.datetime
            text: str
            zoned_datetime: dt.datetime

        @dataclass(kw_only=True)
        class Outer:
            inner: Inner
            date: dt.date
            int_: int
            local_datetime: dt.datetime
            text: str
            zoned_datetime: dt.datetime

        obj = Outer(
            inner=Inner(
                date=date,
                int_=int_,
                local_datetime=local_datetime,
                text=text,
                zoned_datetime=zoned_datetime,
            ),
            date=date,
            int_=int_,
            local_datetime=local_datetime,
            text=text,
            zoned_datetime=zoned_datetime,
        )
        data = deserialize(serialize(obj))
        result = from_dict(Outer, data)
        assert result == obj

    @given(x=sqlite_engines(), y=sqlite_engines())
    def test_engines(self, *, x: Engine, y: Engine) -> None:
        def eq(x: Engine, y: Engine, /) -> bool:
            return x.url == y.url

        self._run_tests(x, y, eq=eq, two_way=True, eq_obj_implies_eq_ser=True)

    @given(data=data(), n=integers(0, 10))
    @mark.parametrize("outer_elements", [param(frozensets), param(sets)])
    @mark.parametrize(
        ("inner_elements", "eq_obj_implies_eq_ser"),
        [param(int64s(), True), param(int64s() | text_ascii(), False)],
    )
    def test_sets_and_frozensets(
        self,
        *,
        data: DataObject,
        n: int,
        outer_elements: Callable[..., SearchStrategy[Any]],
        inner_elements: Callable[..., SearchStrategy[Any]],
        eq_obj_implies_eq_ser: bool,
    ) -> None:
        elements = outer_elements(inner_elements, min_size=n, max_size=n)
        x, y = data.draw(tuples(elements, elements))
        self._run_tests(x, y, two_way=True, eq_obj_implies_eq_ser=eq_obj_implies_eq_ser)

    @given(data=data(), n=integers(0, 3))
    def test_tuples(self, *, data: DataObject, n: int) -> None:
        elements = tuples(*(n * [int64s()]))
        x, y = data.draw(tuples(elements, elements))
        self._run_tests(x, y, eq=eq, two_way=False)

    def test_error(self) -> None:
        with raises(TypeError, match="Type is not JSON serializable: Sentinel"):
            _ = serialize(sentinel)

    def _run_tests(
        self,
        x: Any,
        y: Any,
        /,
        *,
        two_way: bool = False,
        eq: Callable[[Any, Any], bool] = eq,
        eq_obj_implies_eq_ser: bool = False,
    ) -> None:
        ser_x = serialize(x)
        if two_way:
            deser_x = deserialize(ser_x)
            assert eq(deser_x, x)
        ser_y = serialize(y)
        if eq(x, y):
            if eq_obj_implies_eq_ser:
                assert ser_x == ser_y
        else:
            assert ser_x != ser_y
