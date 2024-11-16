import datetime as dt
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum, auto, unique
from fractions import Fraction
from operator import eq
from time import sleep
from typing import Any, Literal, NamedTuple

from dacite import WrongTypeError
from hypothesis import HealthCheck, given, settings
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

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from utilities.functions import not_func
from utilities.hypothesis import (
    int64s,
    slices,
    temp_paths,
    text_ascii,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.orjson import (
    _SCHEMA_KEY,
    _SCHEMA_VALUE,
    DeserializeError,
    SerializeError,
    _is_serializable_binary,
    _is_serializable_fraction,
    _object_hook,
    _ObjectHookError,
    _SerializeInvalidBinaryError,
    _SerializeInvalidFractionError,
    _SerializeTypeError,
    deserialize,
    serialize,
)
from utilities.sentinel import sentinel
from utilities.timer import Timer
from utilities.typing import get_args
from utilities.zoneinfo import UTC, HongKong

_TrueOrFalseLit = Literal["true", "false"]


def _map_abs(obj: Any, /) -> Any:
    return abs(obj) if obj == 0.0 else obj


def _map_complex(obj: complex, /) -> complex:
    return complex(_map_abs(obj.real), _map_abs(obj.imag))


class TestSerializeAndDeserialize:
    @given(data=data())
    @mark.parametrize(
        ("elements", "two_way", "eq_obj_implies_eq_ser"),
        [
            param(binary().filter(_is_serializable_binary), True, True),
            param(booleans(), True, True),
            param(
                complex_numbers(allow_infinity=False, allow_nan=False).map(
                    _map_complex
                ),
                True,
                True,
            ),
            param(dates(), True, True),
            param(datetimes(), True, True),
            param(
                zoned_datetimes(
                    time_zone=sampled_from([HongKong, UTC, dt.UTC]), valid=True
                ),
                True,
                True,
                marks=SKIPIF_CI_AND_WINDOWS,
            ),
            param(
                decimals(allow_nan=False, allow_infinity=False).map(_map_abs),
                True,
                True,
            ),
            param(
                dictionaries(text_ascii(), int64s() | text_ascii(), max_size=3),
                True,
                True,
            ),
            param(
                dictionaries(int64s(), int64s() | text_ascii(), max_size=3), False, True
            ),
            param(
                floats(allow_nan=False, allow_infinity=False).map(_map_abs), True, True
            ),
            param(fractions().filter(_is_serializable_fraction), True, True),
            param(frozensets(int64s(), max_size=3), True, True),
            param(frozensets(text_ascii(), max_size=3), True, True),
            param(frozensets(int64s() | text_ascii(), max_size=3), True, False),
            param(ip_addresses(v=4), True, True),
            param(ip_addresses(v=6), True, True),
            param(lists(int64s(), max_size=3), True, True),
            param(lists(lists(int64s(), max_size=3), max_size=3), True, True),
            param(none(), True, True),
            param(sets(int64s(), max_size=3), True, True),
            param(sets(text_ascii(), max_size=3), True, True),
            param(sets(int64s() | text_ascii(), max_size=3), True, False),
            param(slices(integers(0, 10)), True, True),
            param(temp_paths(), True, True),
            param(text(), True, True),
            param(timedeltas_2w(), True, True),
            param(times(), True, True),
            param(tuples(int64s(), int64s()), False, True),
            param(uuids(), False, True),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        elements: SearchStrategy[Any],
        two_way: bool,
        eq_obj_implies_eq_ser: bool,
    ) -> None:
        self._run_tests(
            data, elements, two_way=two_way, eq_obj_implies_eq_ser=eq_obj_implies_eq_ser
        )

    @given(
        data=data(),
        date=dates(),
        int_=int64s(),
        local_datetime=datetimes(),
        text=text_ascii(),
    )
    def test_dataclasses(
        self,
        *,
        data: DataObject,
        date: dt.date,
        int_: int,
        local_datetime: dt.datetime,
        text: str,
    ) -> None:
        true_or_falses: tuple[_TrueOrFalseLit, ...] = get_args(_TrueOrFalseLit)
        true_or_false = data.draw(sampled_from(true_or_falses))

        @unique
        class Truth(Enum):
            true = auto()
            false = auto()

        truth = data.draw(sampled_from(Truth))

        @dataclass(kw_only=True, slots=True)
        class Inner:
            date: dt.date
            enum: Truth
            int_: int
            literal: _TrueOrFalseLit
            local_datetime: dt.datetime
            text: str

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner
            date: dt.date
            enum: Truth
            int_: int
            literal: _TrueOrFalseLit
            local_datetime: dt.datetime
            text: str

        obj = Outer(
            inner=Inner(
                date=date,
                enum=truth,
                int_=int_,
                literal=true_or_false,
                local_datetime=local_datetime,
                text=text,
            ),
            date=date,
            enum=truth,
            int_=int_,
            literal=true_or_false,
            local_datetime=local_datetime,
            text=text,
        )
        result = deserialize(serialize(obj), cls=Outer)
        assert result == obj

    @given(x=int64s())
    def test_dataclass_non_decorated_subclass(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Parent:
            x: int

        class Child(Parent): ...

        obj = Child(x=x)
        result = deserialize(serialize(obj), cls=Child)
        assert result == obj

    def test_dataclass_enum_subsets(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        TrueOnly = Literal[Truth.true]  # noqa: N806

        @dataclass(kw_only=True, slots=True)
        class Example:
            color: TrueOnly  # pyright: ignore[reportInvalidTypeForm]

        obj = Example(color=Truth.true)
        result = deserialize(serialize(obj), cls=Example, enum_subsets=[TrueOnly])
        assert result == obj

    def test_error_serialize(self) -> None:
        with raises(
            _SerializeTypeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize(sentinel)

    @given(binary=binary().filter(not_func(_is_serializable_binary)))
    def test_error_binary(self, *, binary: bytes) -> None:
        with raises(
            _SerializeInvalidBinaryError, match="Unable to serialize binary data .*"
        ):
            _ = serialize(binary)

    @given(fraction=fractions().filter(not_func(_is_serializable_fraction)))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_error_fraction(self, *, fraction: Fraction) -> None:
        with raises(
            _SerializeInvalidFractionError, match="Unable to serialize fraction .*"
        ):
            _ = serialize(fraction)

    def test_error_deserialize(self) -> None:
        obj = {_SCHEMA_KEY: "invalid", _SCHEMA_VALUE: "invalid"}
        ser = serialize(obj)
        with raises(
            DeserializeError,
            match=r"Unable to deserialize data 'invalid'; object hook failed on \{.*\}",
        ):
            _ = deserialize(ser)

    def test_error_dataclass_enum_subsets(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        TrueOnly = Literal[Truth.true]  # noqa: N806

        @dataclass(kw_only=True, slots=True)
        class Example:
            color: TrueOnly  # pyright: ignore[reportInvalidTypeForm]

        obj = Example(color=Truth.false)
        ser = serialize(obj)
        with raises(
            WrongTypeError,
            match='wrong value type for field "color" - should be "2" instead of value .* of type ".*"',
        ):
            _ = deserialize(ser, cls=Example, enum_subsets=[TrueOnly])

    @given(x=int64s())
    def test_named_tuple(self, *, x: int) -> None:
        class Example(NamedTuple):
            x: int

        obj = Example(x=x)
        result = deserialize(serialize(obj), cls=Example)
        assert result == obj

    def test_timer(self) -> None:
        with Timer() as timer:
            sleep(0.01)

        result = deserialize(serialize(timer))
        assert result == timer

    def test_arbitrary_objects(self) -> None:
        with raises(
            SerializeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize(sentinel)
        result = serialize(sentinel, fallback=True)
        expected = b'{"_k":"any","_v":"<sentinel>"}'
        assert result == expected

    def test_arbitrary_objects_alongside_regular_objects(self) -> None:
        obj = {"truth": True, "sentinel": sentinel}
        result = deserialize(serialize(obj, fallback=True))
        expected = {"truth": True, "sentinel": str(sentinel)}
        assert result == expected

    def test_dataclass_needing_forward_reference(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            mapping: Mapping[str, int] = field(default_factory=dict)

        obj = Example()
        result = deserialize(serialize(obj), cls=Example)
        assert result == obj

    def _run_tests(
        self,
        data: DataObject,
        elements: SearchStrategy[Any],
        /,
        *,
        two_way: bool = False,
        eq: Callable[[Any, Any], bool] = eq,
        eq_obj_implies_eq_ser: bool = False,
    ) -> None:
        x = data.draw(elements)
        ser_x = serialize(x)
        if two_way:
            deser_x = deserialize(ser_x)
            assert eq(deser_x, x)
        y = data.draw(elements)
        ser_y = serialize(y)
        if eq(x, y):
            if eq_obj_implies_eq_ser:
                assert ser_x == ser_y
        else:
            assert ser_x != ser_y


class TestObjectHook:
    def test_error(self) -> None:
        obj = {_SCHEMA_KEY: "invalid", _SCHEMA_VALUE: "invalid"}
        with raises(_ObjectHookError, match=r"Unable to cast to object: 'invalid'"):
            _ = _object_hook(obj)
