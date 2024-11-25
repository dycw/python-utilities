from __future__ import annotations

import datetime as dt
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from math import isinf, isnan
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from hypothesis import HealthCheck, given, settings
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
    just,
    lists,
    recursive,
    sampled_from,
    times,
    timezones,
    tuples,
)
from pytest import mark, param, raises

from tests.conftest import IS_CI_AND_WINDOWS
from utilities.dataclasses import asdict_without_defaults, is_dataclass_instance
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    settings_with_reduced_examples,
    text_ascii,
    text_printable,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.orjson import (
    _DeserializeNoObjectsError,
    _DeserializeObjectNotFoundError,
    _SerializeIntegerError,
    _SerializeTypeError,
    deserialize,
    serialize,
)
from utilities.sentinel import sentinel
from utilities.typing import get_args
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from utilities.dataclasses import Dataclass
    from utilities.types import StrMapping


# strategies


def objects(
    *,
    dataclass1: bool = False,
    dataclass2: bool = False,
    dataclass3: bool = False,
    enum: bool = False,
    ib_trades: bool = False,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    base = (
        booleans()
        | floats(allow_nan=False, allow_infinity=False)
        | dates()
        | datetimes()
        | int64s()
        | sampled_from(get_args(TruthLit))
        | text_ascii().map(Path)
        | text_printable()
        | times()
        | timedeltas_2w()
    )
    if IS_CI_AND_WINDOWS:
        base |= zoned_datetimes()
    else:
        base |= zoned_datetimes(time_zone=timezones() | just(dt.UTC), valid=True)
    if dataclass1:
        base |= builds(DataClass1)
    if dataclass2:
        base |= builds(DataClass2Outer)
    if dataclass3:
        base |= builds(DataClass3)
    if enum:
        base |= sampled_from(TruthEnum)
    if ib_trades:
        from ib_async import Fill, Forex, Trade

        forexes = builds(Forex)
        fills = builds(Fill, contract=forexes)
        base |= builds(Trade, fills=lists(fills))
    return recursive(
        base,
        partial(
            _extend,
            sub_frozenset=sub_frozenset,
            sub_list=sub_list,
            sub_set=sub_set,
            sub_tuple=sub_tuple,
        ),
    )


def _extend(
    strategy: SearchStrategy[Any],
    /,
    *,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    sets = lists(strategy).map(_into_set)
    frozensets = lists(strategy).map(_into_set).map(frozenset)
    extension = (
        dictionaries(text_ascii(), strategy)
        | frozensets
        | lists(strategy)
        | sets
        | tuples(strategy)
    )
    if sub_frozenset:
        extension |= frozensets.map(SubFrozenSet)
    if sub_list:
        extension |= lists(strategy).map(SubList)
    if sub_set:
        extension |= sets.map(SubSet)
    if sub_tuple:
        extension |= tuples(strategy).map(SubTuple)
    return extension


def _into_set(elements: list[Any], /) -> set[Any]:
    with assume_does_not_raise(TypeError, match="unhashable type"):
        return set(elements)


class SubFrozenSet(frozenset):
    pass


class SubList(list):
    pass


class SubSet(set):
    pass


class SubTuple(tuple):  # noqa: SLOT001
    pass


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass1:
    x: int = 0


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Inner:
    x: int = 0


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass2Outer:
    inner: DataClass2Inner


@dataclass(unsafe_hash=True, kw_only=True, slots=True)
class DataClass3:
    truth: Literal["true", "false"]


class TruthEnum(Enum):
    true = auto()
    false = auto()


TruthLit = Literal["true", "false"]


class TestSerializeAndDeserialize:
    @given(obj=objects())
    def test_main(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj))
        assert result == obj

    @given(obj=objects(dataclass1=True))
    def test_dataclass(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClass1})
        assert result == obj

    @given(obj=objects(dataclass2=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_nested(self, *, obj: Any) -> None:
        with assume_does_not_raise(_SerializeIntegerError):
            ser = serialize(obj)
        result = deserialize(ser, objects={DataClass2Inner, DataClass2Outer})
        assert result == obj

    @given(obj=objects(dataclass3=True))
    @settings(suppress_health_check={HealthCheck.filter_too_much})
    def test_dataclass_lit(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClass3})
        assert result == obj

    @given(obj=builds(DataClass1))
    def test_dataclass_no_objects_error(self, *, obj: DataClass1) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeNoObjectsError,
            match="Objects required to deserialize '.*' from .*",
        ):
            _ = deserialize(ser)

    @given(obj=builds(DataClass1))
    def test_dataclass_empty_error(self, *, obj: DataClass1) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeObjectNotFoundError,
            match=r"Unable to find object to deserialize '.*' from .*",
        ):
            _ = deserialize(ser, objects=set())

    @given(obj=objects(enum=True))
    def test_enum(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={TruthEnum})
        assert result == obj

    @given(obj=objects(sub_frozenset=True))
    def test_sub_frozenset(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubFrozenSet})
        assert result == obj

    @given(obj=objects(sub_list=True))
    def test_sub_list(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubList})
        assert result == obj

    @given(obj=objects(sub_set=True))
    def test_sub_set(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubSet})
        assert result == obj

    @given(obj=objects(sub_tuple=True))
    def test_sub_tuple(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubTuple})
        assert result == obj

    @mark.parametrize(
        ("utc", "expected"),
        [
            param(UTC, b'"[dt]2000-01-01T00:00:00+00:00[UTC]"'),
            param(dt.UTC, b'"[dt]2000-01-01T00:00:00+00:00[dt.UTC]"'),
        ],
        ids=str,
    )
    def test_utc(self, *, utc: dt.tzinfo, expected: bytes) -> None:
        datetime = dt.datetime(2000, 1, 1, tzinfo=utc)
        ser = serialize(datetime)
        assert ser == expected
        result = deserialize(ser)
        assert result == datetime
        assert result.tzinfo is utc


class TestSerialize:
    @given(text=text_printable())
    def test_before(self, *, text: str) -> None:
        result = serialize(text, before=str.upper)
        expected = serialize(text.upper())
        assert result == expected

    def test_dataclass(self) -> None:
        obj = DataClass1()
        result = serialize(obj)
        expected = b'{"[dc|DataClass1]":{}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        obj = DataClass2Outer(inner=DataClass2Inner(x=0))
        result = serialize(obj)
        expected = b'{"[dc|DataClass2Outer]":{"inner":{"[dc|DataClass2Inner]":{}}}}'
        assert result == expected

    def test_dataclass_hook_main(self) -> None:
        obj = DataClass1()

        def hook(_: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            return {k: v for k, v in mapping.items() if v >= 0}

        result = serialize(obj, dataclass_final_hook=hook)
        expected = b'{"[dc|DataClass1]":{}}'
        assert result == expected

    @given(x=sampled_from([MIN_INT64 - 1, MAX_INT64 + 1]))
    def test_pre_process(self, *, x: int) -> None:
        with raises(_SerializeIntegerError, match="Integer .* is out of range"):
            _ = serialize(x)

    @given(data=data())
    @settings_with_reduced_examples(suppress_health_check={HealthCheck.filter_too_much})
    def test_ib_trades(self, *, data: DataObject) -> None:
        from ib_async import (
            ComboLeg,
            CommissionReport,
            Contract,
            DeltaNeutralContract,
            Execution,
            Fill,
            Forex,
            Order,
            Trade,
        )

        obj = data.draw(objects(ib_trades=True))  # put inside test, due to imports

        def hook(cls: type[Any], mapping: StrMapping, /) -> Any:
            if issubclass(cls, Contract) and not issubclass(Contract, cls):
                mapping = {k: v for k, v in mapping.items() if k != "secType"}
            return mapping

        with assume_does_not_raise(_SerializeIntegerError):
            ser = serialize(obj, dataclass_final_hook=hook)
        result = deserialize(
            ser,
            objects={
                CommissionReport,
                ComboLeg,
                Contract,
                DeltaNeutralContract,
                Execution,
                Fill,
                Forex,
                Order,
                Trade,
            },
        )

        def unpack(obj: Any, /) -> Any:
            if isinstance(obj, list | tuple):
                return list(map(unpack, obj))
            if isinstance(obj, dict):
                return {k: unpack(v) for k, v in obj.items()}
            if is_dataclass_instance(obj):
                return unpack(asdict_without_defaults(obj))
            with suppress(TypeError):
                if isinf(obj) or isnan(obj):
                    return None
            return obj

        def eq(x: Any, y: Any) -> Any:
            if isinstance(x, list) and isinstance(y, list):
                return all(eq(x_i, y_i) for x_i, y_i in zip(x, y, strict=True))
            if isinstance(x, dict) and isinstance(y, dict):
                return (set(x) == set(y)) and all(eq(x[i], y[i]) for i in x)
            if is_dataclass_instance(x) and is_dataclass_instance(y):
                return eq(unpack(x), unpack(y))
            return x == y

        ur, uo = unpack(result), unpack(obj)
        assert eq(ur, uo)

    def test_fallback(self) -> None:
        with raises(
            _SerializeTypeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize(sentinel)
        result = serialize(sentinel, fallback=True)
        expected = b'"<sentinel>"'
        assert result == expected

    def test_error_serialize(self) -> None:
        with raises(
            _SerializeTypeError, match="Unable to serialize object of type 'Sentinel'"
        ):
            _ = serialize(sentinel)
