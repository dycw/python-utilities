from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from math import inf, nan
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from hypothesis import Phase, example, given, reproduce_failure, settings
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
    dates,
    datetimes,
    dictionaries,
    floats,
    integers,
    just,
    lists,
    none,
    recursive,
    sampled_from,
    sets,
    times,
    timezones,
    tuples,
    uuids,
)
from pytest import mark, param, raises
from typing_extensions import override

from tests.conftest import FLAKY, IS_CI_AND_WINDOWS
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    lists_fixed_length,
    text_ascii,
    text_printable,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.operator import _IsEqualUnsortableSetError, is_equal

if TYPE_CHECKING:
    from utilities.types import Number
    from utilities.typing import StrMapping


def base_objects(
    *,
    dataclass1: bool = False,
    dataclass2: bool = False,
    dataclass3: bool = False,
    dataclass4: bool = False,
    enum: bool = False,
    floats_min_value: Number | None = None,
    floats_max_value: Number | None = None,
    floats_allow_nan: bool | None = None,
    floats_allow_infinity: bool | None = None,
    ib_orders: bool = False,
    ib_trades: bool = False,
) -> SearchStrategy[Any]:
    base = (
        booleans()
        | floats(
            min_value=floats_min_value,
            max_value=floats_max_value,
            allow_nan=floats_allow_nan,
            allow_infinity=floats_allow_infinity,
        )
        | dates()
        | datetimes()
        | int64s()
        | text_ascii().map(Path)
        | text_printable()
        | times()
        | timedeltas_2w()
        | uuids()
    )
    if IS_CI_AND_WINDOWS:
        base |= zoned_datetimes()
    else:
        base |= zoned_datetimes(time_zone=timezones() | just(dt.UTC), valid=True)
    if dataclass1:
        base |= builds(DataClass1).filter(lambda obj: _is_int64(obj.x))
    if dataclass2:
        base |= builds(DataClass2Outer).filter(lambda outer: _is_int64(outer.inner.x))
    if dataclass3:
        base |= builds(DataClass3)
    if dataclass4:
        base |= builds(DataClass4)
    if enum:
        base |= sampled_from(TruthEnum)
    if ib_orders:
        from ib_async import Order

        base |= builds(Order)
    if ib_trades:
        from ib_async import Fill, Forex, Trade

        forexes = builds(Forex)
        fills = builds(Fill, contract=forexes)
        trades = builds(Trade, fills=lists(fills))
        base |= trades
    return base


def make_objects(
    *,
    dataclass1: bool = False,
    dataclass2: bool = False,
    dataclass3: bool = False,
    dataclass4: bool = False,
    enum: bool = False,
    floats_min_value: Number | None = None,
    floats_max_value: Number | None = None,
    floats_allow_nan: bool | None = None,
    floats_allow_infinity: bool | None = None,
    ib_orders: bool = False,
    ib_trades: bool = False,
    extra_base: SearchStrategy[Any] | None = None,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    base = base_objects(
        dataclass1=dataclass1,
        dataclass2=dataclass2,
        dataclass3=dataclass3,
        dataclass4=dataclass4,
        enum=enum,
        floats_min_value=floats_min_value,
        floats_max_value=floats_max_value,
        floats_allow_nan=floats_allow_nan,
        floats_allow_infinity=floats_allow_infinity,
        ib_orders=ib_orders,
        ib_trades=ib_trades,
    )
    if extra_base is not None:
        base |= extra_base
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
    lsts = lists(strategy)
    sets = lsts.map(_into_set)
    frozensets = lists(strategy).map(_into_set).map(frozenset)
    extension = (
        dictionaries(text_ascii(), strategy)
        | frozensets
        | lsts
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


def _is_int64(n: int, /) -> bool:
    return MIN_INT64 <= n <= MAX_INT64


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


@dataclass(kw_only=True, slots=True)
class DataClass4:
    x: int = 0

    @override
    def __eq__(self, other: object) -> bool:
        return self is other

    @override
    def __hash__(self) -> int:
        return id(self)


class TruthEnum(Enum):
    true = auto()
    false = auto()


# tests


class TestIsEqual:
    @given(
        obj=make_objects(
            dataclass1=True,
            dataclass2=True,
            dataclass3=True,
            dataclass4=True,
            ib_orders=True,
            ib_trades=True,
            sub_frozenset=True,
            sub_list=True,
            sub_set=True,
            sub_tuple=True,
        )
    )
    def test_one(self, *, obj: Any) -> None:
        with assume_does_not_raise(_IsEqualUnsortableSetError):
            assert is_equal(obj, obj)

    @given(
        objs=lists_fixed_length(
            make_objects(
                dataclass1=True,
                dataclass2=True,
                dataclass3=True,
                dataclass4=True,
                ib_orders=True,
                ib_trades=True,
                sub_frozenset=True,
                sub_list=True,
                sub_set=True,
                sub_tuple=True,
            ),
            2,
        ).map(tuple)
    )
    def test_two_objects(self, *, objs: tuple[Any, Any]) -> None:
        first, second = objs
        with assume_does_not_raise(_IsEqualUnsortableSetError):
            result = is_equal(first, second)
        assert isinstance(result, bool)

    @given(x=integers())
    def test_dataclass_4(self, *, x: int) -> None:
        first, second = DataClass4(x=x), DataClass4(x=x)
        assert first != second
        assert is_equal(first, second)

    @given(
        objs=lists_fixed_length(dictionaries(text_ascii(), make_objects()), 2).map(
            tuple
        )
    )
    def test_mappings(self, *, objs: tuple[StrMapping, StrMapping]) -> None:
        first, second = objs
        with assume_does_not_raise(_IsEqualUnsortableSetError):
            result = is_equal(first, second)
        assert isinstance(result, bool)

    @given(x=floats(), y=floats())
    # @example(x=inf, y=nan)
    @settings(max_examples=10000, phases={Phase.generate})
    def test_sets_of_floats(self, *, x: float, y: float) -> None:
        assert is_equal({x, y}, {y, x})

    @mark.only
    def test_weird_floats(self) -> None:
        # ii = [-4.233805663404397e32, -423380566340439711385424152756224.0, 41012, nan]
        # jj = [-4.233805663404397e32, -423380566340439711385424152756224.0, 41012, nan]
        # assert is_equal(set(ii), set(jj))

        # aa = {41012, -4.233805663404397e32, nan}
        # bb = {nan, 41012, -4.233805663404397e32}
        # assert is_equal(aa, bb)

        k = -4.233805663404397e32
        assert is_equal({k, nan}, {nan, k})

    @given(obj=sets(integers() | none(), min_size=2).filter(lambda x: None in x))
    def test_error_unsortable(self, *, obj: set[int | None]) -> None:
        with raises(
            _IsEqualUnsortableSetError,
            match=r"Unsortable collection\(s\): .*, .*",
        ):
            _ = is_equal(obj, obj)
