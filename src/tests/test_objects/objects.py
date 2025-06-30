from __future__ import annotations

from enum import Enum, auto
from functools import partial
from typing import Any

import hypothesis.strategies
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
    dictionaries,
    floats,
    integers,
    just,
    lists,
    none,
    recursive,
    sampled_from,
    tuples,
    uuids,
)

from tests.test_typing_funcs.with_future import (
    DataClassFutureCustomEquality,
    DataClassFutureDefaultInInitChild,
    DataClassFutureInt,
    DataClassFutureIntDefault,
    DataClassFutureLiteral,
    DataClassFutureLiteralNullable,
    DataClassFutureNestedInnerFirstOuter,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
    DataClassFutureTypeLiteral,
    DataClassFutureTypeLiteralNullable,
)
from utilities.hypothesis import (
    assume_does_not_raise,
    date_deltas,
    date_time_deltas,
    dates,
    int64s,
    month_days,
    paths,
    plain_datetimes,
    py_datetimes,
    text_ascii,
    text_printable,
    time_deltas,
    times,
    versions,
    year_months,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64


def objects(
    *,
    dataclass_custom_equality: bool = False,
    dataclass_default_in_init_child: bool = False,
    dataclass_int: bool = False,
    dataclass_int_default: bool = False,
    dataclass_literal: bool = False,
    dataclass_literal_nullable: bool = False,
    dataclass_nested: bool = False,
    dataclass_none: bool = False,
    dataclass_type_literal: bool = False,
    dataclass_type_literal_nullable: bool = False,
    enum: bool = False,
    exception_class: bool = False,
    exception_instance: bool = False,
    extra_base: SearchStrategy[Any] | None = None,
    floats_allow_nan: bool = False,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
    all_: bool = False,
    parsable: bool = False,
    sortable: bool = False,
) -> SearchStrategy[Any]:
    base = (
        booleans()
        | dates()
        | hypothesis.strategies.dates()
        | py_datetimes(zoned=booleans())
        | floats(allow_nan=floats_allow_nan)
        | (int64s() if parsable else integers())
        | month_days()
        | none()
        | paths()
        | plain_datetimes()
        | text_printable().filter(lambda x: not x.startswith("["))
        | time_deltas()
        | times()
        | hypothesis.strategies.times()
        | uuids()
        | versions()
        | year_months()
        | zoned_datetimes()
    )
    if dataclass_custom_equality:
        base |= builds(DataClassFutureCustomEquality)
    if dataclass_default_in_init_child:
        base |= builds(DataClassFutureDefaultInInitChild)
    if dataclass_int:
        base |= builds(DataClassFutureInt).filter(lambda obj: _is_int64(obj.int_))
    if dataclass_int_default:
        base |= builds(DataClassFutureIntDefault).filter(
            lambda obj: _is_int64(obj.int_)
        )
    if dataclass_literal:
        base |= builds(DataClassFutureLiteral, truth=sampled_from(["true", "false"]))
    if dataclass_literal_nullable:
        base |= builds(
            DataClassFutureLiteralNullable,
            truth=sampled_from(["true", "false"]) | none(),
        )
    if dataclass_nested:
        base |= builds(DataClassFutureNestedInnerFirstOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        ) | builds(DataClassFutureNestedOuterFirstOuter).filter(
            lambda outer: _is_int64(outer.inner.int_)
        )
    if dataclass_none:
        base |= builds(DataClassFutureNone)
    if dataclass_type_literal:
        base |= builds(
            DataClassFutureTypeLiteral, truth=sampled_from(["true", "false"])
        )
    if dataclass_type_literal_nullable:
        base |= builds(
            DataClassFutureTypeLiteralNullable,
            truth=sampled_from(["true", "false"]) | none(),
        )
    if enum or all_:
        base |= sampled_from(TruthEnum)
    if exception_class or all_:
        base |= just(CustomError)
    if exception_instance or all_:
        base |= builds(CustomError, int64s())
    if extra_base is not None:
        base |= extra_base
    if not sortable:
        base |= date_deltas(parsable=parsable) | date_time_deltas(parsable=parsable)
    extend = partial(
        _extend,
        sub_frozenset=sub_frozenset,
        sub_list=sub_list,
        sub_set=sub_set,
        sub_tuple=sub_tuple,
        all_=all_,
    )
    return recursive(base, extend, max_leaves=10)


def _extend(
    strategy: SearchStrategy[Any],
    /,
    *,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
    all_: bool = False,
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
    if sub_frozenset or all_:
        extension |= frozensets.map(SubFrozenSet)
    if sub_list or all_:
        extension |= lists(strategy).map(SubList)
    if sub_set or all_:
        extension |= sets.map(SubSet)
    if sub_tuple or all_:
        extension |= tuples(strategy).map(SubTuple)
    return extension


def _is_int64(n: int, /) -> bool:
    return MIN_INT64 <= n <= MAX_INT64


def _into_set(elements: list[Any], /) -> set[Any]:
    with assume_does_not_raise(TypeError, match="unhashable type"):
        return set(elements)


class CustomError(Exception): ...


class SubFrozenSet(frozenset):
    pass


class SubList(list):
    pass


class SubSet(set):
    pass


class SubTuple(tuple):  # noqa: SLOT001
    pass


class TruthEnum(Enum):
    true = auto()
    false = auto()
