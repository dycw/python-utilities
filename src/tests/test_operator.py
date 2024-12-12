from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    SearchStrategy,
    booleans,
    builds,
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
    uuids,
)
from pytest import mark

from tests.conftest import IS_CI_AND_WINDOWS
from utilities.hypothesis import (
    assume_does_not_raise,
    int64s,
    text_ascii,
    text_printable,
    timedeltas_2w,
    zoned_datetimes,
)
from utilities.math import MAX_INT64, MIN_INT64
from utilities.operator import _IsEqualUnsortableCollectionsError, is_equal
from utilities.orjson import deserialize, serialize


def objects(
    *,
    dataclass1: bool = False,
    dataclass2: bool = False,
    enum: bool = False,
    sub_frozenset: bool = False,
    sub_list: bool = False,
    sub_set: bool = False,
    sub_tuple: bool = False,
) -> SearchStrategy[Any]:
    base = (
        booleans()
        | floats()
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
    if enum:
        base |= sampled_from(TruthEnum)
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


class TruthEnum(Enum):
    true = auto()
    false = auto()


class TestIsEqual:
    @given(obj=objects())
    @mark.only
    def test_main(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj))
        with assume_does_not_raise(_IsEqualUnsortableCollectionsError):
            assert is_equal(result, obj)
