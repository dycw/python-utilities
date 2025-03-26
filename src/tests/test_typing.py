from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import NoneType
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Self
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import DataObject, data, just, none, sampled_from
from pytest import mark, param

from tests.test_typing_funcs.no_future import (
    DataClassNoFutureNestedInnerFirstInner,
    DataClassNoFutureNestedInnerFirstOuter,
    DataClassNoFutureNestedOuterFirstInner,
    DataClassNoFutureNestedOuterFirstOuter,
)
from tests.test_typing_funcs.with_future import (
    DataClassFutureDate,
    DataClassFutureInt,
    DataClassFutureIntNullable,
    DataClassFutureListInts,
    DataClassFutureLiteral,
    DataClassFutureNestedInnerFirstInner,
    DataClassFutureNestedInnerFirstOuter,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
    DataClassFuturePath,
    DataClassFutureSentinel,
    DataClassFutureStr,
    DataClassFutureTimeDelta,
    DataClassFutureUUID,
)
from utilities.sentinel import Sentinel
from utilities.types import LogLevel, Parallelism
from utilities.typing import (
    contains_self,
    get_args,
    get_literal_elements,
    get_type_hints,
    is_dict_type,
    is_frozenset_type,
    is_list_type,
    is_literal_type,
    is_mapping_type,
    is_namedtuple_class,
    is_namedtuple_instance,
    is_optional_type,
    is_sequence_type,
    is_set_type,
    is_union_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestContainsSelf:
    @mark.parametrize("obj", [param(Self), param(Self | None)])
    def test_main(self, *, obj: Any) -> None:
        assert contains_self(obj)


class TestGetArgs:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(dict[int, int], (int, int)),
            param(frozenset[int], (int,)),
            param(int | None, (int,)),
            param(int | str, (int, str)),
            param(list[int], (int,)),
            param(Literal["a", "b", "c"], ("a", "b", "c")),
            param(Mapping[int, int], (int, int)),
            param(Sequence[int], (int,)),
            param(set[int], (int,)),
            param(LogLevel, ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")),
            param(Parallelism, ("processes", "threads")),
        ],
    )
    def test_main(self, *, obj: Any, expected: tuple[Any, ...]) -> None:
        result = get_args(obj)
        assert result == expected


type _PlusOrMinusOneLit = Literal[1, -1]
type _TruthLit = Literal["true", "false"]
type _True = Literal["true"]
type _False = Literal["false"]
type _TrueAndFalse = _True | _False
type _TruthAndTrueAndFalse = _True | _TrueAndFalse


class TestGetLiteralElements:
    @given(
        case=sampled_from([
            (_PlusOrMinusOneLit, [1, -1]),
            (_TruthLit, ["true", "false"]),
            (_TrueAndFalse, ["true", "false"]),
            (_TruthAndTrueAndFalse, ["true", "false"]),
        ])
    )
    def test_main(self, *, case: tuple[Any, list[Any]]) -> None:
        obj, expected = case
        result = get_literal_elements(obj)
        assert result == expected


class TestGetTypeHints:
    @given(data=data())
    def test_date(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            date: dt.date

        cls = data.draw(sampled_from([Example, DataClassFutureDate]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"date": dt.date}
        assert hints == expected

    @given(data=data())
    def test_int(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            int_: int

        cls = data.draw(sampled_from([Example, DataClassFutureInt]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"int_": int}
        assert hints == expected

    @given(data=data())
    def test_int_nullable(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            int_: int | None = None

        cls = data.draw(sampled_from([Example, DataClassFutureIntNullable]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"int_": int | None}
        assert hints == expected

    @given(data=data())
    def test_list(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            ints: list[int]

        cls = data.draw(sampled_from([Example, DataClassFutureListInts]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"ints": list[int]}
        assert hints == expected

    def test_nested_local(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        @dataclass(kw_only=True, slots=True)
        class Inner:
            int_: int

        hints = get_type_hints(Outer, localns=locals())
        expected = {"inner": Inner}
        assert hints == expected

    def test_nested_no_future_inner_then_outer(self) -> None:
        hints = get_type_hints(
            DataClassNoFutureNestedInnerFirstOuter, globalns=globals()
        )
        expected = {"inner": DataClassNoFutureNestedInnerFirstInner}
        assert hints == expected

    def test_nested_no_future_outer_then_inner(self) -> None:
        hints = get_type_hints(
            DataClassNoFutureNestedOuterFirstOuter, globalns=globals()
        )
        expected = {"inner": DataClassNoFutureNestedOuterFirstInner}
        assert hints == expected

    def test_nested_with_future_inner_then_outer(self) -> None:
        hints = get_type_hints(DataClassFutureNestedInnerFirstOuter, globalns=globals())
        expected = {"inner": DataClassFutureNestedInnerFirstInner}
        assert hints == expected

    def test_nested_with_future_outer_then_inner(self) -> None:
        hints = get_type_hints(DataClassFutureNestedOuterFirstOuter, globalns=globals())
        expected = {"inner": DataClassFutureNestedOuterFirstInner}
        assert hints == expected

    @given(data=data())
    def test_literal(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: Literal["true", "false"]

        cls = data.draw(sampled_from([Example, DataClassFutureLiteral]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"truth": Literal["true", "false"]}
        assert hints == expected

    @given(data=data())
    def test_none(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            none: None

        cls = data.draw(sampled_from([Example, DataClassFutureNone]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"none": NoneType}
        assert hints == expected

    @given(data=data())
    def test_path(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            path: Path

        cls = data.draw(sampled_from([Example, DataClassFuturePath]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"path": Path}
        assert hints == expected

    @given(data=data())
    def test_sentinel(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            sentinel: Sentinel

        cls = data.draw(sampled_from([Example, DataClassFutureSentinel]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"sentinel": Sentinel}
        assert hints == expected

    @given(data=data())
    def test_str(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            str_: str

        cls = data.draw(sampled_from([Example, DataClassFutureStr]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"str_": str}
        assert hints == expected

    @given(data=data())
    def test_timedelta(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            timedelta: dt.timedelta

        cls = data.draw(sampled_from([Example, DataClassFutureTimeDelta]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"timedelta": dt.timedelta}
        assert hints == expected

    @given(data=data())
    def test_uuid(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            uuid: UUID

        cls = data.draw(sampled_from([Example, DataClassFutureUUID]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"uuid": UUID}
        assert hints == expected

    def test_unresolved(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        @dataclass(kw_only=True, slots=True)
        class Inner:
            int_: int

        hints = get_type_hints(Outer)
        expected = {"inner": "Inner"}
        assert hints == expected


class TestIsAnnotationOfType:
    @mark.parametrize(
        ("func", "obj", "expected"),
        [
            param(is_dict_type, dict[int, int], True),
            param(is_dict_type, list[int], False),
            param(is_frozenset_type, frozenset[int], True),
            param(is_frozenset_type, list[int], False),
            param(is_list_type, list[int], True),
            param(is_list_type, set[int], False),
            param(is_mapping_type, Mapping[int, int], True),
            param(is_mapping_type, list[int], False),
            param(is_literal_type, Literal["a", "b", "c"], True),
            param(is_literal_type, list[int], False),
            param(is_optional_type, int | None, True),
            param(is_optional_type, int | str, False),
            param(is_optional_type, list[int], False),
            param(is_optional_type, list[int] | None, True),
            param(is_optional_type, Literal["a", "b", "c"], False),
            param(is_optional_type, Literal["a", "b", "c"] | None, True),
            param(is_sequence_type, Sequence[int], True),
            param(is_sequence_type, list[int], False),
            param(is_set_type, list[int], False),
            param(is_union_type, int | str, True),
            param(is_union_type, list[int], False),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        assert func(obj) is expected


class TestIsNamedTuple:
    def test_main(self) -> None:
        class Example(NamedTuple):
            x: int

        assert is_namedtuple_class(Example)
        assert is_namedtuple_instance(Example(x=0))

    def test_class(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        assert not is_namedtuple_class(Example)
        assert not is_namedtuple_instance(Example(x=0))
