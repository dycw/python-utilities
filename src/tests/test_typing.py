from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from random import Random
from types import NoneType
from typing import TYPE_CHECKING, Any, Final, Literal, NamedTuple
from uuid import UUID

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    dates,
    datetimes,
    floats,
    integers,
    just,
    none,
    sampled_from,
    sets,
    tuples,
)
from pytest import mark, param, raises

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
    DataClassFutureTimeDeltaNullable,
    DataClassFutureTypeLiteral,
    DataClassFutureUUID,
    TrueOrFalseFutureLit,
    TrueOrFalseFutureTypeLit,
)
from utilities.hypothesis import text_ascii
from utilities.sentinel import Sentinel
from utilities.types import LogLevel, Number, Parallelism, Seed
from utilities.typing import (
    IsInstanceGenError,
    IsSubclassGenError,
    _GetTypeClassesTupleError,
    _GetTypeClassesTypeError,
    _GetUnionTypeClassesInternalTypeError,
    _GetUnionTypeClassesUnionTypeError,
    get_args,
    get_literal_elements,
    get_type_classes,
    get_type_hints,
    get_union_type_classes,
    is_dict_type,
    is_frozenset_type,
    is_instance_gen,
    is_list_type,
    is_literal_type,
    is_mapping_type,
    is_namedtuple_class,
    is_namedtuple_instance,
    is_optional_type,
    is_sequence_type,
    is_set_type,
    is_subclass_gen,
    is_tuple_type,
    is_union_type,
)

if TYPE_CHECKING:
    from collections.abc import Callable


class TestGetArgs:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(dict[int, int], (int, int)),
            param(frozenset[int], (int,)),
            param(int | None, (int, NoneType)),
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

    @mark.parametrize(
        ("obj", "expected"),
        [param(int | None, (int,)), param(int | str | None, (int, str))],
    )
    def test_optional_drop_none(
        self, *, obj: Any, expected: tuple[type[Any], ...]
    ) -> None:
        result = get_args(obj, optional_drop_none=True)
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


class TestGetTypeClasses:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(int, (int,)),
            param((int, float), (int, float)),
            param(Number, (int, float)),
            param(Seed, (int, float, str, bytes, bytearray, Random)),
            param((int, Number), (int, int, float)),
            param((int, (Number,)), (int, int, float)),
        ],
    )
    def test_main(self, *, obj: Any, expected: tuple[type[Any], ...]) -> None:
        result = get_type_classes(obj)
        assert result == expected

    def test_error_type(self) -> None:
        with raises(
            _GetTypeClassesTypeError,
            match="Object must be a type, tuple or Union type; got None of type <class 'NoneType'>",
        ):
            _ = get_type_classes(None)

    def test_error_tuple(self) -> None:
        with raises(
            _GetTypeClassesTupleError,
            match="Tuple must contain types, tuples or Union types only; got None of type <class 'NoneType'>",
        ):
            _ = get_type_classes((None,))


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

    @given(data=data())
    def test_literal(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TrueOrFalseFutureLit

        cls = data.draw(sampled_from([Example, DataClassFutureLiteral]))
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globals(), localns=localns)
        expected = {"truth": TrueOrFalseFutureLit}
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
    def test_timedelta_nullable(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            timedelta: dt.timedelta | None

        cls = data.draw(sampled_from([Example, DataClassFutureTimeDeltaNullable]))
        globalns = data.draw(just(globals()) | none())
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globalns, localns=localns)
        expected = {"timedelta": dt.timedelta | None}
        assert hints == expected

    @given(data=data())
    def test_type_literal(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TrueOrFalseFutureTypeLit

        cls = data.draw(sampled_from([Example, DataClassFutureTypeLiteral]))
        localns = data.draw(just(locals()) | none())
        hints = get_type_hints(cls, globalns=globals(), localns=localns)
        expected = {"truth": TrueOrFalseFutureTypeLit}
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

    def test_warning(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        @dataclass(kw_only=True, slots=True)
        class Inner:
            int_: int

        with raises(
            UserWarning,
            match="Error getting type hints for <.*>; name 'Inner' is not defined",
        ):
            _ = get_type_hints(Outer, warn_name_errors=True)


class TestGetUnionTypeClasses:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(Number, (int, float)),
            param(Seed, (int, float, str, bytes, bytearray, Random)),
        ],
    )
    def test_main(self, *, obj: Any, expected: type[Any]) -> None:
        result = get_union_type_classes(obj)
        assert result == expected

    def test_error_union_type(self) -> None:
        with raises(
            _GetUnionTypeClassesUnionTypeError,
            match="Object must be a Union type; got None of type <class 'NoneType'>",
        ):
            _ = get_union_type_classes(None)

    def test_error_internal_type(self) -> None:
        with raises(
            _GetUnionTypeClassesInternalTypeError,
            match=r"Union type must contain types only; got typing\.Literal\[True\] of type <class 'typing\._LiteralGenericAlias'>",
        ):
            _ = get_union_type_classes(Literal[True] | None)


class TestIsAnnotationOfType:
    @mark.parametrize(
        ("func", "obj", "expected"),
        [
            param(is_dict_type, Mapping[int, int], False),
            param(is_dict_type, Sequence[int], False),
            param(is_dict_type, dict[int, int], True),
            param(is_dict_type, frozenset[int], False),
            param(is_dict_type, list[int], False),
            param(is_dict_type, set[int], False),
            param(is_dict_type, tuple[int, int], False),
            param(is_frozenset_type, Mapping[int, int], False),
            param(is_frozenset_type, Sequence[int], False),
            param(is_frozenset_type, dict[int, int], False),
            param(is_frozenset_type, frozenset[int], True),
            param(is_frozenset_type, list[int], False),
            param(is_frozenset_type, set[int], False),
            param(is_frozenset_type, tuple[int, int], False),
            param(is_list_type, Mapping[int, int], False),
            param(is_list_type, Sequence[int], False),
            param(is_list_type, dict[int, int], False),
            param(is_list_type, frozenset[int], False),
            param(is_list_type, list[int], True),
            param(is_list_type, set[int], False),
            param(is_list_type, tuple[int, int], False),
            param(is_literal_type, Literal["a", "b", "c"], True),
            param(is_literal_type, list[int], False),
            param(is_mapping_type, Mapping[int, int], True),
            param(is_mapping_type, Sequence[int], False),
            param(is_mapping_type, dict[int, int], False),
            param(is_mapping_type, frozenset[int], False),
            param(is_mapping_type, list[int], False),
            param(is_mapping_type, set[int], False),
            param(is_mapping_type, tuple[int, int], False),
            param(is_optional_type, Literal["a", "b", "c"] | None, True),
            param(is_optional_type, Literal["a", "b", "c"], False),
            param(is_optional_type, int | None, True),
            param(is_optional_type, int | str, False),
            param(is_optional_type, list[int] | None, True),
            param(is_optional_type, list[int], False),
            param(is_sequence_type, Mapping[int, int], False),
            param(is_sequence_type, Sequence[int], True),
            param(is_sequence_type, dict[int, int], False),
            param(is_sequence_type, frozenset[int], False),
            param(is_sequence_type, list[int], False),
            param(is_sequence_type, set[int], False),
            param(is_sequence_type, tuple[int, int], False),
            param(is_set_type, Mapping[int, int], False),
            param(is_set_type, Sequence[int], False),
            param(is_set_type, dict[int, int], False),
            param(is_set_type, frozenset[int], False),
            param(is_set_type, list[int], False),
            param(is_set_type, set[int], True),
            param(is_set_type, tuple[int, int], False),
            param(is_tuple_type, Mapping[int, int], False),
            param(is_tuple_type, Sequence[int], False),
            param(is_tuple_type, dict[int, int], False),
            param(is_tuple_type, frozenset[int], False),
            param(is_tuple_type, list[int], False),
            param(is_tuple_type, set[int], False),
            param(is_tuple_type, tuple[int, int], True),
            param(is_union_type, int | str, True),
            param(is_union_type, list[int], False),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        assert func(obj) is expected


class TestIsInstanceGen:
    @given(data=data())
    @mark.parametrize(
        ("strategy", "type_", "min_size", "expected"),
        [
            # types - bool/int
            param(booleans(), bool, None, True),
            param(booleans(), int, 2, False),
            param(integers(), bool, None, False),
            param(integers(), int, None, True),
            param(booleans(), (bool, int), None, True),
            param(integers(), (bool, int), None, True),
            # types - datetime/date
            param(dates(), dt.date, None, True),
            param(dates(), dt.datetime, None, False),
            param(datetimes(), dt.date, None, False),
            param(datetimes(), dt.datetime, None, True),
            # parent union
            param(booleans(), Number, 2, False),
            param(integers(), Number, None, True),
            param(floats(), Number, None, True),
            # child tuple/union - skip
            # literals
            param(sampled_from([1, 2]), Literal[1, 2, 3], 2, True),
            param(sampled_from([1, 2, 3]), Literal[1, 2, 3], 3, True),
            param(sampled_from([1, 2, 3]), Literal[1, 2], 3, False),
            param(sampled_from([1, 2, 3]), int, 3, True),
            param(sampled_from([1, 2, 3]), bool, 3, False),
            param(sampled_from(["1", "2", "3"]), str, 3, True),
            param(sampled_from(["1", "2", "3"]), bool, 3, False),
            param(sampled_from([1, "2", 3]), int | str, 3, True),
            param(sampled_from([1, "2", 3]), int, 3, False),
            param(sampled_from([1, "2", 3]), str, 3, False),
            param(booleans(), Literal[1, 2, 3], 2, False),
            param(text_ascii(), Literal["a", "b", "c"], 4, False),
            # tuple types
            param(tuples(booleans()), tuple[bool], None, True),
            param(tuples(booleans()), tuple[int], 2, False),
            param(tuples(integers()), tuple[bool], None, False),
            param(tuples(integers()), tuple[int], None, True),
            param(tuples(booleans()), tuple[Number], 2, False),
            param(tuples(integers()), tuple[Number], None, True),
            param(tuples(floats()), tuple[Number], None, True),
            param(tuples(booleans()), bool, 2, False),
            param(tuples(just("a"), booleans()), tuple[Literal["a"], bool], None, True),
            param(tuples(just("a"), booleans()), tuple[Literal["a"], int], 2, False),
            param(
                tuples(just("a"), integers()), tuple[Literal["a"], bool], None, False
            ),
            param(tuples(just("a"), integers()), tuple[Literal["a"], int], None, True),
            param(
                tuples(just("a"), booleans()),
                tuple[Literal["a", "b"], bool],
                None,
                True,
            ),
            param(
                tuples(just("a"), booleans()), tuple[Literal["a", "b"], int], 2, False
            ),
            param(
                tuples(just("a"), integers()),
                tuple[Literal["a", "b"], bool],
                None,
                False,
            ),
            param(
                tuples(just("a"), integers()), tuple[Literal["a", "b"], int], None, True
            ),
            param(booleans(), tuple[bool], 2, False),
            param(booleans() | none(), tuple[bool], 3, False),
            param(text_ascii(), tuple[bool], None, False),
            param(text_ascii() | none(), tuple[bool], None, False),
        ],
    )
    def test_main(
        self,
        *,
        data: DataObject,
        strategy: SearchStrategy[Any],
        type_: Any,
        min_size: int | None,
        expected: bool,
    ) -> None:
        match expected:
            case True:
                value = data.draw(strategy)
                assert is_instance_gen(value, type_)
            case False:
                values = data.draw(
                    sets(strategy, min_size=10 if min_size is None else min_size)
                )
                assert not all(is_instance_gen(v, type_) for v in values)

    @given(bool_=booleans())
    def test_bool_value_vs_custom_int(self, *, bool_: bool) -> None:
        class MyInt(int): ...

        assert not is_instance_gen(bool_, MyInt)

    @given(int_=integers())
    def test_int_value_vs_custom_int(self, *, int_: int) -> None:
        class MyInt(int): ...

        assert not is_instance_gen(int_, MyInt)
        assert is_instance_gen(MyInt(int_), MyInt)

    def test_error(self) -> None:
        with raises(
            IsInstanceGenError,
            match=r"Invalid arguments; got None of type <class 'NoneType'> and typing\.Final of type <class 'typing\._SpecialForm'>",
        ):
            _ = is_instance_gen(None, Final)


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


class TestIsSubclassGen:
    @mark.parametrize(
        ("child", "parent", "expected"),
        [
            # types - bool/int
            param(bool, bool, True),
            param(bool, int, False),
            param(int, bool, False),
            param(int, int, True),
            param(bool, (bool, int), True),
            param(int, (bool, int), True),
            # types - datetime/date
            param(dt.date, dt.date, True),
            param(dt.date, dt.datetime, False),
            param(dt.datetime, dt.date, False),
            param(dt.datetime, dt.datetime, True),
            # parent union
            param(bool, Number, False),
            param(int, Number, True),
            param(float, Number, True),
            # child tuple
            param((bool,), bool, True),
            param((bool,), int, False),
            param((int,), bool, False),
            param((int,), int, True),
            param((bool, int), int, False),
            param((bool, int), (bool, int), True),
            param((bool, int), (bool, int, float), True),
            # child union
            param(bool, bool | None, True),
            param(bool | None, bool, False),
            param(bool | None, bool | None, True),
            param(Number, int, False),
            param(Number, float, False),
            param(Number, Number, True),
            # literals
            param(Literal[1, 2], Literal[1, 2, 3], True),
            param(Literal[1, 2, 3], Literal[1, 2, 3], True),
            param(Literal[1, 2, 3], Literal[1, 2], False),
            param(Literal[1, 2, 3], int, True),
            param(Literal[1, 2, 3], bool, False),
            param(Literal["1", "2", "3"], str, True),
            param(Literal["1", "2", "3"], bool, False),
            param(Literal[1, "2", 3], int | str, True),
            param(Literal[1, "2", 3], int, False),
            param(Literal[1, "2", 3], str, False),
            param(bool, Literal[1, 2, 3], False),
            param(str, Literal["a", "b", "c"], False),
            # tuple types
            param(tuple[bool], tuple[bool], True),
            param(tuple[bool], tuple[int], False),
            param(tuple[int], tuple[bool], False),
            param(tuple[int], tuple[int], True),
            param(tuple[bool], tuple[Number], False),
            param(tuple[int], tuple[Number], True),
            param(tuple[float], tuple[Number], True),
            param(tuple[bool], bool, False),
            param(tuple[Literal["a"], bool], tuple[Literal["a"], bool], True),
            param(tuple[Literal["a"], bool], tuple[Literal["a"], int], False),
            param(tuple[Literal["a"], int], tuple[Literal["a"], bool], False),
            param(tuple[Literal["a"], int], tuple[Literal["a"], int], True),
            param(tuple[Literal["a"], bool], tuple[Literal["a", "b"], bool], True),
            param(tuple[Literal["a"], bool], tuple[Literal["a", "b"], int], False),
            param(tuple[Literal["a"], int], tuple[Literal["a", "b"], bool], False),
            param(tuple[Literal["a"], int], tuple[Literal["a", "b"], int], True),
            param(bool, tuple[bool], False),
            param(bool | None, tuple[bool], False),
            param(str, tuple[Literal["a", "b", "c"]], False),
            param(str | None, tuple[Literal["a", "b", "c"]], False),
        ],
    )
    def test_main(self, *, child: type[Any], parent: Any, expected: bool) -> None:
        assert is_subclass_gen(child, parent) is expected

    def test_custom_int(self) -> None:
        class MyInt(int): ...

        assert not is_subclass_gen(bool, MyInt)
        assert not is_subclass_gen(MyInt, bool)
        assert not is_subclass_gen(int, MyInt)
        assert is_subclass_gen(MyInt, int)
        assert is_subclass_gen(MyInt, MyInt)

    def test_error(self) -> None:
        with raises(
            IsSubclassGenError,
            match="Argument must be a class; got None of type <class 'NoneType'>",
        ):
            _ = is_subclass_gen(None, NoneType)
