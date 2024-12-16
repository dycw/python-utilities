from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, NotRequired, Self, TypedDict

from beartype import beartype
from pytest import mark, param

from tests.test_operator import (
    DataClass1,
    DataClass2Inner,
    DataClass2Outer,
    DataClass3,
    DataClass4,
)
from tests.test_typing_funcs.no_future import (
    Example_TestTypingFuncs_BeartypeCondOnMethod,
    Example_TestTypingFuncs_Inner,
    Example_TestTypingFuncs_Outer,
)
from utilities.beartype import beartype_cond
from utilities.typing import (
    contains_self,
    eval_typed_dict,
    get_args,
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


class TestEvalTypedDict:
    def test_main(self) -> None:
        class Example(TypedDict):
            a: int
            b: str
            c: NotRequired[float]

        result = eval_typed_dict(Example)
        expected = {"a": int, "b": str, "c": NotRequired[float]}
        assert result == expected

    def test_nested(self) -> None:
        class Outer(TypedDict):
            a: int
            b: str
            inner: Inner

        class Inner(TypedDict):
            c: int
            d: str

        _ = Inner

        result = eval_typed_dict(Outer, globals_=globals(), locals_=locals())
        expected = {"a": int, "b": str, "inner": {"c": int, "d": str}}
        assert result == expected


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
        ],
    )
    def test_main(self, *, obj: Any, expected: tuple[Any, ...]) -> None:
        result = get_args(obj)
        assert result == expected


class TestGetTypeHints:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        result = get_type_hints(Example)
        expected = {"x": int}
        assert result == expected

    def test_beartype(self) -> None:
        @beartype
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

            def identity(self) -> Self:
                return self

        hints = get_type_hints(Example)
        expected = {"x": int}
        assert hints == expected

    def test_beartype_cond(self) -> None:
        @beartype_cond
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

            @beartype_cond
            def identity(self) -> Self:
                return self

        hints = get_type_hints(Example)
        expected = {"x": int}
        assert hints == expected

    def test_nested(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        hints = get_type_hints(Outer, localns=locals())
        expected = {"inner": Inner}
        assert hints == expected

    def test_no_future(self) -> None:
        hints = get_type_hints(Example_TestTypingFuncs_Outer)
        expected = {"inner": Example_TestTypingFuncs_Inner}
        assert hints == expected

    def test_no_future2(self) -> None:
        hints = get_type_hints(Example_TestTypingFuncs_BeartypeCondOnMethod)
        expected = {}
        assert hints == expected

    def test_dataclass1(self) -> None:
        hints = get_type_hints(DataClass1)
        expected = {"x": int}
        assert hints == expected

    def test_dataclass2(self) -> None:
        hints = get_type_hints(DataClass2Outer)
        expected = {"inner": DataClass2Inner}
        assert hints == expected

    def test_dataclass3(self) -> None:
        hints = get_type_hints(DataClass3)
        expected = {"truth": Literal["true", "false"]}
        assert hints == expected

    def test_dataclass4(self) -> None:
        hints = get_type_hints(DataClass4)
        expected = {"x": int}
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
            param(
                is_optional_type, Literal["a", "b", "c"] | None, True, marks=mark.only
            ),
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
