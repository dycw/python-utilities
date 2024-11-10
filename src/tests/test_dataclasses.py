from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from types import NoneType
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from pytest import mark, param, raises

from utilities.dataclasses import (
    Dataclass,
    GetDataClassClassError,
    extend_non_sentinel,
    get_dataclass_class,
    get_dataclass_fields,
    is_dataclass_class,
    is_dataclass_instance,
    replace_non_sentinel,
    yield_field_names,
)
from utilities.sentinel import sentinel

if TYPE_CHECKING:
    from collections.abc import Sequence


class TestDataClassProtocol:
    def test_main(self) -> None:
        T = TypeVar("T", bound=Dataclass)

        def identity(x: T, /) -> T:
            return x

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        _ = identity(Example())


class TestExtendNonSentinel:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Sequence[int] = field(default_factory=list)

        obj = Example()
        obj1 = extend_non_sentinel(obj, x=1)
        assert obj1.x == [1]
        obj2 = extend_non_sentinel(obj1, x=sentinel)
        assert obj2.x == [1]

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestGetDataClassClass:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert get_dataclass_class(obj) is Example

    def test_error(self) -> None:
        with raises(GetDataClassClassError):
            _ = get_dataclass_class(cast(Any, None))


class TestGetDataClassFields:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: bool

        result = get_dataclass_fields(Example)
        expected = {"x": bool}
        assert result == expected

    def test_enum(self) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Truth

        result = get_dataclass_fields(Example, localns=locals())
        expected = {"x": Truth}
        assert result == expected

    def test_literal(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Literal["true", "false"]

        result = get_dataclass_fields(Example, localns={"Literal": Literal})
        expected = {"x": Literal["true", "false"]}
        assert result == expected


class TestIsDataClassClass:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        assert is_dataclass_class(Example)
        assert not is_dataclass_class(Example())

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_class(obj)


class TestIsDataClassInstance:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        assert not is_dataclass_instance(Example)
        assert is_dataclass_instance(Example())

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestReplaceNonSentinel:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example()
        obj1 = replace_non_sentinel(obj, x=1)
        assert obj1.x == 1
        obj2 = replace_non_sentinel(obj1, x=sentinel)
        assert obj2.x == 1

    @mark.parametrize("obj", [param(None), param(NoneType)])
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestYieldDataClassFieldNames:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert list(yield_field_names(obj)) == ["x"]
