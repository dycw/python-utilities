from __future__ import annotations

from itertools import chain
from types import NoneType
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    data,
    integers,
    lists,
    none,
    permutations,
    sampled_from,
)
from pytest import mark, param, raises

from utilities.core import (
    MaxNullableError,
    MinNullableError,
    get_class,
    get_class_name,
    max_nullable,
    min_nullable,
)
from utilities.errors import ImpossibleCaseError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class TestGetClass:
    @mark.parametrize(
        ("obj", "expected"), [param(None, NoneType), param(NoneType, NoneType)]
    )
    def test_main(self, *, obj: Any, expected: type[Any]) -> None:
        assert get_class(obj) is expected


class TestGetClassName:
    def test_class(self) -> None:
        class Example: ...

        assert get_class_name(Example) == "Example"

    def test_instance(self) -> None:
        class Example: ...

        assert get_class_name(Example()) == "Example"

    def test_qual(self) -> None:
        assert (
            get_class_name(ImpossibleCaseError, qual=True)
            == "utilities.errors.ImpossibleCaseError"
        )


class TestMinMaxNullable:
    @given(
        data=data(),
        values=lists(integers(), min_size=1),
        nones=lists(none()),
        case=sampled_from([(min_nullable, min), (max_nullable, max)]),
    )
    def test_main(
        self,
        *,
        data: DataObject,
        values: list[int],
        nones: list[None],
        case: tuple[
            Callable[[Iterable[int | None]], int], Callable[[Iterable[int]], int]
        ],
    ) -> None:
        func_nullable, func_builtin = case
        values_use = data.draw(permutations(list(chain(values, nones))))
        result = func_nullable(values_use)
        expected = func_builtin(values)
        assert result == expected

    @given(
        nones=lists(none()),
        value=integers(),
        func=sampled_from([min_nullable, max_nullable]),
    )
    def test_default(
        self, *, nones: list[None], value: int, func: Callable[..., int]
    ) -> None:
        result = func(nones, default=value)
        assert result == value

    @given(nones=lists(none()))
    def test_error_min_nullable(self, *, nones: list[None]) -> None:
        with raises(
            MinNullableError, match=r"Minimum of an all-None iterable is undefined"
        ):
            _ = min_nullable(nones)

    @given(nones=lists(none()))
    def test_error_max_nullable(self, *, nones: list[None]) -> None:
        with raises(
            MaxNullableError, match=r"Maximum of an all-None iterable is undefined"
        ):
            max_nullable(nones)
