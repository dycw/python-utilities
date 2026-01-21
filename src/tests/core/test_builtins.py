from __future__ import annotations

import sys
from collections.abc import Callable
from functools import cache, lru_cache, partial, wraps
from itertools import chain
from operator import neg
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
    get_func_name,
    identity,
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


class TestGetFuncName:
    @mark.parametrize(
        ("func", "expected"),
        [
            param(identity, "identity"),
            param(lambda x: x, "<lambda>"),  # pyright: ignore[reportUnknownLambdaType]
            param(len, "len"),
            param(neg, "neg"),
            param(object.__init__, "object.__init__"),
            param(object.__str__, "object.__str__"),
            param(repr, "repr"),
            param(str, "str"),
            param(str.join, "str.join"),
            param(sys.exit, "exit"),
        ],
    )
    def test_main(self, *, func: Callable[..., Any], expected: str) -> None:
        assert get_func_name(func) == expected

    def test_cache(self) -> None:
        @cache
        def cache_func(x: int, /) -> int:
            return x

        assert get_func_name(cache_func) == "cache_func"

    def test_decorated(self) -> None:
        @wraps(identity)
        def wrapped[T](x: T, /) -> T:
            return identity(x)

        assert get_func_name(wrapped) == "identity"

    def test_lru_cache(self) -> None:
        @lru_cache
        def lru_cache_func(x: int, /) -> int:
            return x

        assert get_func_name(lru_cache_func) == "lru_cache_func"

    def test_object(self) -> None:
        class Example:
            def __call__[T](self, x: T, /) -> T:
                return identity(x)

        assert get_func_name(Example()) == "Example"

    def test_obj_method(self) -> None:
        class Example:
            def obj_method[T](self, x: T) -> T:
                return identity(x)

        assert get_func_name(Example().obj_method) == "Example.obj_method"

    def test_obj_classmethod(self) -> None:
        class Example:
            @classmethod
            def obj_classmethod[T](cls: T) -> T:
                return identity(cls)

        assert get_func_name(Example.obj_classmethod) == "Example.obj_classmethod"

    def test_obj_staticmethod(self) -> None:
        class Example:
            @staticmethod
            def obj_staticmethod[T](x: T) -> T:
                return identity(x)

        assert get_func_name(Example.obj_staticmethod) == "Example.obj_staticmethod"

    def test_partial(self) -> None:
        assert get_func_name(partial(identity)) == "identity"


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

    @mark.parametrize("func", [param(min_nullable), param(max_nullable)])
    def test_default(self, *, func: Callable[..., int]) -> None:
        assert func([], default=True) is True

    def test_error_min(self) -> None:
        with raises(MinNullableError, match=r"Minimum of \[\] is undefined"):
            _ = min_nullable([])

    def test_error_max(self) -> None:
        with raises(MaxNullableError, match=r"Maximum of \[\] is undefined"):
            max_nullable([])
