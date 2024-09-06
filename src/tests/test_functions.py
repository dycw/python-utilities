from __future__ import annotations

import sys
from functools import wraps
from operator import neg
from types import NoneType
from typing import TYPE_CHECKING, Any, TypeVar

from hypothesis import given
from hypothesis.strategies import booleans, integers
from pytest import mark, param

from utilities.asyncio import try_await
from utilities.functions import (
    first,
    get_class,
    get_class_name,
    get_func_name,
    identity,
    is_none,
    is_not_none,
    not_func,
    second,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_T = TypeVar("_T")


class TestFirst:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        assert first(pair) == x


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


class TestGetFuncName:
    @mark.parametrize(
        ("func", "expected"),
        [
            param(identity, "identity"),
            param(lambda x: x, "<lambda>"),  # pyright: ignore[reportUnknownLambdaType]
            param(len, "len"),
            param(neg, "neg"),
            param(object.__init__, "object.__init__"),
            param(object().__str__, "object.__str__"),
            param(repr, "repr"),
            param(str, "str"),
            param(try_await, "try_await"),
            param(str.join, "str.join"),
            param(sys.exit, "exit"),
        ],
    )
    def test_main(self, *, func: Callable[..., Any], expected: str) -> None:
        assert get_func_name(func) == expected

    def test_decorated(self) -> None:
        @wraps(identity)
        def wrapped(x: _T, /) -> _T:
            return identity(x)

        assert get_func_name(wrapped) == "identity"

    def test_object(self) -> None:
        class Example:
            def __call__(self, x: _T, /) -> _T:
                return identity(x)

        obj = Example()
        assert get_func_name(obj) == "Example"

    def test_obj_method(self) -> None:
        class Example:
            def obj_method(self, x: _T) -> _T:
                return identity(x)

        obj = Example()
        assert get_func_name(obj.obj_method) == "obj_method"

    def test_obj_classmethod(self) -> None:
        class Example:
            @classmethod
            def obj_classmethod(cls: _T) -> _T:
                return identity(cls)

        assert get_func_name(Example.obj_classmethod) == "obj_classmethod"

    def test_obj_staticmethod(self) -> None:
        class Example:
            @staticmethod
            def obj_staticmethod(x: _T) -> _T:
                return identity(x)

        assert get_func_name(Example.obj_staticmethod) == "obj_staticmethod"


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x


class TestIsNoneAndIsNotNone:
    @mark.parametrize(
        ("func", "obj", "expected"),
        [
            param(is_none, None, True),
            param(is_none, 0, False),
            param(is_not_none, None, False),
            param(is_not_none, 0, True),
        ],
    )
    def test_main(
        self, *, func: Callable[[Any], bool], obj: Any, expected: bool
    ) -> None:
        result = func(obj)
        assert result is expected


class TestNotFunc:
    @given(x=booleans())
    def test_main(self, *, x: bool) -> None:
        def return_x() -> bool:
            return x

        return_not_x = not_func(return_x)
        result = return_not_x()
        expected = not x
        assert result is expected


class TestSecond:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        assert second(pair) == y
