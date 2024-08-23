from __future__ import annotations

from functools import wraps
from types import NoneType
from typing import Any, TypeVar

from hypothesis import given
from hypothesis.strategies import integers
from pytest import mark, param

from utilities.functions import (
    first,
    get_class,
    get_class_name,
    get_func_name,
    identity,
    if_not_none,
    second,
)

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


class TestIfNotNone:
    def test_uses_first(self) -> None:
        result = if_not_none(0, "0")
        assert result == 0

    def test_uses_second(self) -> None:
        result = if_not_none(None, 0)
        assert result == 0


class TestGetFuncName:
    def test_main(self) -> None:
        assert get_func_name(identity) == "identity"

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

    def test_object_method(self) -> None:
        class Example:
            def identity(self, x: _T) -> _T:
                return identity(x)

        obj = Example()
        assert get_func_name(obj.identity) == "identity"

    def test_object_classmethod(self) -> None:
        class Example:
            @classmethod
            def identity(cls: _T) -> _T:
                return identity(cls)

        assert get_func_name(Example.identity) == "identity"

    def test_object_staticmethod(self) -> None:
        class Example:
            @staticmethod
            def identity(x: _T) -> _T:
                return identity(x)

        assert get_func_name(Example.identity) == "identity"


class TestIdentity:
    @given(x=integers())
    def test_main(self, *, x: int) -> None:
        assert identity(x) == x


class TestSecond:
    @given(x=integers(), y=integers())
    def test_main(self, *, x: int, y: int) -> None:
        pair = x, y
        assert second(pair) == y
