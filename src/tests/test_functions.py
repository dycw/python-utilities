from __future__ import annotations

import sys
from functools import cache, lru_cache, partial, wraps
from operator import neg
from types import NoneType
from typing import TYPE_CHECKING, Any, TypeVar, cast

from hypothesis import given
from hypothesis.strategies import booleans, integers
from pytest import CaptureFixture, mark, param, raises

from utilities.asyncio import try_await
from utilities.functions import (
    EnsureNotNoneError,
    ensure_not_none,
    first,
    get_class,
    get_class_name,
    get_func_name,
    get_func_qualname,
    identity,
    is_none,
    is_not_none,
    not_func,
    second,
    send_and_next,
    start_generator_coroutine,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


_T = TypeVar("_T")


class TestEnsureNotNone:
    def test_main(self) -> None:
        maybe_int = cast(int | None, 0)
        result = ensure_not_none(maybe_int)
        assert result == 0

    def test_error(self) -> None:
        with raises(EnsureNotNoneError, match="Object .* must not be None"):
            _ = ensure_not_none(None)


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


class TestGetFuncNameAndGetFuncQualName:
    @mark.parametrize(
        ("func", "exp_name", "exp_qual_name"),
        [
            param(identity, "identity", "utilities.functions.identity"),
            param(
                lambda x: x,  # pyright: ignore[reportUnknownLambdaType]
                "<lambda>",
                "tests.test_functions.TestGetFuncNameAndGetFuncQualName.<lambda>",
            ),
            param(len, "len", "builtins.len"),
            param(neg, "neg", "_operator.neg"),
            param(object.__init__, "object.__init__", "builtins.object.__init__"),
            param(object.__str__, "object.__str__", "builtins.object.__str__"),
            param(repr, "repr", "builtins.repr"),
            param(str, "str", "builtins.str"),
            param(try_await, "try_await", "utilities.asyncio.try_await"),
            param(str.join, "str.join", "builtins.str.join"),
            param(sys.exit, "exit", "sys.exit"),
        ],
    )
    def test_main(
        self, *, func: Callable[..., Any], exp_name: str, exp_qual_name: str
    ) -> None:
        assert get_func_name(func) == exp_name
        assert get_func_qualname(func) == exp_qual_name

    def test_cache(self) -> None:
        @cache
        def cache_func(x: int, /) -> int:
            return x

        assert get_func_name(cache_func) == "cache_func"
        assert (
            get_func_qualname(cache_func)
            == "tests.test_functions.TestGetFuncNameAndGetFuncQualName.test_cache.<locals>.cache_func"
        )

    def test_decorated(self) -> None:
        @wraps(identity)
        def wrapped(x: _T, /) -> _T:
            return identity(x)

        assert get_func_name(wrapped) == "identity"
        assert get_func_qualname(wrapped) == "utilities.functions.identity"

    def test_lru_cache(self) -> None:
        @lru_cache
        def lru_cache_func(x: int, /) -> int:
            return x

        assert get_func_name(lru_cache_func) == "lru_cache_func"
        assert (
            get_func_qualname(lru_cache_func)
            == "tests.test_functions.TestGetFuncNameAndGetFuncQualName.test_lru_cache.<locals>.lru_cache_func"
        )

    def test_object(self) -> None:
        class Example:
            def __call__(self, x: _T, /) -> _T:
                return identity(x)

        obj = Example()
        assert get_func_name(obj) == "Example"
        assert get_func_qualname(obj) == "tests.test_functions.Example"

    def test_obj_method(self) -> None:
        class Example:
            def obj_method(self, x: _T) -> _T:
                return identity(x)

        obj = Example()
        assert get_func_name(obj.obj_method) == "Example.obj_method"
        assert (
            get_func_qualname(obj.obj_method)
            == "tests.test_functions.TestGetFuncNameAndGetFuncQualName.test_obj_method.<locals>.Example.obj_method"
        )

    def test_obj_classmethod(self) -> None:
        class Example:
            @classmethod
            def obj_classmethod(cls: _T) -> _T:
                return identity(cls)

        assert get_func_name(Example.obj_classmethod) == "Example.obj_classmethod"
        assert (
            get_func_qualname(Example.obj_classmethod)
            == "tests.test_functions.TestGetFuncNameAndGetFuncQualName.test_obj_classmethod.<locals>.Example.obj_classmethod"
        )

    def test_obj_staticmethod(self) -> None:
        class Example:
            @staticmethod
            def obj_staticmethod(x: _T) -> _T:
                return identity(x)

        assert get_func_name(Example.obj_staticmethod) == "Example.obj_staticmethod"
        assert (
            get_func_qualname(Example.obj_staticmethod)
            == "tests.test_functions.TestGetFuncNameAndGetFuncQualName.test_obj_staticmethod.<locals>.Example.obj_staticmethod"
        )

    def test_partial(self) -> None:
        part = partial(identity)
        assert get_func_name(part) == "identity"
        assert get_func_qualname(part) == "utilities.functions.identity"


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


class TestSendAndNext:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        @start_generator_coroutine
        def func() -> Generator[int | None, float | None, str]:
            print("Initial")  # noqa: T201
            while True:
                input_ = ensure_not_none((yield))
                output = round(ensure_not_none(input_))
                if output >= 0:
                    print(f"Received {input_}, yielding {output}")  # noqa: T201
                    yield output
                else:
                    return "Done"

        generator = func()
        out = capsys.readouterr().out
        assert out == "Initial\n", out
        result = send_and_next(0.1, generator)
        assert result == 0
        out = capsys.readouterr().out
        assert out == "Received 0.1, yielding 0\n", out
        result = send_and_next(0.9, generator)
        assert result == 1
        out = capsys.readouterr().out
        assert out == "Received 0.9, yielding 1\n", out
        result = send_and_next(1.1, generator)
        assert result == 1
        out = capsys.readouterr().out
        assert out == "Received 1.1, yielding 1\n", out
        with raises(StopIteration) as exc:
            _ = send_and_next(-0.9, generator)
        assert exc.value.args == ("Done",)


class TestStartGeneratorCoroutine:
    def test_main(self, *, capsys: CaptureFixture) -> None:
        @start_generator_coroutine
        def func() -> Generator[int, float, str]:
            print("Pre-initial")  # noqa: T201
            x = yield 0
            print(f"Post-initial; x={x}")  # noqa: T201
            while x >= 0:
                print(f"Pre-yield; x={x}")  # noqa: T201
                x = yield round(x)
                print(f"Post-yield; x={x}")  # noqa: T201
            return "Done"

        generator = func()
        out = capsys.readouterr().out
        assert out == "Pre-initial\n", out
        assert generator.send(0.1) == 0
        out = capsys.readouterr().out
        assert out == "Post-initial; x=0.1\nPre-yield; x=0.1\n", out
        assert generator.send(0.9) == 1
        out = capsys.readouterr().out
        assert out == "Post-yield; x=0.9\nPre-yield; x=0.9\n", out
        assert generator.send(1.1) == 1
        out = capsys.readouterr().out
        assert out == "Post-yield; x=1.1\nPre-yield; x=1.1\n", out
        with raises(StopIteration) as exc:
            _ = generator.send(-0.9)
        assert exc.value.args == ("Done",)
