from __future__ import annotations

import sys
from dataclasses import dataclass
from functools import cache, lru_cache, partial, wraps
from operator import neg
from types import NoneType
from typing import TYPE_CHECKING, Any, TypeVar, cast

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    builds,
    data,
    dictionaries,
    integers,
    lists,
    sampled_from,
)
from pytest import mark, param, raises

from utilities.asyncio import try_await
from utilities.datetime import get_now, get_today
from utilities.functions import (
    EnsureBoolError,
    EnsureBytesError,
    EnsureClassError,
    EnsureDateError,
    EnsureDatetimeError,
    EnsureFloatError,
    EnsureHashableError,
    EnsureIntError,
    EnsureMemberError,
    EnsureNotNoneError,
    EnsureNumberError,
    EnsureSizedError,
    EnsureSizedNotStrError,
    EnsureStrError,
    EnsureTimeError,
    ensure_bool,
    ensure_bytes,
    ensure_class,
    ensure_date,
    ensure_datetime,
    ensure_float,
    ensure_hashable,
    ensure_int,
    ensure_member,
    ensure_not_none,
    ensure_number,
    ensure_sized,
    ensure_sized_not_str,
    ensure_str,
    ensure_time,
    first,
    get_class,
    get_class_name,
    get_func_name,
    get_func_qualname,
    identity,
    is_dataclass_class,
    is_dataclass_instance,
    is_hashable,
    is_none,
    is_not_none,
    is_sequence_of_tuple_or_str_mapping,
    is_sized,
    is_sized_not_str,
    is_string_mapping,
    is_subclass_except_bool_int,
    is_tuple,
    is_tuple_or_str_mapping,
    make_isinstance,
    map_object,
    not_func,
    second,
)
from utilities.sentinel import sentinel

if TYPE_CHECKING:
    import datetime as dt
    from collections.abc import Callable

    from utilities.types import Number


_T = TypeVar("_T")


class TestEnsureBytes:
    @mark.parametrize(
        ("obj", "nullable"), [param(b"", False), param(b"", True), param(None, True)]
    )
    def test_main(self, *, obj: bytes | None, nullable: bool) -> None:
        _ = ensure_bytes(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a byte string"),
            param(True, "Object .* must be a byte string or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureBytesError, match=f"{match}; got .* instead"):
            _ = ensure_bytes(sentinel, nullable=nullable)


class TestEnsureBool:
    @mark.parametrize(
        ("obj", "nullable"), [param(True, False), param(True, True), param(None, True)]
    )
    def test_main(self, *, obj: bool | None, nullable: bool) -> None:
        _ = ensure_bool(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a boolean"),
            param(True, "Object .* must be a boolean or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureBoolError, match=f"{match}; got .* instead"):
            _ = ensure_bool(sentinel, nullable=nullable)


class TestEnsureClass:
    @mark.parametrize(
        ("obj", "cls", "nullable"),
        [
            param(True, bool, False),
            param(True, bool, True),
            param(True, (bool,), False),
            param(True, (bool,), True),
            param(None, bool, True),
        ],
    )
    def test_main(self, *, obj: Any, cls: Any, nullable: bool) -> None:
        _ = ensure_class(obj, cls, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be an instance of .*"),
            param(True, "Object .* must be an instance of .* or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureClassError, match=f"{match}; got .* instead"):
            _ = ensure_class(sentinel, bool, nullable=nullable)


class TestEnsureDate:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(get_today(), False), param(get_today(), True), param(None, True)],
    )
    def test_main(self, *, obj: dt.date | None, nullable: bool) -> None:
        _ = ensure_date(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a date"),
            param(True, "Object .* must be a date or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureDateError, match=f"{match}; got .* instead"):
            _ = ensure_date(sentinel, nullable=nullable)


class TestEnsureDatetime:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(get_now(), False), param(get_now(), True), param(None, True)],
    )
    def test_main(self, *, obj: dt.datetime | None, nullable: bool) -> None:
        _ = ensure_datetime(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a datetime"),
            param(True, "Object .* must be a datetime or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureDatetimeError, match=f"{match}; got .* instead"):
            _ = ensure_datetime(sentinel, nullable=nullable)


class TestEnsureFloat:
    @mark.parametrize(
        ("obj", "nullable"), [param(0.0, False), param(0.0, True), param(None, True)]
    )
    def test_main(self, *, obj: float | None, nullable: bool) -> None:
        _ = ensure_float(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a float"),
            param(True, "Object .* must be a float or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureFloatError, match=f"{match}; got .* instead"):
            _ = ensure_float(sentinel, nullable=nullable)


class TestEnsureHashable:
    @mark.parametrize("obj", [param(0), param((1, 2, 3))])
    def test_main(self, *, obj: Any) -> None:
        assert ensure_hashable(obj) == obj

    def test_error(self) -> None:
        with raises(EnsureHashableError, match=r"Object .* must be hashable\."):
            _ = ensure_hashable([1, 2, 3])


class TestEnsureInt:
    @mark.parametrize(
        ("obj", "nullable"), [param(0, False), param(0, True), param(None, True)]
    )
    def test_main(self, *, obj: int | None, nullable: bool) -> None:
        _ = ensure_int(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be an integer"),
            param(True, "Object .* must be an integer or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureIntError, match=f"{match}; got .* instead"):
            _ = ensure_int(sentinel, nullable=nullable)


class TestEnsureMember:
    @mark.parametrize(
        ("obj", "nullable"),
        [
            param(True, True),
            param(True, False),
            param(False, True),
            param(False, False),
            param(None, True),
        ],
    )
    def test_main(self, *, obj: Any, nullable: bool) -> None:
        _ = ensure_member(obj, {True, False}, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a member of .*"),
            param(True, "Object .* must be a member of .* or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureMemberError, match=match):
            _ = ensure_member(sentinel, {True, False}, nullable=nullable)


class TestEnsureNotNone:
    def test_main(self) -> None:
        maybe_int = cast(int | None, 0)
        result = ensure_not_none(maybe_int)
        assert result == 0

    def test_error(self) -> None:
        with raises(EnsureNotNoneError, match="Object must not be None"):
            _ = ensure_not_none(None)

    def test_error_with_desc(self) -> None:
        with raises(EnsureNotNoneError, match="Name must not be None"):
            _ = ensure_not_none(None, desc="Name")


class TestEnsureNumber:
    @mark.parametrize(
        ("obj", "nullable"),
        [param(0, False), param(0.0, False), param(0.0, True), param(None, True)],
    )
    def test_main(self, *, obj: Number, nullable: bool) -> None:
        _ = ensure_number(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a number"),
            param(True, "Object .* must be a number or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureNumberError, match=f"{match}; got .* instead"):
            _ = ensure_number(sentinel, nullable=nullable)


class TestEnsureSized:
    @mark.parametrize("obj", [param([]), param(()), param("")])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized(obj)

    def test_error(self) -> None:
        with raises(EnsureSizedError, match=r"Object .* must be sized"):
            _ = ensure_sized(None)


class TestEnsureSizedNotStr:
    @mark.parametrize("obj", [param([]), param(())])
    def test_main(self, *, obj: Any) -> None:
        _ = ensure_sized_not_str(obj)

    @mark.parametrize("obj", [param(None), param("")])
    def test_error(self, *, obj: Any) -> None:
        with raises(
            EnsureSizedNotStrError, match="Object .* must be sized, but not a string"
        ):
            _ = ensure_sized_not_str(obj)


class TestEnsureStr:
    @mark.parametrize(
        ("obj", "nullable"), [param("", False), param("", True), param(None, True)]
    )
    def test_main(self, *, obj: bool | None, nullable: bool) -> None:
        _ = ensure_str(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a string"),
            param(True, "Object .* must be a string or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureStrError, match=f"{match}; got .* instead"):
            _ = ensure_str(sentinel, nullable=nullable)


class TestEnsureTime:
    @mark.parametrize(
        ("obj", "nullable"),
        [
            param(get_now().time(), False),
            param(get_now().time(), True),
            param(None, True),
        ],
    )
    def test_main(self, *, obj: dt.time | None, nullable: bool) -> None:
        _ = ensure_time(obj, nullable=nullable)

    @mark.parametrize(
        ("nullable", "match"),
        [
            param(False, "Object .* must be a time"),
            param(True, "Object .* must be a time or None"),
        ],
    )
    def test_error(self, *, nullable: bool, match: str) -> None:
        with raises(EnsureTimeError, match=f"{match}; got .* instead"):
            _ = ensure_time(sentinel, nullable=nullable)


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


class TestIsDataClassClass:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        assert is_dataclass_class(Example)
        assert not is_dataclass_class(Example())

    @given(obj=sampled_from([None, type(None)]))
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_class(obj)


class TestIsDataClassInstance:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        assert not is_dataclass_instance(Example)
        assert is_dataclass_instance(Example())

    @given(obj=sampled_from([None, type(None)]))
    def test_others(self, *, obj: Any) -> None:
        assert not is_dataclass_instance(obj)


class TestIsHashable:
    @mark.parametrize(
        ("obj", "expected"),
        [param(0, True), param((1, 2, 3), True), param([1, 2, 3], False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_hashable(obj) is expected


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


class TestIsSequenceOfTupleOrStrgMapping:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param([(1, 2, 3)], True),
            param([{"a": 1, "b": 2, "c": 3}], True),
            param([(1, 2, 3), {"a": 1, "b": 2, "c": 3}], True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_sequence_of_tuple_or_str_mapping(obj)
        assert result is expected


class TestIsSized:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", True)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized(obj) is expected


class TestIsSizedNotStr:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param([], True), param((), True), param("", False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        assert is_sized_not_str(obj) is expected


class TestIsStringMapping:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_string_mapping(obj)
        assert result is expected


class TestIsSubclassExceptBoolInt:
    @mark.parametrize(
        ("x", "y", "expected"),
        [param(bool, bool, True), param(bool, int, False), param(int, int, True)],
    )
    def test_main(self, *, x: type[Any], y: type[Any], expected: bool) -> None:
        assert is_subclass_except_bool_int(x, y) is expected

    def test_subclass_of_int(self) -> None:
        class MyInt(int): ...

        assert not is_subclass_except_bool_int(bool, MyInt)


class TestIsTuple:
    @mark.parametrize(
        ("obj", "expected"),
        [param(None, False), param((1, 2, 3), True), param([1, 2, 3], False)],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_tuple(obj)
        assert result is expected


class TestIsTupleOrStringMapping:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((1, 2, 3), True),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_tuple_or_str_mapping(obj)
        assert result is expected


class TestMakeIsInstance:
    @mark.parametrize(
        ("obj", "expected"), [param(True, True), param(False, True), param(None, False)]
    )
    def test_main(self, *, obj: bool | None, expected: bool) -> None:
        func = make_isinstance(bool)
        result = func(obj)
        assert result is expected


class TestMapObject:
    @given(x=integers())
    def test_int(self, *, x: int) -> None:
        result = map_object(neg, x)
        expected = -x
        assert result == expected

    @given(x=dictionaries(integers(), integers()))
    def test_dict(self, *, x: dict[int, int]) -> None:
        result = map_object(neg, x)
        expected = {k: -v for k, v in x.items()}
        assert result == expected

    @given(x=lists(integers()))
    def test_sequences(self, *, x: list[int]) -> None:
        result = map_object(neg, x)
        expected = list(map(neg, x))
        assert result == expected

    @given(data=data())
    def test_dataclasses(self, *, data: DataObject) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = data.draw(builds(Example))
        result = map_object(neg, obj)
        expected = {"x": -obj.x}
        assert result == expected

    @given(x=lists(dictionaries(integers(), integers())))
    def test_nested(self, *, x: list[dict[int, int]]) -> None:
        result = map_object(neg, x)
        expected = [{k: -v for k, v in x_i.items()} for x_i in x]
        assert result == expected

    @given(x=lists(integers()))
    def test_before(self, *, x: list[int]) -> None:
        def before(x: Any, /) -> Any:
            return x + 1 if isinstance(x, int) else x

        result = map_object(neg, x, before=before)
        expected = [-(i + 1) for i in x]
        assert result == expected


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
