from __future__ import annotations

from dataclasses import dataclass, field, fields
from types import NoneType
from typing import Any, Literal, TypeVar, cast

from hypothesis import given
from hypothesis.strategies import integers, lists
from ib_async import Future
from polars import DataFrame
from pytest import raises
from typing_extensions import override

from tests.test_operator import DataClass3
from utilities.dataclasses import (
    Dataclass,
    GetDataClassClassError,
    _AsDictWithTypesElement,
    _is_not_default_value,
    _YieldFieldsClass,
    asdict_with_types,
    asdict_without_defaults,
    replace_non_sentinel,
    repr_without_defaults,
    yield_fields,
)
from utilities.functions import get_class_name
from utilities.iterables import one
from utilities.orjson import OrjsonLogRecord
from utilities.polars import are_frames_equal
from utilities.sentinel import sentinel
from utilities.typing import get_args, is_list_type, is_literal_type, is_optional_type

if TYPE_CHECKING:
    from utilities.types import StrMapping


TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


class TestAsDictWithTypes:
    @given(x=integers())
    def test_field_without_defaults(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = Example(x=x)
        result = asdict_with_types(obj)
        expected = {"x": _AsDictWithTypesElement(value=1, type_=int)}
        assert result == expected


class TestAsDictWithoutDefaultsAndReprWithoutDefaults:
    @given(x=integers())
    def test_field_without_defaults(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = Example(x=x)
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {"x": x}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
        repr_exp = f"Example(x={x})"
        assert repr_res == repr_exp

    def test_field_with_default(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example()
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
        repr_exp = "Example()"
        assert repr_res == repr_exp

    def test_field_with_dataframe(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DataFrame = field(default_factory=DataFrame)

        obj = Example()
        extra = {DataFrame: are_frames_equal}
        asdict_res = asdict_without_defaults(obj, globalns=globals(), extra=extra)
        asdict_exp = {}
        assert set(asdict_res) == set(asdict_exp)
        repr_res = repr_without_defaults(obj, globalns=globals(), extra=extra)
        repr_exp = "Example()"
        assert repr_res == repr_exp

    @given(x=integers())
    def test_final(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        def final(obj: type[Dataclass], mapping: StrMapping) -> StrMapping:
            return {f"[{get_class_name(obj)}]": mapping}

        obj = Example(x=x)
        result = asdict_without_defaults(obj, final=final)
        expected = {"[Example]": {"x": x}}
        assert result == expected

    @given(x=integers())
    def test_nested_with_recursive(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        obj = Outer(inner=Inner(x=x))
        asdict_res = asdict_without_defaults(obj, localns=locals(), recursive=True)
        asdict_exp = {"inner": {"x": x}}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals(), recursive=True)
        repr_exp = f"Outer(inner=Inner(x={x}))"
        assert repr_res == repr_exp

    @given(x=integers())
    def test_nested_without_recursive(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        obj = Outer(inner=Inner(x=x))
        asdict_res = asdict_without_defaults(obj, localns=locals())
        asdict_exp = {"inner": Inner(x=x)}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals())
        repr_exp = f"Outer(inner=TestAsDictWithoutDefaultsAndReprWithoutDefaults.test_nested_without_recursive.<locals>.Inner(x={x}))"
        assert repr_res == repr_exp

    def test_ib_async(self) -> None:
        fut = Future(
            conId=495512557,
            symbol="ES",
            lastTradeDateOrContractMonth="20241220",
            strike=0.0,
            right="",
            multiplier="50",
            exchange="",
            primaryExchange="",
            currency="USD",
            localSymbol="ESZ4",
            tradingClass="ES",
            includeExpired=False,
            secIdType="",
            secId="",
            description="",
            issuerId="",
            comboLegsDescrip="",
            comboLegs=[],
            deltaNeutralContract=None,
        )
        result = asdict_without_defaults(fut)
        expected = {
            "secType": "FUT",
            "conId": 495512557,
            "symbol": "ES",
            "lastTradeDateOrContractMonth": "20241220",
            "multiplier": "50",
            "currency": "USD",
            "localSymbol": "ESZ4",
            "tradingClass": "ES",
        }
        assert result == expected


class TestDataClassProtocol:
    def test_main(self) -> None:
        T = TypeVar("T", bound=Dataclass)

        def identity(x: T, /) -> T:
            return x

        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        _ = identity(Example())


class TestReplaceNonSentinel:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example()
        assert obj.x == 0
        obj1 = replace_non_sentinel(obj, x=1)
        assert obj1.x == 1
        obj2 = replace_non_sentinel(obj1, x=sentinel)
        assert obj2.x == 1

    def test_in_place(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example()
        assert obj.x == 0
        replace_non_sentinel(obj, x=1, in_place=True)
        assert obj.x == 1
        replace_non_sentinel(obj, x=sentinel, in_place=True)
        assert obj.x == 1


class TestReprWithoutDefaults:
    def test_overriding_repr(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

            @override
            def __repr__(self) -> str:
                return repr_without_defaults(self)

        obj = Example()
        result = repr(obj)
        expected = "Example()"
        assert result == expected

    @given(x=integers())
    def test_non_repr_field(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = field(default=0, repr=False)

        obj = Example(x=x)
        result = repr_without_defaults(obj)
        expected = "Example()"
        assert result == expected

    @given(x=integers(), y=integers())
    def test_ignore(self, *, x: int, y: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int
            y: int

        obj = Example(x=x, y=y)
        result = repr_without_defaults(obj, ignore="x")
        expected = f"Example(y={y})"
        assert result == expected


class TestYieldFields:
    def test_class_with_none_type_no_default(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None

        result = list(yield_fields(Example))
        expected = [_YieldFieldsClass(name="x", type_=NoneType, kw_only=True)]
        assert result == expected

    def test_class_with_none_type_and_default(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        result = list(yield_fields(Example))
        expected = [
            _YieldFieldsClass(name="x", type_=NoneType, default=None, kw_only=True)
        ]
        assert result == expected

    def test_class_with_int_type(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        result = list(yield_fields(Example))
        expected = [_YieldFieldsClass(name="x", type_=int, kw_only=True)]
        assert result == expected

    def test_class_with_list_int_type(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: list[int] = field(default_factory=list)

        results = list(yield_fields(Example))
        expected = [
            _YieldFieldsClass(
                name="x", type_=list[int], default_factory=list, kw_only=True
            )
        ]
        assert results == expected
        result = one(results)
        assert is_list_type(result.type_)
        assert get_args(result.type_) == (int,)

    def test_class_nested(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        results = list(yield_fields(Outer, localns=locals()))
        expected = [_YieldFieldsClass(name="inner", type_=Inner, kw_only=True)]
        assert results == expected
        result = one(results)
        assert result.type_ is Inner

    def test_class_literal(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit

        results = list(yield_fields(Example, globalns=globals()))
        expected = [
            _YieldFieldsClass(
                name="truth", type_=cast(type[Any], TruthLit), kw_only=True
            )
        ]
        assert results == expected
        result = one(results)
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    @mark.only
    def test_class_literal_nullable(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit | None = None

        results = list(yield_fields(Example, globalns=globals()))
        expected = [
            _YieldFieldsClass(
                name="truth",
                type_=cast(type[Any], TruthLit | None),
                default=None,
                kw_only=True,
            )
        ]
        assert results == expected
        result = one(results)
        assert is_optional_type(result.type_)
        assert get_args(result.type_) == ("true", "false")
