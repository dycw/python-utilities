from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from types import NoneType
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from hypothesis import given
from hypothesis.strategies import integers, lists
from ib_async import Future
from polars import DataFrame
from pytest import mark, param, raises
from typing_extensions import override

from utilities.dataclasses import (
    Dataclass,
    GetDataClassClassError,
    asdict_without_defaults,
    get_dataclass_class,
    get_dataclass_fields,
    is_dataclass_class,
    is_dataclass_instance,
    replace_non_sentinel,
    repr_without_defaults,
    yield_field_names,
)
from utilities.functions import get_class_name
from utilities.sentinel import sentinel

if TYPE_CHECKING:
    from utilities.types import StrMapping


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

    def test_field_with_default_and_value_equal(self) -> None:
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

    @given(x=integers().filter(lambda x: x != 0))
    def test_field_with_default_and_value_not_equal(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example(x=x)
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {"x": x}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
        repr_exp = f"Example(x={x})"
        assert repr_res == repr_exp

    def test_field_with_default_factory_and_value_equal(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: list[int] = field(default_factory=list)

        obj = Example()
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
        repr_exp = "Example()"
        assert repr_res == repr_exp

    @given(x=lists(integers(), min_size=1))
    def test_field_with_default_factory_and_value_not_equal(
        self, *, x: list[int]
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: list[int] = field(default_factory=list)

        obj = Example(x=x)
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {"x": x}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
        repr_exp = f"Example(x={x})"
        assert repr_res == repr_exp

    def test_field_with_dataframe(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DataFrame = field(default_factory=DataFrame)

        obj = Example()
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {"x": DataFrame()}
        assert set(asdict_res) == set(asdict_exp)
        repr_res = repr_without_defaults(obj)
        repr_exp = f"Example(x={DataFrame()})"
        assert repr_res == repr_exp

    @given(x=integers())
    def test_final(self, *, x: int) -> None:
        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
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
        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Outer:
            inner: Inner

        obj = Outer(inner=Inner(x=x))
        asdict_res = asdict_without_defaults(obj, recursive=True)
        asdict_exp = {"inner": {"x": x}}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, recursive=True)
        repr_exp = f"Outer(inner=Inner(x={x}))"
        assert repr_res == repr_exp

    @given(x=integers())
    def test_nested_without_recursive(self, *, x: int) -> None:
        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(unsafe_hash=True, kw_only=True, slots=True)
        class Outer:
            inner: Inner

        obj = Outer(inner=Inner(x=x))
        asdict_res = asdict_without_defaults(obj)
        asdict_exp = {"inner": Inner(x=x)}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj)
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


class TestGetDataClassClass:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert get_dataclass_class(obj) is Example

    def test_error(self) -> None:
        with raises(
            GetDataClassClassError,
            match="Object must be a dataclass instance or class; got None",
        ):
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
    def test_with_default_and_value_equal(self) -> None:
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

    def test_field_with_dataframe(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DataFrame = field(default_factory=DataFrame)

            @override
            def __repr__(self) -> str:
                return repr_without_defaults(self)

        obj = Example()
        result = repr(obj)
        expected = f"Example(x={DataFrame()})"
        assert result == expected


class TestYieldDataClassFieldNames:
    def test_main(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        for obj in [Example(), Example]:
            assert list(yield_field_names(obj)) == ["x"]
