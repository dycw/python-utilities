from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from re import DOTALL
from types import NoneType
from typing import Any, Literal, cast

from hypothesis import given
from hypothesis.strategies import integers, lists, sampled_from
from ib_async import Future
from polars import DataFrame
from pytest import raises
from typing_extensions import override

from utilities.dataclasses import (
    YieldFieldsError,
    _MappingToDataclassCaseInsensitiveNonUniqueError,
    _MappingToDataclassEmptyError,
    _YieldFieldsClass,
    _YieldFieldsInstance,
    asdict_without_defaults,
    mapping_to_dataclass,
    replace_non_sentinel,
    repr_without_defaults,
    yield_fields,
)
from utilities.functions import get_class_name
from utilities.hypothesis import paths, text_ascii
from utilities.iterables import one
from utilities.orjson import OrjsonLogRecord
from utilities.polars import are_frames_equal
from utilities.sentinel import sentinel
from utilities.types import Dataclass, StrMapping
from utilities.typing import get_args, is_list_type, is_literal_type, is_optional_type

TruthLit = Literal["true", "false"]  # in 3.12, use type TruthLit = ...


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

    @given(y=integers())
    def test_nested_with_recursive(self, *, y: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner
            y: int

        obj = Outer(inner=Inner(), y=y)
        asdict_res = asdict_without_defaults(obj, localns=locals(), recursive=True)
        asdict_exp = {"inner": {}, "y": y}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals(), recursive=True)
        repr_exp = f"Outer(inner=Inner(), y={y})"
        assert repr_res == repr_exp

    @given(y=integers())
    def test_nested_without_recursive(self, *, y: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner
            y: int

        obj = Outer(inner=Inner(), y=y)
        asdict_res = asdict_without_defaults(obj, localns=locals())
        asdict_exp = {"inner": Inner(), "y": y}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals())
        repr_exp = f"Outer(inner=TestAsDictWithoutDefaultsAndReprWithoutDefaults.test_nested_without_recursive.<locals>.Inner(x=0), y={y})"
        assert repr_res == repr_exp

    @given(y=lists(integers()), z=integers())
    def test_nested_in_list_with_recursive(self, *, y: list[int], z: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: list[Inner]
            y: list[int]
            z: int

        obj = Outer(inner=[Inner()], y=y, z=z)
        asdict_res = asdict_without_defaults(obj, localns=locals(), recursive=True)
        asdict_exp = {"inner": [{}], "y": y, "z": z}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals(), recursive=True)
        repr_exp = f"Outer(inner=[Inner()], y={y}, z={z})"
        assert repr_res == repr_exp

    @given(y=lists(integers()), z=integers())
    def test_nested_in_list_without_recursive(self, *, y: list[int], z: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int = 0

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: list[Inner]
            y: list[int]
            z: int

        obj = Outer(inner=[Inner()], y=y, z=z)
        asdict_res = asdict_without_defaults(obj, localns=locals())
        asdict_exp = {"inner": [Inner(x=0)], "y": y, "z": z}
        assert asdict_res == asdict_exp
        repr_res = repr_without_defaults(obj, localns=locals())
        repr_exp = f"Outer(inner=[TestAsDictWithoutDefaultsAndReprWithoutDefaults.test_nested_in_list_without_recursive.<locals>.Inner(x=0)], y={y}, z={z})"
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


class TestMappingToDataclass:
    @given(value=integers())
    def test_case_sensitive(self, *, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = mapping_to_dataclass(Example, {"x": value})
        expected = Example(x=value)
        assert obj == expected

    @given(key=sampled_from(["x", "X"]), value=integers())
    def test_case_insensitive(self, *, key: str, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = mapping_to_dataclass(Example, {key: value}, case_sensitive=False)
        expected = Example(x=value)
        assert obj == expected

    @given(value=paths())
    def test_path(self, *, value: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: Path

        obj = mapping_to_dataclass(Example, {"x": value})
        expected = Example(x=value)
        assert obj == expected

    @given(value=text_ascii())
    def test_post(self, *, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: str

        obj = mapping_to_dataclass(Example, {"x": value}, post=lambda _, x: x.upper())
        expected = Example(x=value.upper())
        assert obj == expected

    @given(value=integers())
    def test_error_case_sensitive_empty_error(self, *, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        with raises(
            _MappingToDataclassEmptyError, match=r"Mapping .* does not contain 'x'"
        ):
            _ = mapping_to_dataclass(Example, {"X": value})

    @given(value=integers())
    def test_error_case_insensitive_empty_error(self, *, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        with raises(
            _MappingToDataclassEmptyError,
            match=r"Mapping .* does not contain 'x' \(modulo case\)",
        ):
            _ = mapping_to_dataclass(Example, {"y": value}, case_sensitive=False)

    @given(value1=integers(), value2=integers())
    def test_error_case_insensitive_non_unique_error(
        self, *, value1: int, value2: int
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        with raises(
            _MappingToDataclassCaseInsensitiveNonUniqueError,
            match=re.compile(
                r"Mapping .* must contain 'x' exactly once \(modulo case\); got 'x', 'X' and perhaps more",
                flags=DOTALL,
            ),
        ):
            _ = mapping_to_dataclass(
                Example, {"x": value1, "X": value2}, case_sensitive=False
            )


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
            none: None

        result = one(yield_fields(Example))
        expected = _YieldFieldsClass(name="none", type_=NoneType, kw_only=True)
        assert result == expected

    def test_class_with_none_type_and_default(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        result = one(yield_fields(Example))
        expected = _YieldFieldsClass(
            name="x", type_=NoneType, default=None, kw_only=True
        )
        assert result == expected

    def test_class_with_int_type(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        result = one(yield_fields(Example))
        expected = _YieldFieldsClass(name="x", type_=int, kw_only=True)
        assert result == expected

    def test_class_with_list_int_type(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: list[int] = field(default_factory=list)

        result = one(yield_fields(Example))
        expected = _YieldFieldsClass(
            name="x", type_=list[int], default_factory=list, kw_only=True
        )
        assert result == expected
        assert is_list_type(result.type_)
        assert get_args(result.type_) == (int,)

    def test_class_nested(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Inner:
            x: int

        @dataclass(kw_only=True, slots=True)
        class Outer:
            inner: Inner

        result = one(yield_fields(Outer, localns=locals()))
        expected = _YieldFieldsClass(name="inner", type_=Inner, kw_only=True)
        assert result == expected
        assert result.type_ is Inner

    def test_class_literal(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit

        result = one(yield_fields(Example, globalns=globals()))
        expected = _YieldFieldsClass(name="truth", type_=TruthLit, kw_only=True)
        assert result == expected
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    def test_class_literal_nullable(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            truth: TruthLit | None = None

        result = one(yield_fields(Example, globalns=globals()))
        expected = _YieldFieldsClass(
            name="truth", type_=TruthLit | None, default=None, kw_only=True
        )
        assert result == expected
        assert is_optional_type(result.type_)
        args = get_args(result.type_)
        assert args == (Literal["true", "false"],)
        arg = one(args)
        assert get_args(arg) == ("true", "false")

    def test_class_orjson_log_record(self) -> None:
        result = list(yield_fields(OrjsonLogRecord, globalns=globals()))
        exp_head = [
            _YieldFieldsClass(name="name", type_=str, kw_only=True),
            _YieldFieldsClass(name="message", type_=str, kw_only=True),
            _YieldFieldsClass(name="level", type_=int, kw_only=True),
        ]
        assert result[:3] == exp_head
        exp_tail = [
            _YieldFieldsClass(
                name="extra", type_=StrMapping | None, default=None, kw_only=True
            ),
            _YieldFieldsClass(
                name="log_file", type_=Path | None, default=None, kw_only=True
            ),
            _YieldFieldsClass(
                name="log_file_line_num", type_=int | None, default=None, kw_only=True
            ),
        ]
        assert result[-3:] == exp_tail

    def test_instance(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: None = None

        obj = Example()
        result = one(yield_fields(obj))
        expected = _YieldFieldsInstance(
            name="x", value=None, type_=NoneType, default=None, kw_only=True
        )
        assert result == expected

    @given(x=integers())
    def test_instance_is_default_value_with_no_default(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = Example(x=x)
        field = one(yield_fields(obj))
        assert not field.equals_default()

    @given(x=integers())
    def test_instance_is_default_value_with_default(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example(x=x)
        field = one(yield_fields(obj))
        result = field.equals_default()
        expected = x == 0
        assert result is expected

    @given(x=lists(integers()))
    def test_instance_is_default_value_with_default_factory(
        self, *, x: list[int]
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: list[int] = field(default_factory=list)

        obj = Example(x=x)
        fld = one(yield_fields(obj))
        result = fld.equals_default()
        expected = x == []
        assert result is expected

    def test_error(self) -> None:
        with raises(
            YieldFieldsError,
            match="Object must be a dataclass instance or class; got None",
        ):
            _ = list(yield_fields(cast(Any, None)))
