from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from re import DOTALL
from types import NoneType
from typing import Any, Literal, cast, override

from hypothesis import given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    lists,
    none,
    sampled_from,
)
from polars import DataFrame
from pytest import raises

from tests.test_typing_funcs.no_future import (
    DataClassNoFutureInt,
    DataClassNoFutureIntDefault,
)
from tests.test_typing_funcs.with_future import (
    DataClassFutureInt,
    DataClassFutureIntDefault,
    DataClassFutureIntNullable,
    DataClassFutureListInts,
    DataClassFutureListIntsDefault,
    DataClassFutureLiteral,
    DataClassFutureLiteralNullable,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
    DataClassFutureNoneDefault,
    DataClassFuturePath,
    DataClassFutureStr,
    DataClassFutureTypeLiteral,
    DataClassFutureTypeLiteralNullable,
)
from utilities.dataclasses import (
    YieldFieldsError,
    _MappingToDataclassCaseInsensitiveNonUniqueError,
    _MappingToDataclassEmptyError,
    _YieldFieldsClass,
    _YieldFieldsInstance,
    dataclass_repr,
    dataclass_to_dict,
    mapping_to_dataclass,
    replace_non_sentinel,
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


class TestDataclassToDictAndDataclassRepr:
    @given(x=integers(), defaults=booleans())
    def test_field_without_defaults(self, *, x: int, defaults: bool) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int

        obj = Example(x=x)
        dict_res = dataclass_to_dict(obj, defaults=defaults)
        dict_exp = {"x": x}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, defaults=defaults)
        repr_exp = f"Example(x={x})"
        assert repr_res == repr_exp

    @given(x=integers())
    def test_field_with_default_included(self, *, x: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example(x=x)
        dict_res = dataclass_to_dict(obj, defaults=True)
        dict_exp = {"x": x}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, defaults=True)
        repr_exp = f"Example(x={x})"
        assert repr_res == repr_exp

    def test_field_with_default_dropped(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

        obj = Example()
        dict_res = dataclass_to_dict(obj)
        dict_exp = {}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj)
        repr_exp = "Example()"
        assert repr_res == repr_exp

    def test_field_with_dataframe_included(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DataFrame = field(default_factory=DataFrame)

        obj = Example()
        extra = {DataFrame: are_frames_equal}
        dict_res = dataclass_to_dict(
            obj, globalns=globals(), extra=extra, defaults=True
        )
        dict_exp = {"x": DataFrame()}
        assert set(dict_res) == set(dict_exp)
        repr_res = dataclass_repr(obj, globalns=globals(), extra=extra, defaults=True)
        repr_exp = f"Example(x={DataFrame()})"
        assert repr_res == repr_exp

    def test_field_with_dataframe_dropped(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: DataFrame = field(default_factory=DataFrame)

        obj = Example()
        extra = {DataFrame: are_frames_equal}
        dict_res = dataclass_to_dict(obj, globalns=globals(), extra=extra)
        dict_exp = {}
        assert set(dict_res) == set(dict_exp)
        repr_res = dataclass_repr(obj, globalns=globals(), extra=extra)
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
        result = dataclass_to_dict(obj, final=final)
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
        dict_res = dataclass_to_dict(obj, localns=locals(), recursive=True)
        dict_exp = {"inner": {}, "y": y}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, localns=locals(), recursive=True)
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
        dict_res = dataclass_to_dict(obj, localns=locals())
        dict_exp = {"inner": Inner(), "y": y}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, localns=locals())
        repr_exp = f"Outer(inner=TestDataclassToDictAndDataclassRepr.test_nested_without_recursive.<locals>.Inner(x=0), y={y})"
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
        dict_res = dataclass_to_dict(obj, localns=locals(), recursive=True)
        dict_exp = {"inner": [{}], "y": y, "z": z}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, localns=locals(), recursive=True)
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
        dict_res = dataclass_to_dict(obj, localns=locals())
        dict_exp = {"inner": [Inner(x=0)], "y": y, "z": z}
        assert dict_res == dict_exp
        repr_res = dataclass_repr(obj, localns=locals())
        repr_exp = f"Outer(inner=[TestDataclassToDictAndDataclassRepr.test_nested_in_list_without_recursive.<locals>.Inner(x=0)], y={y}, z={z})"
        assert repr_res == repr_exp


class TestMappingToDataclass:
    @given(int_=integers())
    def test_int_case_sensitive(self, *, int_: int) -> None:
        obj = mapping_to_dataclass(DataClassFutureInt, {"int_": int_})
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(key=sampled_from(["int_", "INT_"]), int_=integers())
    def test_int_case_insensitive(self, *, key: str, int_: int) -> None:
        obj = mapping_to_dataclass(
            DataClassFutureInt, {key: int_}, case_sensitive=False
        )
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(data=data(), int_=integers() | none())
    def test_int_nullable(self, *, data: DataObject, int_: int | None) -> None:
        if int_ is None:
            mapping = data.draw(sampled_from([{"int_": int_}, {}]))
        else:
            mapping = {"int_": int_}
        obj = mapping_to_dataclass(DataClassFutureIntNullable, mapping)
        expected = DataClassFutureIntNullable(int_=int_)
        assert obj == expected

    @given(data=data(), ints=lists(integers()))
    def test_list_ints_nullable(self, *, data: DataObject, ints: list[int]) -> None:
        if len(ints) == 0:
            mapping = data.draw(sampled_from([{"ints": ints}, {}]))
        else:
            mapping = {"ints": ints}
        obj = mapping_to_dataclass(DataClassFutureListIntsDefault, mapping)
        expected = DataClassFutureListIntsDefault(ints=ints)
        assert obj == expected

    @given(value=paths())
    def test_path(self, *, value: Path) -> None:
        obj = mapping_to_dataclass(DataClassFuturePath, {"path": value})
        expected = DataClassFuturePath(path=value)
        assert obj == expected

    @given(value=text_ascii())
    def test_post(self, *, value: str) -> None:
        obj = mapping_to_dataclass(
            DataClassFutureStr, {"str_": value}, post=lambda _, x: x.upper()
        )
        expected = DataClassFutureStr(str_=value.upper())
        assert obj == expected

    @given(value=integers())
    def test_error_case_sensitive_empty_error(self, *, value: int) -> None:
        with raises(
            _MappingToDataclassEmptyError, match=r"Mapping .* does not contain 'int_'"
        ):
            _ = mapping_to_dataclass(DataClassFutureInt, {"INT_": value})

    @given(value=integers())
    def test_error_case_insensitive_empty_error(self, *, value: int) -> None:
        with raises(
            _MappingToDataclassEmptyError,
            match=r"Mapping .* does not contain 'int_' \(modulo case\)",
        ):
            _ = mapping_to_dataclass(
                DataClassFutureInt, {"other": value}, case_sensitive=False
            )

    @given(value1=integers(), value2=integers())
    def test_error_case_insensitive_non_unique_error(
        self, *, value1: int, value2: int
    ) -> None:
        with raises(
            _MappingToDataclassCaseInsensitiveNonUniqueError,
            match=re.compile(
                r"Mapping .* must contain 'int_' exactly once \(modulo case\); got 'int_', 'INT_' and perhaps more",
                flags=DOTALL,
            ),
        ):
            _ = mapping_to_dataclass(
                DataClassFutureInt,
                {"int_": value1, "INT_": value2},
                case_sensitive=False,
            )


class TestReplaceNonSentinel:
    def test_main(self) -> None:
        obj = DataClassFutureIntDefault()
        assert obj.int_ == 0
        obj1 = replace_non_sentinel(obj, int_=1)
        assert obj1.int_ == 1
        obj2 = replace_non_sentinel(obj1, int_=sentinel)
        assert obj2.int_ == 1

    def test_in_place(self) -> None:
        obj = DataClassFutureIntDefault()
        assert obj.int_ == 0
        replace_non_sentinel(obj, int_=1, in_place=True)
        assert obj.int_ == 1
        replace_non_sentinel(obj, int_=sentinel, in_place=True)
        assert obj.int_ == 1


class TestReprWithoutDefaults:
    def test_overriding_repr(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example:
            x: int = 0

            @override
            def __repr__(self) -> str:
                return dataclass_repr(self)

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
        result = dataclass_repr(obj)
        expected = "Example()"
        assert result == expected


class TestYieldFields:
    def test_class_no_future_int(self) -> None:
        result = one(yield_fields(DataClassNoFutureInt))
        expected = _YieldFieldsClass(name="int_", type_=int, kw_only=True)
        assert result == expected

    def test_class_no_future_int_default(self) -> None:
        result = one(yield_fields(DataClassNoFutureIntDefault))
        expected = _YieldFieldsClass(name="int_", type_=int, default=0, kw_only=True)
        assert result == expected

    def test_class_future_none(self) -> None:
        result = one(yield_fields(DataClassFutureNone))
        expected = _YieldFieldsClass(name="none", type_=NoneType, kw_only=True)
        assert result == expected

    def test_class_future_non_default(self) -> None:
        result = one(yield_fields(DataClassFutureNoneDefault))
        expected = _YieldFieldsClass(
            name="none", type_=NoneType, default=None, kw_only=True
        )
        assert result == expected

    def test_class_future_int(self) -> None:
        result = one(yield_fields(DataClassFutureInt))
        expected = _YieldFieldsClass(name="int_", type_=int, kw_only=True)
        assert result == expected

    def test_class_future_list_ints(self) -> None:
        result = one(yield_fields(DataClassFutureListInts))
        expected = _YieldFieldsClass(name="ints", type_=list[int], kw_only=True)
        assert result == expected
        assert is_list_type(result.type_)
        assert get_args(result.type_) == (int,)

    def test_class_future_list_ints_default(self) -> None:
        result = one(yield_fields(DataClassFutureListIntsDefault))
        expected = _YieldFieldsClass(
            name="ints", type_=list[int], default_factory=list, kw_only=True
        )
        assert result == expected
        assert is_list_type(result.type_)
        assert get_args(result.type_) == (int,)

    def test_class_future_literal(self) -> None:
        result = one(yield_fields(DataClassFutureLiteral))
        expected = _YieldFieldsClass(name="truth", type_=TruthLit, kw_only=True)
        assert result == expected
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    def test_class_future_literal_nullable(self) -> None:
        result = one(yield_fields(DataClassFutureLiteralNullable))
        expected = _YieldFieldsClass(
            name="truth", type_=TruthLit | None, default=None, kw_only=True
        )
        assert result == expected
        assert is_optional_type(result.type_)
        args = get_args(result.type_)
        assert args == (Literal["true", "false"],)
        arg = one(args)
        assert get_args(arg) == ("true", "false")

    def test_class_future_nested(self) -> None:
        result = one(
            yield_fields(DataClassFutureNestedOuterFirstOuter, globalns=globals())
        )
        expected = _YieldFieldsClass(
            name="inner", type_=DataClassFutureNestedOuterFirstInner, kw_only=True
        )
        assert result == expected
        assert result.type_ is DataClassFutureNestedOuterFirstInner

    def test_class_future_type_literal(self) -> None:
        result = one(yield_fields(DataClassFutureTypeLiteral, globalns=globals()))
        expected = _YieldFieldsClass(name="truth", type_=TruthLit, kw_only=True)
        assert result == expected
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    def test_class_future_type_literal_nullable(self) -> None:
        result = one(
            yield_fields(DataClassFutureTypeLiteralNullable, globalns=globals())
        )
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

    @given(int_=integers())
    def test_instance_no_future_int(self, *, int_: int) -> None:
        obj = DataClassNoFutureInt(int_=int_)
        result = one(yield_fields(obj))
        expected = _YieldFieldsInstance(
            name="int_", value=int_, type_=int, kw_only=True
        )
        assert result == expected

    @given(int_=integers())
    def test_instance_no_future_int_default(self, *, int_: int) -> None:
        obj = DataClassNoFutureIntDefault(int_=int_)
        result = one(yield_fields(obj))
        expected = _YieldFieldsInstance(
            name="int_", value=int_, type_=int, default=0, kw_only=True
        )
        assert result == expected

    def test_instance_future_none_default(self) -> None:
        obj = DataClassFutureNoneDefault()
        result = one(yield_fields(obj))
        expected = _YieldFieldsInstance(
            name="none", value=None, type_=NoneType, default=None, kw_only=True
        )
        assert result == expected

    @given(int_=integers())
    def test_instance_future_int(self, *, int_: int) -> None:
        obj = DataClassFutureInt(int_=int_)
        field = one(yield_fields(obj))
        assert not field.equals_default()
        assert field.keep()
        assert not field.keep(include=[])
        assert not field.keep(exclude=["int_"])

    @given(int_=integers())
    def test_instance_with_default_equals_default(self, *, int_: int) -> None:
        obj = DataClassFutureIntDefault(int_=int_)
        field = one(yield_fields(obj))
        result = field.equals_default()
        expected = int_ == 0
        assert result is expected
        assert field.keep() is not expected
        assert field.keep(defaults=True)

    @given(ints=lists(integers()))
    def test_instance_future_list_ints_default(self, *, ints: list[int]) -> None:
        obj = DataClassFutureListIntsDefault(ints=ints)
        field = one(yield_fields(obj))
        result = field.equals_default()
        expected = ints == []
        assert result is expected
        assert field.keep() is not expected

    def test_error(self) -> None:
        with raises(
            YieldFieldsError,
            match="Object must be a dataclass instance or class; got None",
        ):
            _ = list(yield_fields(cast("Any", None)))
