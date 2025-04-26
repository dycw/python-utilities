from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import NoneType
from typing import Any, cast, override

from hypothesis import given
from hypothesis.strategies import booleans, integers, lists, sampled_from
from polars import DataFrame
from pytest import raises

from tests.test_typing_funcs.no_future import (
    DataClassNoFutureInt,
    DataClassNoFutureIntDefault,
)
from tests.test_typing_funcs.with_future import (
    DataClassFutureInt,
    DataClassFutureIntDefault,
    DataClassFutureIntLowerAndUpper,
    DataClassFutureIntOneAndTwo,
    DataClassFutureListInts,
    DataClassFutureListIntsDefault,
    DataClassFutureLiteral,
    DataClassFutureLiteralNullable,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
    DataClassFutureNoneDefault,
    DataClassFutureTypeLiteral,
    DataClassFutureTypeLiteralNullable,
    TrueOrFalseFutureLit,
    TrueOrFalseFutureTypeLit,
)
from utilities.dataclasses import (
    MappingToDataclassError,
    OneFieldEmptyError,
    OneFieldNonUniqueError,
    StrMappingToFieldMappingError,
    YieldFieldsError,
    _parse_dataclass_split_key_value_pairs,
    _ParseDataClassParseValueError,
    _ParseDataClassSplitKeyValuePairsDuplicateKeysError,
    _ParseDataClassSplitKeyValuePairsSplitError,
    _YieldFieldsClass,
    _YieldFieldsInstance,
    dataclass_repr,
    dataclass_to_dict,
    mapping_to_dataclass,
    one_field,
    parse_dataclass,
    replace_non_sentinel,
    serialize_dataclass,
    str_mapping_to_field_mapping,
    yield_fields,
)
from utilities.functions import get_class_name
from utilities.iterables import one
from utilities.orjson import OrjsonLogRecord
from utilities.polars import are_frames_equal
from utilities.sentinel import sentinel
from utilities.types import Dataclass, StrMapping
from utilities.typing import get_args, is_list_type, is_literal_type, is_optional_type


class TestDataClassRepr:
    def test_overriding_repr(self) -> None:
        @dataclass(kw_only=True, slots=True)
        class Example(DataClassFutureIntDefault):
            @override
            def __repr__(self) -> str:
                return dataclass_repr(self)

        obj = Example()
        result = repr(obj)
        expected = "Example()"
        assert result == expected

    def test_overriding_repr_defaults(self) -> None:
        @dataclass(kw_only=True)
        class Example(DataClassFutureIntDefault):
            @override
            def __repr__(self) -> str:
                return dataclass_repr(self, defaults=True)

        obj = Example()
        result = repr(obj)
        expected = "Example(int_=0)"
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
    @given(key=sampled_from(["int_", "INT_"]), int_=integers())
    def test_exact_match_case_insensitive(self, *, key: str, int_: int) -> None:
        obj = mapping_to_dataclass(DataClassFutureInt, {key: int_})
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(key=sampled_from(["in", "IN"]), int_=integers())
    def test_head_case_insensitive(self, *, key: str, int_: int) -> None:
        obj = mapping_to_dataclass(DataClassFutureInt, {key: int_}, head=True)
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(int_=integers())
    def test_exact_match_case_sensitive(self, *, int_: int) -> None:
        obj = mapping_to_dataclass(
            DataClassFutureInt, {"int_": int_}, case_sensitive=True
        )
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(int_=integers())
    def test_head_case_sensitive(self, *, int_: int) -> None:
        obj = mapping_to_dataclass(
            DataClassFutureInt, {"int": int_}, head=True, case_sensitive=True
        )
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    @given(int_=integers())
    def test_extra_key(self, *, int_: int) -> None:
        obj = mapping_to_dataclass(
            DataClassFutureInt, {"int_": int_, "extra": int_}, allow_extra=True
        )
        expected = DataClassFutureInt(int_=int_)
        assert obj == expected

    def test_error(self) -> None:
        with raises(
            MappingToDataclassError,
            match="Unable to construct 'DataClassFutureInt'; missing values for 'int_'",
        ):
            _ = mapping_to_dataclass(DataClassFutureInt, {})


class TestOneField:
    def test_error_exact_match_case_insensitive_empty_error(self) -> None:
        with raises(
            OneFieldEmptyError,
            match=r"Dataclass 'DataClassFutureInt' does not contain a field 'invalid' \(modulo case\)",
        ):
            _ = one_field(DataClassFutureInt, "invalid")

    def test_error_exact_match_case_insensitive_non_unique_error(self) -> None:
        with raises(
            OneFieldNonUniqueError,
            match=r"Dataclass 'DataClassFutureIntLowerAndUpper' must contain field 'int_' exactly once \(modulo case\); got 'int_', 'INT_' and perhaps more",
        ):
            _ = one_field(DataClassFutureIntLowerAndUpper, "int_")

    def test_error_head_case_insensitive_empty_error(self) -> None:
        with raises(
            OneFieldEmptyError,
            match=r"Dataclass 'DataClassFutureInt' does not contain any field starting with 'invalid' \(modulo case\)",
        ):
            _ = one_field(DataClassFutureInt, "invalid", head=True)

    def test_error_head_case_insensitive_non_unique_error(self) -> None:
        with raises(
            OneFieldNonUniqueError,
            match=r"Dataclass 'DataClassFutureIntOneAndTwo' must contain exactly one field starting with 'int' \(modulo case\); got 'int1', 'int2' and perhaps more",
        ):
            _ = one_field(DataClassFutureIntOneAndTwo, "int", head=True)

    def test_error_exact_match_case_sensitive_empty_error(self) -> None:
        with raises(
            OneFieldEmptyError,
            match=r"Dataclass 'DataClassFutureInt' does not contain a field 'INT_'",
        ):
            _ = one_field(DataClassFutureInt, "INT_", case_sensitive=True)

    # there is no head=False, case_sensitive=True, non-unique case

    def test_error_head_case_sensitive_empty_error(self) -> None:
        with raises(
            OneFieldEmptyError,
            match=r"Dataclass 'DataClassFutureInt' does not contain any field starting with 'INT_'",
        ):
            _ = one_field(DataClassFutureInt, "INT_", head=True, case_sensitive=True)

    def test_error_head_case_sensitive_non_unique_error(self) -> None:
        with raises(
            OneFieldNonUniqueError,
            match=r"Dataclass 'DataClassFutureIntOneAndTwo' must contain exactly one field starting with 'int'; got 'int1', 'int2' and perhaps more",
        ):
            _ = one_field(
                DataClassFutureIntOneAndTwo, "int", head=True, case_sensitive=True
            )


class TestParseDataClassSplitKeyValuePairs:
    @given(text=sampled_from(["a=1,b=22,c=333", "{a=1,b=22,c=333}"]))
    def test_main(self, *, text: str) -> None:
        result = _parse_dataclass_split_key_value_pairs(text, DataClassFutureInt)
        expected = {"a": "1", "b": "22", "c": "333"}
        assert result == expected


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


class TestSerializeAndParseDataClass:
    @given(int_=integers())
    def test_main_future_int(self, *, int_: int) -> None:
        obj = DataClassFutureInt(int_=int_)
        serialized = serialize_dataclass(obj)
        result = parse_dataclass(serialized, DataClassFutureInt)
        assert result == obj

    def test_main_future_int_default(self) -> None:
        obj = DataClassFutureIntDefault()
        serialized = serialize_dataclass(obj)
        result = parse_dataclass(serialized, DataClassFutureIntDefault)
        assert result == obj

    @given(int_=integers())
    def test_extra_type(self, *, int_: int) -> None:
        obj = DataClassFutureNestedOuterFirstOuter(
            inner=DataClassFutureNestedOuterFirstInner(int_=int_)
        )

        def serializer(obj: DataClassFutureNestedOuterFirstInner, /) -> str:
            return serialize_dataclass(obj)

        serialized = serialize_dataclass(
            obj, extra_serializers={DataClassFutureNestedOuterFirstInner: serializer}
        )
        result = parse_dataclass(
            serialized,
            DataClassFutureNestedOuterFirstOuter,
            globalns=globals(),
            extra_parsers={
                DataClassFutureNestedOuterFirstInner: lambda text: parse_dataclass(
                    text, DataClassFutureNestedOuterFirstInner
                )
            },
        )
        assert result == obj

    @given(key=sampled_from(["int_", "INT_"]), int_=integers())
    def test_parse_text_case_insensitive(self, *, key: str, int_: int) -> None:
        result = parse_dataclass(f"{key}={int_}", DataClassFutureInt)
        expected = DataClassFutureInt(int_=int_)
        assert result == expected

    @given(int_=integers())
    def test_parse_text_case_sensitive(self, *, int_: int) -> None:
        result = parse_dataclass(
            f"int_={int_}", DataClassFutureInt, case_sensitive=True
        )
        expected = DataClassFutureInt(int_=int_)
        assert result == expected

    @given(key=sampled_from(["int_", "INT_"]), int_=integers())
    def test_parse_mapping_case_insensitive(self, *, key: str, int_: int) -> None:
        result = parse_dataclass({key: str(int_)}, DataClassFutureInt)
        expected = DataClassFutureInt(int_=int_)
        assert result == expected

    @given(int_=integers())
    def test_parse_mapping_case_sensitive(self, *, int_: int) -> None:
        result = parse_dataclass(
            {"int_": str(int_)}, DataClassFutureInt, case_sensitive=True
        )
        expected = DataClassFutureInt(int_=int_)
        assert result == expected

    def test_parser_split_key_value_pairs_split(self) -> None:
        with raises(
            _ParseDataClassSplitKeyValuePairsSplitError,
            match="Unable to construct 'DataClassFutureInt'; failed to split key-value pair 'bbb'",
        ):
            _ = parse_dataclass("a=1,bbb,c=333", DataClassFutureInt)

    def test_error_parse_split_key_value_pairs_duplicate(self) -> None:
        with raises(
            _ParseDataClassSplitKeyValuePairsDuplicateKeysError,
            match=r"Unable to construct 'DataClassFutureInt' since there are duplicate keys; got \{'b': 2\}",
        ):
            _ = parse_dataclass("a=1,b=22a,b=22b,c=3", DataClassFutureInt)

    def test_error_parse_value(self) -> None:
        with raises(
            _ParseDataClassParseValueError,
            match="Unable to construct 'DataClassFutureInt'; unable to parse field 'int_' of type <class 'int'>; got 'invalid'",
        ):
            _ = parse_dataclass("int_=invalid", DataClassFutureInt)


class TestStrMappingToFieldMapping:
    @given(key=sampled_from(["int_", "INT_"]), int_=integers())
    def test_main_text_case_insensitive(self, *, key: str, int_: int) -> None:
        result = str_mapping_to_field_mapping(DataClassFutureInt, {key: int_})
        assert len(result) == 1
        assert one(result) == one(yield_fields(DataClassFutureInt))
        assert one(result.values()) == int_

    @given(key=sampled_from(["in", "IN"]), int_=integers())
    def test_head_case_insensitive(self, *, key: str, int_: int) -> None:
        result = str_mapping_to_field_mapping(
            DataClassFutureInt, {key: int_}, head=True
        )
        assert len(result) == 1
        assert one(result) == one(yield_fields(DataClassFutureInt))
        assert one(result.values()) == int_

    @given(int_=integers())
    def test_exact_match_case_sensitive(self, *, int_: int) -> None:
        result = str_mapping_to_field_mapping(
            DataClassFutureInt, {"int_": int_}, case_sensitive=True
        )
        assert len(result) == 1
        assert one(result) == one(yield_fields(DataClassFutureInt))
        assert one(result.values()) == int_

    @given(int_=integers())
    def test_head_case_sensitive(self, *, int_: int) -> None:
        result = str_mapping_to_field_mapping(
            DataClassFutureInt, {"int": int_}, head=True, case_sensitive=True
        )
        assert len(result) == 1
        assert one(result) == one(yield_fields(DataClassFutureInt))
        assert one(result.values()) == int_

    @given(int_=integers())
    def test_extra_key(self, *, int_: int) -> None:
        result = str_mapping_to_field_mapping(
            DataClassFutureInt, {"int_": int_, "extra": int_}, allow_extra=True
        )
        assert len(result) == 1
        assert one(result) == one(yield_fields(DataClassFutureInt))
        assert one(result.values()) == int_

    @given(int_=integers())
    def test_error_exact_match_case_insensitive(self, *, int_: int) -> None:
        with raises(
            StrMappingToFieldMappingError,
            match=r"Dataclass 'DataClassFutureInt' does not contain a field 'invalid' \(modulo case\)",
        ):
            _ = str_mapping_to_field_mapping(
                DataClassFutureInt, {"int_": int_, "invalid": int_}
            )

    @given(int_=integers())
    def test_error_exact_match_case_sensitive(self, *, int_: int) -> None:
        with raises(
            StrMappingToFieldMappingError,
            match=r"Dataclass 'DataClassFutureInt' does not contain a field 'extra'",
        ):
            _ = str_mapping_to_field_mapping(
                DataClassFutureInt, {"int_": int_, "extra": int_}, case_sensitive=True
            )

    @given(int_=integers())
    def test_error_head_case_insensitive(self, *, int_: int) -> None:
        with raises(
            StrMappingToFieldMappingError,
            match=r"Dataclass .* does not contain any field starting with 'invalid' \(modulo case\)",
        ):
            _ = str_mapping_to_field_mapping(
                DataClassFutureInt, {"invalid": int_}, head=True
            )

    @given(int_=integers())
    def test_error_head_case_sensitive(self, *, int_: int) -> None:
        with raises(
            StrMappingToFieldMappingError,
            match=r"Dataclass .* does not contain any field starting with 'invalid'",
        ):
            _ = str_mapping_to_field_mapping(
                DataClassFutureInt, {"invalid": int_}, head=True, case_sensitive=True
            )


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
        result = one(yield_fields(DataClassFutureLiteral, globalns=globals()))
        expected = _YieldFieldsClass(
            name="truth", type_=TrueOrFalseFutureLit, kw_only=True
        )
        assert result == expected
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    def test_class_future_literal_nullable(self) -> None:
        result = one(yield_fields(DataClassFutureLiteralNullable, globalns=globals()))
        expected = _YieldFieldsClass(
            name="truth", type_=TrueOrFalseFutureLit | None, default=None, kw_only=True
        )
        assert result == expected
        assert is_optional_type(result.type_)
        args = get_args(result.type_)
        assert args == (TrueOrFalseFutureLit,)
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
        result = one(
            yield_fields(
                DataClassFutureTypeLiteral, globalns=globals(), localns=locals()
            )
        )
        expected = _YieldFieldsClass(
            name="truth", type_=TrueOrFalseFutureTypeLit, kw_only=True
        )
        assert result == expected
        assert is_literal_type(result.type_)
        assert get_args(result.type_) == ("true", "false")

    def test_class_future_type_literal_nullable(self) -> None:
        result = one(
            yield_fields(DataClassFutureTypeLiteralNullable, globalns=globals())
        )
        expected = _YieldFieldsClass(
            name="truth",
            type_=TrueOrFalseFutureTypeLit | None,
            default=None,
            kw_only=True,
        )
        assert result == expected
        assert is_optional_type(result.type_)
        args = get_args(result.type_)
        assert args == (TrueOrFalseFutureTypeLit,)
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
