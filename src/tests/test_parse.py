from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from pathlib import Path
from types import NoneType
from typing import Final, Literal

from hypothesis import given
from hypothesis.strategies import (
    booleans,
    dates,
    dictionaries,
    floats,
    frozensets,
    integers,
    lists,
    none,
    sampled_from,
    sets,
    times,
)
from pytest import raises

from tests.test_operator import TruthEnum
from tests.test_typing_funcs.with_future import (
    DataClassFutureInt,
    DataClassFutureIntEven,
    DataClassFutureIntEvenOrOddTypeUnion,
    DataClassFutureIntEvenOrOddUnion,
    DataClassFutureIntOdd,
    TrueOrFalseFutureLit,
    TrueOrFalseFutureTypeLit,
)
from utilities.errors import ImpossibleCaseError
from utilities.functions import ensure_path
from utilities.hypothesis import (
    datetime_durations,
    numbers,
    paths,
    plain_datetimes,
    text_ascii,
    timedeltas_2w,
    versions,
    zoned_datetimes,
)
from utilities.math import is_equal
from utilities.parse import (
    _ParseObjectExtraNonUniqueError,
    _ParseObjectParseError,
    _SerializeObjectExtraNonUniqueError,
    _SerializeObjectSerializeError,
    parse_object,
    serialize_object,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.text import parse_bool
from utilities.types import Duration, Number
from utilities.version import Version


class TestSerializeAndParseObject:
    @given(bool_=booleans())
    def test_bool(self, *, bool_: bool) -> None:
        serialized = serialize_object(bool_)
        result = parse_object(bool, serialized)
        assert result is bool_

    @given(date=dates())
    def test_date(self, *, date: dt.date) -> None:
        serialized = serialize_object(date)
        result = parse_object(dt.date, serialized)
        assert result == date

    @given(datetime=plain_datetimes() | zoned_datetimes())
    def test_datetime(self, *, datetime: dt.datetime) -> None:
        serialized = serialize_object(datetime)
        result = parse_object(dt.datetime, serialized)
        assert result == datetime

    @given(mapping=dictionaries(dates(), zoned_datetimes()))
    def test_dict(self, *, mapping: dict[dt.date, dt.datetime]) -> None:
        serialized = serialize_object(mapping)
        result = parse_object(dict[dt.date, dt.datetime], serialized)
        assert result == mapping

    @given(duration=datetime_durations(two_way=True))
    def test_duration(self, *, duration: Duration) -> None:
        serialized = serialize_object(duration)
        result = parse_object(Duration, serialized)
        assert result == duration

    @given(truth=sampled_from(TruthEnum))
    def test_enum(self, *, truth: TruthEnum) -> None:
        serialized = serialize_object(truth)
        result = parse_object(TruthEnum, serialized)
        assert result is truth

    @given(float_=floats())
    def test_float(self, *, float_: float) -> None:
        serialized = serialize_object(float_)
        result = parse_object(float, serialized)
        assert is_equal(result, float_)

    @given(dates=frozensets(dates()))
    def test_frozenset(self, *, dates: frozenset[dt.date]) -> None:
        serialized = serialize_object(dates)
        result = parse_object(frozenset[dt.date], serialized)
        assert result == dates

    @given(int_=integers())
    def test_int(self, *, int_: int) -> None:
        serialized = serialize_object(int_)
        result = parse_object(int, serialized)
        assert result == int_

    @given(dates=lists(dates()))
    def test_list(self, *, dates: list[dt.date]) -> None:
        serialized = serialize_object(dates)
        result = parse_object(list[dt.date], serialized)
        assert result == dates

    @given(bool_=booleans())
    def test_literal_extra(self, *, bool_: bool) -> None:
        text = serialize_object(bool_)
        result = parse_object(bool, text, extra={Literal["lit"]: parse_bool})
        assert result is bool_

    @given(truth=sampled_from(["true", "false"]))
    def test_literal(self, *, truth: Literal["true", "false"]) -> None:
        result = parse_object(TrueOrFalseFutureLit, truth)
        assert result == truth

    def test_none(self) -> None:
        serialized = serialize_object(None)
        result = parse_object(None, serialized)
        assert result is None

    def test_none_type(self) -> None:
        serialized = serialize_object(None)
        result = parse_object(NoneType, serialized)
        assert result is None

    @given(number=numbers())
    def test_number(self, *, number: Number) -> None:
        serialized = serialize_object(number)
        result = parse_object(Number, serialized)
        assert result == number

    @given(path=paths())
    def test_path(self, *, path: Path) -> None:
        serialized = serialize_object(path)
        result = parse_object(Path, serialized)
        assert result == path

    @given(path=paths())
    def test_path_expanded(self, *, path: Path) -> None:
        path_use = Path("~", path)
        serialized = serialize_object(path_use)
        result = ensure_path(parse_object(Path, serialized))
        assert result == result.expanduser()

    def test_nullable_number_none(self) -> None:
        serialized = serialize_object(None)
        result = parse_object(Number | None, serialized)
        assert result is None

    @given(number=numbers())
    def test_nullable_number_number(self, *, number: Number) -> None:
        serialized = serialize_object(number)
        result = parse_object(Number | None, serialized)
        assert result == number

    def test_nullable_duration_none(self) -> None:
        serialized = serialize_object(None)
        result = parse_object(Duration | None, serialized)
        assert result is None

    @given(duration=datetime_durations(two_way=True))
    def test_nullable_duration_duration(self, *, duration: Duration) -> None:
        serialized = serialize_object(duration)
        result = parse_object(Duration | None, serialized)
        assert result == duration

    def test_nullable_int_none(self) -> None:
        serialized = serialize_object(None)
        result = parse_object(int | None, serialized)
        assert result is None

    @given(int_=integers())
    def test_nullable_int_int(self, *, int_: int) -> None:
        serialized = serialize_object(int_)
        result = parse_object(int | None, serialized)
        assert result == int_

    def test_sentinel(self) -> None:
        serialized = serialize_object(sentinel)
        result = parse_object(Sentinel, serialized)
        assert result is sentinel

    @given(dates=sets(dates()))
    def test_set(self, *, dates: set[dt.date]) -> None:
        serialized = serialize_object(dates)
        result = parse_object(set[dt.date], serialized)
        assert result == dates

    @given(serialized=text_ascii())
    def test_to_serialized(self, *, serialized: str) -> None:
        result = parse_object(str, serialized)
        assert result == serialized

    @given(time=times())
    def test_time(self, *, time: dt.time) -> None:
        serialized = serialize_object(time)
        result = parse_object(dt.time, serialized)
        assert result == time

    @given(timedelta=timedeltas_2w())
    def test_timedelta(self, *, timedelta: dt.timedelta) -> None:
        serialized = serialize_object(timedelta)
        result = parse_object(dt.timedelta, serialized)
        assert result == timedelta

    @given(x=integers(), y=integers())
    def test_tuple(self, *, x: int, y: int) -> None:
        serialized = serialize_object((x, y))
        result = parse_object(tuple[int, int], serialized)
        assert result == (x, y)

    @given(int_=integers())
    def test_type_extra(self, *, int_: int) -> None:
        serialized = serialize_object(int_)
        result = parse_object(
            DataClassFutureInt,
            serialized,
            extra={DataClassFutureInt: lambda text: DataClassFutureInt(int_=int(text))},
        )
        expected = DataClassFutureInt(int_=int_)
        assert result == expected

    @given(truth=sampled_from(["true", "false"]))
    def test_type_literal(self, *, truth: Literal["true", "false"]) -> None:
        result = parse_object(TrueOrFalseFutureTypeLit, truth)
        assert result == truth

    @given(int_=integers())
    def test_type_union_with_extra(self, *, int_: int) -> None:
        def parser(text: str, /) -> DataClassFutureIntEvenOrOddTypeUnion:
            int_ = int(text)
            match int_ % 2:
                case 0:
                    return DataClassFutureIntEven(even_int=int_)
                case 1:
                    return DataClassFutureIntOdd(odd_int=int_)
                case _:
                    raise ImpossibleCaseError(case=[f"{int_=}"])

        serialized = serialize_object(int_)
        result = parse_object(
            DataClassFutureIntEvenOrOddTypeUnion,
            serialized,
            extra={DataClassFutureIntEvenOrOddTypeUnion: parser},
        )
        match int_ % 2:
            case 0:
                expected = DataClassFutureIntEven(even_int=int_)
            case 1:
                expected = DataClassFutureIntOdd(odd_int=int_)
            case _:
                raise ImpossibleCaseError(case=[f"{int_=}"])
        assert result == expected

    @given(int_=integers())
    def test_union_with_extra(self, *, int_: int) -> None:
        def parser(text: str, /) -> DataClassFutureIntEvenOrOddUnion:
            int_ = int(text)
            match int_ % 2:
                case 0:
                    return DataClassFutureIntEven(even_int=int_)
                case 1:
                    return DataClassFutureIntOdd(odd_int=int_)
                case _:
                    raise ImpossibleCaseError(case=[f"{int_=}"])

        serialized = serialize_object(int_)
        result = parse_object(
            DataClassFutureIntEvenOrOddUnion,
            serialized,
            extra={DataClassFutureIntEvenOrOddUnion: parser},
        )
        match int_ % 2:
            case 0:
                expected = DataClassFutureIntEven(even_int=int_)
            case 1:
                expected = DataClassFutureIntOdd(odd_int=int_)
            case _:
                raise ImpossibleCaseError(case=[f"{int_=}"])
        assert result == expected

    @given(version=versions())
    def test_version(self, *, version: Version) -> None:
        serialized = serialize_object(version)
        result = parse_object(Version, serialized)
        assert result == version


class TestParseObject:
    @given(text=sampled_from(["F_a_l_s_e", "T_r_u_e"]))
    def test_extra(self, *, text: str) -> None:
        def parser(text: str, /) -> bool:
            match text:
                case "F_a_l_s_e":
                    return False
                case "T_r_u_e":
                    return True
                case _:
                    raise ImpossibleCaseError(case=[f"{text=}"])

        bool_ = parse_object(bool, text, extra={bool: parser})
        match text:
            case "F_a_l_s_e":
                expected = False
            case "T_r_u_e":
                expected = True
            case _:
                raise ImpossibleCaseError(case=[f"{text=}"])
        assert bool_ is expected

    @given(text=sampled_from(["F_a_l_s_e", "T_r_u_e"]))
    def test_extra_both_exact_match_and_non_unique_parents(self, *, text: str) -> None:
        def parser(text: str, /) -> bool:
            match text:
                case "F_a_l_s_e":
                    return False
                case "T_r_u_e":
                    return True
                case _:
                    raise ImpossibleCaseError(case=[f"{text=}"])

        result = parse_object(
            bool, text, extra={bool: parser, bool | int: bool, bool | float: bool}
        )
        match text:
            case "F_a_l_s_e":
                expected = False
            case "T_r_u_e":
                expected = True
            case _:
                raise ImpossibleCaseError(case=[f"{text=}"])
        assert result is expected

    @given(value=text_ascii(min_size=10) | none())
    def test_optional_type_with_union_extra_not_used(
        self, *, value: str | None
    ) -> None:
        text = serialize_object(value)

        def parser(text: str, /) -> Number:
            return int(text)

        result = parse_object(str | None, text, extra={Number: parser})
        assert result == value

    def test_error_bool(self) -> None:
        with raises(
            _ParseObjectParseError,
            match="Unable to parse <class 'bool'>; got 'invalid'",
        ):
            _ = parse_object(bool, "invalid")

    def test_error_date(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'datetime\.date'>; got 'invalid'",
        ):
            _ = parse_object(dt.date, "invalid")

    def test_error_datetime(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'datetime\.datetime'>; got 'invalid'",
        ):
            _ = parse_object(dt.datetime, "invalid")

    def test_error_dict_extract_group(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse dict\[int, int\]; got 'invalid'",
        ):
            _ = parse_object(dict[int, int], "invalid")

    def test_error_dict_internal(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse dict\[int, int\]; got '\{invalid=invalid\}'",
        ):
            _ = parse_object(dict[int, int], "{invalid=invalid}")

    def test_error_duration(self) -> None:
        with raises(
            _ParseObjectParseError, match=r"Unable to parse Duration; got 'invalid'"
        ):
            _ = parse_object(Duration, "invalid")

    def test_error_enum(self) -> None:
        with raises(
            _ParseObjectParseError,
            match="Unable to parse <enum 'TruthEnum'>; got 'invalid'",
        ):
            _ = parse_object(TruthEnum, "invalid")

    def test_error_extra_empty(self) -> None:
        with raises(
            _ParseObjectParseError,
            match="Unable to parse <class 'tests.test_typing_funcs.with_future.DataClassFutureInt'>; got 'invalid'",
        ):
            _ = parse_object(DataClassFutureInt, "invalid", extra={})

    @given(text=sampled_from(["F_a_l_s_e", "T_r_u_e"]))
    def test_error_extra_empty_bool_does_not_use_int(self, *, text: str) -> None:
        def parser(text: str, /) -> bool:
            match text:
                case "F_a_l_s_e":
                    return False
                case "T_r_u_e":
                    return True
                case _:
                    raise ImpossibleCaseError(case=[f"{text=}"])

        with raises(
            _ParseObjectParseError, match="Unable to parse <class 'bool'>; got '.*'"
        ):
            _ = parse_object(bool, text, extra={int: parser})

    @given(int_=integers())
    def test_error_extra_non_unique(self, *, int_: int) -> None:
        with raises(
            _ParseObjectExtraNonUniqueError,
            match="Unable to parse <class 'int'> since `extra` must contain exactly one parent class; got <class 'int'>, <class 'int'> and perhaps more",
        ):
            _ = parse_object(int, str(int_), extra={int | bool: int, int | float: int})

    def test_error_union_type_extra(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse tests\.test_typing_funcs\.with_future\.DataClassFutureIntEven \| tests\.test_typing_funcs\.with_future\.DataClassFutureIntOdd; got 'invalid'",
        ):
            _ = parse_object(DataClassFutureIntEvenOrOddUnion, "invalid", extra={})

    def test_error_float(self) -> None:
        with raises(
            _ParseObjectParseError,
            match="Unable to parse <class 'float'>; got 'invalid'",
        ):
            _ = parse_object(float, "invalid")

    def test_error_frozenset_extract_group(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse frozenset\[int\]; got 'invalid'",
        ):
            _ = parse_object(frozenset[int], "invalid")

    def test_error_frozenset_internal(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse frozenset\[int\]; got '\{invalid\}'",
        ):
            _ = parse_object(frozenset[int], "{invalid}")

    def test_error_int(self) -> None:
        with raises(
            _ParseObjectParseError, match="Unable to parse <class 'int'>; got 'invalid'"
        ):
            _ = parse_object(int, "invalid")

    def test_error_list_extract_group(self) -> None:
        with raises(
            _ParseObjectParseError, match=r"Unable to parse list\[int\]; got 'invalid'"
        ):
            _ = parse_object(list[int], "invalid")

    def test_error_list_internal(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse list\[int\]; got '\[invalid\]'",
        ):
            _ = parse_object(list[int], "[invalid]")

    def test_error_none(self) -> None:
        with raises(
            _ParseObjectParseError, match="Unable to parse None; got 'invalid'"
        ):
            _ = parse_object(None, "invalid")

    def test_error_none_type(self) -> None:
        with raises(
            _ParseObjectParseError,
            match="Unable to parse <class 'NoneType'>; got 'invalid'",
        ):
            _ = parse_object(NoneType, "invalid")

    def test_error_nullable_int(self) -> None:
        with raises(
            _ParseObjectParseError, match=r"Unable to parse int \| None; got 'invalid'"
        ):
            _ = parse_object(int | None, "invalid")

    def test_error_nullable_not_type(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse collections\.abc\.Iterable\[None\] \| None; got 'invalid'",
        ):
            _ = parse_object(Iterable[None] | None, "invalid")

    def test_error_number(self) -> None:
        with raises(
            _ParseObjectParseError, match=r"Unable to parse Number; got 'invalid'"
        ):
            _ = parse_object(Number, "invalid")

    def test_error_sentinel(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'utilities\.sentinel\.Sentinel'>; got 'invalid'",
        ):
            _ = parse_object(Sentinel, "invalid")

    def test_error_set_extract_group(self) -> None:
        with raises(
            _ParseObjectParseError, match=r"Unable to parse set\[int\]; got 'invalid'"
        ):
            _ = parse_object(set[int], "invalid")

    def test_error_set_internal(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse set\[int\]; got '\{invalid\}'",
        ):
            _ = parse_object(set[int], "{invalid}")

    def test_error_time(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'datetime\.time'>; got 'invalid'",
        ):
            _ = parse_object(dt.time, "invalid")

    def test_error_timedelta(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'datetime\.timedelta'>; got 'invalid'",
        ):
            _ = parse_object(dt.timedelta, "invalid")

    def test_error_tuple_extract_group(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse tuple\[int, int\]; got 'invalid'",
        ):
            _ = parse_object(tuple[int, int], "invalid")

    def test_error_tuple_internal(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse tuple\[int, int\]; got '\(invalid,invalid\)'",
        ):
            _ = parse_object(tuple[int, int], "(invalid,invalid)")

    def test_error_tuple_inconsistent_args_and_serializeds(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse tuple\[int, int\]; got '\(serialized1, serialized2, serialized3\)'",
        ):
            _ = parse_object(tuple[int, int], "(serialized1, serialized2, serialized3)")

    def test_error_type_not_implemented(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'tests\.test_typing_funcs\.with_future\.DataClassFutureInt'>; got 'invalid'",
        ):
            _ = parse_object(DataClassFutureInt, "invalid")

    def test_error_union_not_implemented(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse tests\.test_typing_funcs\.with_future\.DataClassFutureIntEven \| tests\.test_typing_funcs\.with_future\.DataClassFutureIntOdd; got 'invalid'",
        ):
            _ = parse_object(DataClassFutureIntEvenOrOddUnion, "invalid")

    def test_error_version(self) -> None:
        with raises(
            _ParseObjectParseError,
            match=r"Unable to parse <class 'utilities\.version\.Version'>; got 'invalid'",
        ):
            _ = parse_object(Version, "invalid")


class TestSerializeObject:
    @given(bool_=booleans())
    def test_bool_custom(self, *, bool_: bool) -> None:
        def serializer(bool_: bool, /) -> str:  # noqa: FBT001
            match bool_:
                case True:
                    return "1"
                case False:
                    return "0"

        serialized = serialize_object(bool_, extra={bool: serializer})
        match bool_:
            case True:
                expected = "1"
            case False:
                expected = "0"
        assert serialized == expected

    @given(bool_=booleans())
    def test_bool_extra_not_used(self, *, bool_: bool) -> None:
        def serializer(int_: int, /) -> str:
            return f"({int_})"

        serialized = serialize_object(bool_, extra={int: serializer})
        expected = str(bool_)
        assert serialized == expected

    @given(int_=integers())
    def test_type_with_extra(self, *, int_: int) -> None:
        obj = DataClassFutureInt(int_=int_)

        def serializer(obj: DataClassFutureInt, /) -> str:
            return str(obj.int_)

        serialized = serialize_object(obj, extra={DataClassFutureInt: serializer})
        expected = str(int_)
        assert serialized == expected

    @given(int_=integers())
    def test_type_union_with_extra(self, *, int_: int) -> None:
        match int_ % 2:
            case 0:
                obj = DataClassFutureIntEven(even_int=int_)
            case 1:
                obj = DataClassFutureIntOdd(odd_int=int_)
            case _:
                raise ImpossibleCaseError(case=[f"{int_=}"])

        def serializer(obj: DataClassFutureIntEvenOrOddTypeUnion, /) -> str:
            match obj:
                case DataClassFutureIntEven():
                    return str(obj.even_int)
                case DataClassFutureIntOdd():
                    return str(obj.odd_int)

        serialized = serialize_object(
            obj, extra={DataClassFutureIntEvenOrOddTypeUnion: serializer}
        )
        expected = str(int_)
        assert serialized == expected

    @given(int_=integers())
    def test_union_with_extra(self, *, int_: int) -> None:
        match int_ % 2:
            case 0:
                obj = DataClassFutureIntEven(even_int=int_)
            case 1:
                obj = DataClassFutureIntOdd(odd_int=int_)
            case _:
                raise ImpossibleCaseError(case=[f"{int_=}"])

        def serializer(obj: DataClassFutureIntEvenOrOddUnion, /) -> str:
            match obj:
                case DataClassFutureIntEven():
                    return str(obj.even_int)
                case DataClassFutureIntOdd():
                    return str(obj.odd_int)

        serialized = serialize_object(
            obj, extra={DataClassFutureIntEvenOrOddUnion: serializer}
        )
        expected = str(int_)
        assert serialized == expected

    def test_error_extra_empty(self) -> None:
        with raises(
            _SerializeObjectSerializeError,
            match=r"Unable to serialize object typing\.Final of type <class 'typing\._SpecialForm'>",
        ):
            _ = serialize_object(Final, extra={})

    @given(bool_=booleans())
    def test_error_extra_non_unique(self, *, bool_: bool) -> None:
        with raises(
            _SerializeObjectExtraNonUniqueError,
            match=r"Unable to serialize object (True|False) of type <class 'bool'> since `extra` must contain exactly one parent class; got <class 'str'>, <class 'str'> and perhaps more",
        ):
            _ = serialize_object(bool_, extra={bool | int: str, bool | float: str})

    def test_error_not_implemented(self) -> None:
        with raises(_SerializeObjectSerializeError):
            _ = serialize_object(Final)
