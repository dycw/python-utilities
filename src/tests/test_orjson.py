from __future__ import annotations

import datetime as dt
from io import StringIO
from logging import DEBUG, FileHandler, StreamHandler, getLogger
from pathlib import Path
from re import search
from typing import TYPE_CHECKING, Any

from hypothesis import given
from hypothesis.strategies import (
    builds,
    dates,
    dictionaries,
    integers,
    lists,
    none,
    sampled_from,
    tuples,
)
from orjson import JSONDecodeError
from pytest import approx, mark, param, raises

from tests.conftest import SKIPIF_CI_AND_WINDOWS
from tests.test_operator import (
    SubFrozenSet,
    SubList,
    SubSet,
    SubTuple,
    TruthEnum,
    make_objects,
)
from tests.test_typing_funcs.with_future import (
    DataClassFutureCustomEquality,
    DataClassFutureDefaultInInitChild,
    DataClassFutureDefaultInInitParent,
    DataClassFutureInt,
    DataClassFutureIntDefault,
    DataClassFutureLiteral,
    DataClassFutureLiteralNullable,
    DataClassFutureNestedInnerFirstInner,
    DataClassFutureNestedInnerFirstOuter,
    DataClassFutureNestedOuterFirstInner,
    DataClassFutureNestedOuterFirstOuter,
    DataClassFutureNone,
)
from utilities.datetime import MINUTE, SECOND, get_now
from utilities.functions import is_sequence_of
from utilities.hypothesis import (
    assume_does_not_raise,
    temp_paths,
    text_ascii,
    text_printable,
    zoned_datetimes,
)
from utilities.iterables import one
from utilities.logging import get_logging_level_number
from utilities.math import MAX_INT64, MIN_INT64
from utilities.operator import IsEqualError, is_equal
from utilities.orjson import (
    OrjsonFormatter,
    OrjsonLogRecord,
    Unserializable,
    _DeserializeNoObjectsError,
    _DeserializeObjectNotFoundError,
    _object_hook_get_object,
    _SerializeIntegerError,
    deserialize,
    get_log_records,
    serialize,
)
from utilities.sentinel import Sentinel, sentinel
from utilities.types import DateOrDateTime, LogLevel
from utilities.typing import get_args
from utilities.zoneinfo import UTC

if TYPE_CHECKING:
    from collections.abc import Sequence

    from utilities.types import Dataclass, StrMapping


# formatter


class TestGetLogRecords:
    def test_main(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        handler = FileHandler(file := tmp_path.joinpath("log"))
        handler.setFormatter(OrjsonFormatter())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("", extra={"a": 1, "b": 2, "_ignored": 3})
        output = get_log_records(tmp_path, parallelism="threads")
        assert output.path == tmp_path
        assert output.files == [file]
        assert output.num_files == 1
        assert output.num_files_ok == 1
        assert output.num_files_error == 0
        assert output.num_lines == 1
        assert output.num_lines_ok == 1
        assert output.num_lines_blank == 0
        assert output.num_lines_error == 0
        assert len(output.records) == 1
        assert output.missing == set()
        assert output.other_errors == []
        # properties
        assert output.frac_files_ok == 1.0
        assert output.frac_files_error == 0.0
        assert output.frac_lines_ok == 1.0
        assert output.frac_lines_blank == 0.0
        assert output.frac_lines_error == 0.0

        # record
        record = one(output.records)
        assert record.name == str(tmp_path)
        assert record.message == ""
        assert record.level == "DEBUG"
        assert record.line_num == approx(92, rel=0.1)
        assert abs(record.datetime - get_now()) <= MINUTE
        assert record.func_name == "test_main"
        assert record.stack_info is None
        assert record.extra == {"a": 1, "b": 2}
        assert record.log_file == file
        assert record.log_file_line_num == 1

        # slicing
        assert is_sequence_of(output[:], OrjsonLogRecord)

    @given(
        items=lists(
            tuples(
                sampled_from(get_args(LogLevel)),
                text_ascii(),
                dictionaries(text_ascii(), integers()),
            )
        ),
        root=temp_paths(),
        name=text_ascii() | none(),
        message=text_ascii() | none(),
        level=sampled_from(get_args(LogLevel)) | none(),
        min_level=sampled_from(get_args(LogLevel)) | none(),
        max_level=sampled_from(get_args(LogLevel)) | none(),
        date_or_datetime=dates() | zoned_datetimes() | none(),
        min_date_or_datetime=dates() | zoned_datetimes() | none(),
        max_date_or_datetime=dates() | zoned_datetimes() | none(),
    )
    def test_filter(
        self,
        *,
        root: Path,
        items: Sequence[tuple[LogLevel, str, StrMapping]],
        name: str | None,
        message: str | None,
        level: LogLevel | None,
        min_level: LogLevel | None,
        max_level: LogLevel | None,
        date_or_datetime: DateOrDateTime | None,
        min_date_or_datetime: DateOrDateTime | None,
        max_date_or_datetime: DateOrDateTime | None,
    ) -> None:
        logger = getLogger(str(root))
        logger.setLevel(DEBUG)
        handler = FileHandler(root.joinpath("log"))
        handler.setFormatter(OrjsonFormatter())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        for level_, message_, extra in items:
            logger.log(get_logging_level_number(level_), message_, extra=extra)
        output = get_log_records(root, parallelism="threads").filter(
            name=name,
            message=message,
            level=level,
            min_level=min_level,
            max_level=max_level,
            date_or_datetime=date_or_datetime,
            min_date_or_datetime=min_date_or_datetime,
            max_date_or_datetime=max_date_or_datetime,
        )
        records = output.records
        if name is not None:
            assert all(search(name, r.name) for r in records)
        if message is not None:
            assert all(search(message, r.message) for r in records)
        if level is not None:
            assert all(r.level_num == get_logging_level_number(level) for r in records)
        if min_level is not None:
            assert all(
                r.level_num >= get_logging_level_number(min_level) for r in records
            )
        if max_level is not None:
            assert all(
                r.level_num <= get_logging_level_number(max_level) for r in records
            )
        if date_or_datetime is not None:
            match date_or_datetime:
                case dt.datetime() as datetime:
                    assert all(r.datetime == datetime for r in records)
                case dt.date() as date:
                    assert all(r.date == date for r in records)
        if min_date_or_datetime is not None:
            match min_date_or_datetime:
                case dt.datetime() as min_datetime:
                    assert all(r.datetime >= min_datetime for r in records)
                case dt.date() as min_date:
                    assert all(r.date >= min_date for r in records)
        if max_date_or_datetime is not None:
            match max_date_or_datetime:
                case dt.datetime() as max_datetime:
                    assert all(r.datetime <= max_datetime for r in records)
                case dt.date() as max_date:
                    assert all(r.date <= max_date for r in records)

    def test_skip_blank_lines(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        handler = FileHandler(file := tmp_path.joinpath("log"))
        with file.open(mode="w") as fh:
            _ = fh.write("\n")
        handler.setFormatter(OrjsonFormatter())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("", extra={"a": 1, "b": 2, "_ignored": 3})
        result = get_log_records(tmp_path, parallelism="threads")
        assert result.path == tmp_path
        assert result.num_lines == 2
        assert result.num_lines_ok == 1
        assert result.num_lines_blank == 1
        assert result.num_lines_error == 0

    def test_skip_dir(self, *, tmp_path: Path) -> None:
        tmp_path.joinpath("dir").mkdir()
        result = get_log_records(tmp_path, parallelism="threads")
        assert result.path == tmp_path
        assert result.num_files == 0
        assert result.num_files_ok == 0
        assert result.num_files_error == 0
        assert len(result.other_errors) == 0

    @SKIPIF_CI_AND_WINDOWS
    def test_error_file(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("log")
        with file.open(mode="wb") as fh:
            _ = fh.write(b"\x80")
        result = get_log_records(tmp_path, parallelism="threads")
        assert result.path == tmp_path
        assert result.files == [file]
        assert result.num_files == 1
        assert result.num_files_ok == 0
        assert result.num_files_error == 1
        assert len(result.other_errors) == 1
        assert isinstance(one(result.other_errors), UnicodeDecodeError)

    def test_error_deserialize_due_to_missing(self, *, tmp_path: Path) -> None:
        logger = getLogger(str(tmp_path))
        logger.setLevel(DEBUG)
        handler = FileHandler(file := tmp_path.joinpath("log"))
        handler.setFormatter(OrjsonFormatter())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("", extra={"obj": DataClassFutureIntDefault()})
        result = get_log_records(tmp_path, parallelism="threads")
        assert result.path == tmp_path
        assert result.files == [file]
        assert result.num_lines == 1
        assert result.num_lines_ok == 0
        assert result.num_lines_error == 1
        assert result.missing == {DataClassFutureIntDefault.__qualname__}
        assert result.other_errors == []

    def test_error_deserialize_due_to_decode(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("log")
        with file.open(mode="w") as fh:
            _ = fh.write("message")
        result = get_log_records(tmp_path, parallelism="threads")
        assert result.path == tmp_path
        assert result.files == [file]
        assert result.num_lines == 1
        assert result.num_lines_ok == 0
        assert result.num_lines_error == 1
        assert result.missing == set()
        assert len(result.other_errors) == 1
        assert isinstance(one(result.other_errors), JSONDecodeError)


class TestOrjsonFormatter:
    def test_main(self, *, tmp_path: Path) -> None:
        name = str(tmp_path)
        logger = getLogger(name)
        logger.setLevel(DEBUG)
        handler = StreamHandler(buffer := StringIO())
        handler.setFormatter(OrjsonFormatter())
        handler.setLevel(DEBUG)
        logger.addHandler(handler)
        logger.debug("message", extra={"a": 1, "b": 2, "_ignored": 3})
        record = deserialize(buffer.getvalue().encode(), objects={OrjsonLogRecord})
        assert isinstance(record, OrjsonLogRecord)
        assert record.name == name
        assert record.message == "message"
        assert record.level == DEBUG
        assert record.path_name == Path(__file__)
        assert abs(record.datetime - get_now(time_zone="local")) <= SECOND
        assert record.func_name == TestOrjsonFormatter.test_main.__name__
        assert record.stack_info is None
        assert record.extra == {"a": 1, "b": 2}


# serialize/deserialize


class TestSerializeAndDeserialize:
    @given(
        obj=make_objects(
            dataclass_custom_equality=True,
            dataclass_int=True,
            dataclass_int_default=True,
            dataclass_literal=True,
            dataclass_literal_nullable=True,
            dataclass_nested=True,
            dataclass_none=True,
            enum=True,
            sub_frozenset=True,
            sub_list=True,
            sub_set=True,
            sub_tuple=True,
        )
    )
    def test_all(self, *, obj: Any) -> None:
        with assume_does_not_raise(_SerializeIntegerError):
            ser = serialize(obj, globalns=globals())
        result = deserialize(
            ser,
            objects={
                DataClassFutureCustomEquality,
                DataClassFutureInt,
                DataClassFutureIntDefault,
                DataClassFutureLiteral,
                DataClassFutureLiteralNullable,
                DataClassFutureNestedInnerFirstInner,
                DataClassFutureNestedInnerFirstOuter,
                DataClassFutureNestedOuterFirstInner,
                DataClassFutureNestedOuterFirstOuter,
                DataClassFutureNone,
                SubFrozenSet,
                SubList,
                SubSet,
                SubTuple,
                TruthEnum,
            },
        )
        with assume_does_not_raise(IsEqualError):
            assert is_equal(result, obj)

    @given(obj=make_objects())
    def test_base(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj))
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_custom_equality=True))
    def test_dataclass_custom_equality(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClassFutureCustomEquality})
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_default_in_init_child=True))
    def test_dataclass_default_in_init_child_hook_in_serialize(
        self, *, obj: Any
    ) -> None:
        def hook(cls: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            if issubclass(cls, DataClassFutureDefaultInInitParent):
                mapping = {k: v for k, v in mapping.items() if k != "int_"}
            return mapping

        result = deserialize(
            serialize(obj, dataclass_hook=hook),
            objects={DataClassFutureDefaultInInitChild},
        )
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_default_in_init_child=True))
    def test_dataclass_default_in_init_child_hook_in_deserialize(
        self, *, obj: Any
    ) -> None:
        def hook(cls: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            if issubclass(cls, DataClassFutureDefaultInInitParent):
                mapping = {k: v for k, v in mapping.items() if k != "int_"}
            return mapping

        result = deserialize(
            serialize(obj),
            dataclass_hook=hook,
            objects={DataClassFutureDefaultInInitChild},
        )
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_int=True))
    def test_dataclass_int(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClassFutureInt})
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_int_default=True))
    def test_dataclass_int_default(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClassFutureIntDefault})
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_literal=True))
    def test_dataclass_literal(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClassFutureLiteral})
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_literal_nullable=True))
    def test_dataclass_literal_nullable(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={DataClassFutureLiteralNullable})
        with assume_does_not_raise(IsEqualError):
            assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_nested=True))
    def test_dataclass_nested(self, *, obj: Any) -> None:
        ser = serialize(obj, globalns=globals())
        result = deserialize(
            ser,
            objects={
                DataClassFutureNestedInnerFirstInner,
                DataClassFutureNestedInnerFirstOuter,
                DataClassFutureNestedOuterFirstInner,
                DataClassFutureNestedOuterFirstOuter,
            },
        )
        assert is_equal(result, obj)

    @given(obj=make_objects(dataclass_none=True))
    def test_dataclass_none(self, *, obj: Any) -> None:
        ser = serialize(obj, globalns=globals())
        result = deserialize(ser, objects={DataClassFutureNone})
        assert is_equal(result, obj)

    @given(obj=builds(DataClassFutureNone))
    def test_dataclass_no_objects_error(self, *, obj: DataClassFutureNone) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeNoObjectsError,
            match="Objects required to deserialize '.*' from .*",
        ):
            _ = deserialize(ser)

    @given(obj=builds(DataClassFutureNone))
    def test_dataclass_empty_error(self, *, obj: DataClassFutureNone) -> None:
        ser = serialize(obj)
        with raises(
            _DeserializeObjectNotFoundError,
            match=r"Unable to find object to deserialize '.*' from .*",
        ):
            _ = deserialize(ser, objects=set())

    def test_deserialize_hook(self) -> None:
        obj = DataClassFutureDefaultInInitChild()
        ser = serialize(obj)
        with raises(TypeError, match="got an unexpected keyword argument 'int_'"):
            _ = deserialize(ser, objects={DataClassFutureDefaultInInitChild})

        def hook(cls: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            if issubclass(cls, DataClassFutureDefaultInInitParent):
                mapping = {k: v for k, v in mapping.items() if k != "int_"}
            return mapping

        result = deserialize(
            ser, dataclass_hook=hook, objects={DataClassFutureDefaultInInitChild}
        )
        assert result == obj

    @given(obj=make_objects(enum=True))
    def test_enum(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={TruthEnum})
        with assume_does_not_raise(IsEqualError):
            assert is_equal(result, obj)

    def test_none(self) -> None:
        result = deserialize(serialize(None))
        assert result is None

    @given(obj=make_objects(sub_frozenset=True))
    def test_sub_frozenset(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubFrozenSet})
        assert is_equal(result, obj)

    @given(obj=make_objects(sub_list=True))
    def test_sub_list(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubList})
        assert is_equal(result, obj)

    @given(obj=make_objects(sub_set=True))
    def test_sub_set(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubSet})
        assert is_equal(result, obj)

    @given(obj=make_objects(sub_tuple=True))
    def test_sub_tuple(self, *, obj: Any) -> None:
        result = deserialize(serialize(obj), objects={SubTuple})
        assert is_equal(result, obj)

    def test_unserializable(self) -> None:
        ser = serialize(sentinel)
        exp_ser = b'{"[dc|Unserializable]":{"qualname":"Sentinel","repr":"<sentinel>","str":"<sentinel>"}}'
        assert ser == exp_ser
        result = deserialize(ser)
        exp_res = Unserializable(
            qualname="Sentinel", repr="<sentinel>", str="<sentinel>"
        )
        assert result == exp_res

    @mark.parametrize(
        ("utc", "expected"),
        [
            param(UTC, b'"[dt]2000-01-01T00:00:00+00:00[UTC]"'),
            param(dt.UTC, b'"[dt]2000-01-01T00:00:00+00:00[dt.UTC]"'),
        ],
        ids=str,
    )
    def test_utc(self, *, utc: dt.tzinfo, expected: bytes) -> None:
        datetime = dt.datetime(2000, 1, 1, tzinfo=utc)
        ser = serialize(datetime)
        assert ser == expected
        result = deserialize(ser)
        assert result == datetime
        assert result.tzinfo is utc


class TestSerialize:
    @given(text=text_printable())
    def test_before(self, *, text: str) -> None:
        result = serialize(text, before=str.upper)
        expected = serialize(text.upper())
        assert result == expected

    def test_dataclass(self) -> None:
        obj = DataClassFutureNone(none=None)
        result = serialize(obj)
        expected = b'{"[dc|DataClassFutureNone]":{"none":"[none]"}}'
        assert result == expected

    def test_dataclass_nested(self) -> None:
        obj = DataClassFutureNestedOuterFirstOuter(
            inner=DataClassFutureNestedOuterFirstInner(int_=0)
        )
        result = serialize(obj, globalns=globals())
        expected = b'{"[dc|DataClassFutureNestedOuterFirstOuter]":{"inner":{"[dc|DataClassFutureNestedOuterFirstInner]":{"int_":0}}}}'
        assert result == expected

    def test_dataclass_hook_main(self) -> None:
        obj = DataClassFutureDefaultInInitChild()

        def hook(cls: type[Dataclass], mapping: StrMapping, /) -> StrMapping:
            if issubclass(cls, DataClassFutureDefaultInInitParent):
                mapping = {k: v for k, v in mapping.items() if k != "int_"}
            return mapping

        result = serialize(obj, dataclass_hook=hook)
        expected = b'{"[dc|DataClassFutureDefaultInInitChild]":{}}'
        assert result == expected

    @given(x=sampled_from([MIN_INT64 - 1, MAX_INT64 + 1]))
    def test_pre_process(self, *, x: int) -> None:
        with raises(_SerializeIntegerError, match="Integer .* is out of range"):
            _ = serialize(x)


class TestObjectHookGetObject:
    def test_main(self) -> None:
        result = _object_hook_get_object(Sentinel.__qualname__, objects={Sentinel})
        assert result is Sentinel

    def test_redirect(self) -> None:
        qualname = f"old_{Sentinel.__qualname__}"
        result = _object_hook_get_object(qualname, redirects={qualname: Sentinel})
        assert result is Sentinel

    def test_unserializable(self) -> None:
        result = _object_hook_get_object(Unserializable.__qualname__)
        assert result is Unserializable

    def test_error_no_objects(self) -> None:
        with raises(
            _DeserializeNoObjectsError,
            match="Objects required to deserialize 'qualname' from .*",
        ):
            _ = _object_hook_get_object("qualname")

    def test_error_object_not_found(self) -> None:
        with raises(
            _DeserializeObjectNotFoundError,
            match=r"Unable to find object to deserialize 'qualname' from .*",
        ):
            _ = _object_hook_get_object("qualname", objects=set())
