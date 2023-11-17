from __future__ import annotations

import enum
import typing
from enum import auto
from operator import eq
from pathlib import Path
from typing import Any, TypedDict, cast

import sqlalchemy
from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    integers,
    none,
    permutations,
    sampled_from,
    sets,
    tuples,
)
from pytest import mark, param, raises
from sqlalchemy import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    DOUBLE,
    DOUBLE_PRECISION,
    FLOAT,
    INT,
    INTEGER,
    NCHAR,
    NUMERIC,
    NVARCHAR,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    UUID,
    VARBINARY,
    VARCHAR,
    BigInteger,
    Boolean,
    Column,
    Connection,
    Date,
    DateTime,
    Double,
    Engine,
    Float,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
    select,
)
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.exc import DatabaseError, DuplicateColumnError, NoSuchTableError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import (
    lists_fixed_length,
    sqlite_engines,
    temp_paths,
    text_ascii,
)
from utilities.itertools import one
from utilities.pytest import skipif_not_linux
from utilities.sqlalchemy import (
    Dialect,
    EngineError,
    FirstArgumentInvalidError,
    IncorrectNumberOfTablesError,
    InvalidItemError,
    InvalidItemInIterableError,
    NotATableOrAMappedClassError,
    ParseEngineError,
    SecondArgumentNotATableOrMappedClassError,
    SeriesAndTableColumnIncompatibleError,
    SeriesMatchesAgainstMultipleColumnsError,
    SeriesMatchesAgainstNoColumnError,
    TableAlreadyExistsError,
    TablenameMixin,
    TupleNotAPairError,
    UnequalBooleanColumnCreateConstraintError,
    UnequalBooleanColumnNameError,
    UnequalColumnTypesError,
    UnequalDateTimeColumnTimezoneError,
    UnequalEnumColumnCreateConstraintError,
    UnequalEnumColumnInheritSchemaError,
    UnequalEnumColumnLengthError,
    UnequalEnumColumnNativeEnumError,
    UnequalEnumColumnTypesError,
    UnequalFloatColumnAsDecimalError,
    UnequalFloatColumnDecimalReturnScaleError,
    UnequalFloatColumnPrecisionsError,
    UnequalIntervalColumnDayPrecisionError,
    UnequalIntervalColumnNativeError,
    UnequalIntervalColumnSecondPrecisionError,
    UnequalLargeBinaryColumnLengthError,
    UnequalNullableStatusError,
    UnequalNumberOfColumnsError,
    UnequalNumericScaleError,
    UnequalPrimaryKeyStatusError,
    UnequalSetOfColumnsError,
    UnequalStringCollationError,
    UnequalStringLengthError,
    UnequalTableOrColumnNamesError,
    UnequalTableOrColumnSnakeCaseNamesError,
    UnequalUUIDAsUUIDError,
    UnequalUUIDNativeUUIDError,
    check_dataframe_schema_against_table,
    check_engine,
    check_selectable_for_duplicate_columns,
    check_table_against_reflection,
    check_tables_equal,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_engine,
    ensure_tables_created,
    ensure_tables_dropped,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    get_table_name,
    insert_items,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    parse_engine,
    redirect_to_no_such_table_error,
    redirect_to_table_already_exists_error,
    serialize_engine,
    yield_connection,
)
from utilities.sqlalchemy.sqlalchemy import (
    _check_column_collections_equal,
    _check_column_types_equal,
    _check_columns_equal,
    _check_series_against_against_table,
    _check_table_or_column_names_equal,
    _insert_items_collect,
    _insert_items_collect_iterable,
    _InsertionItem,
    _match_series_name_to_table_column,
    _reflect_table,
)


class TestCheckColumnsEqual:
    def test_equal(self) -> None:
        x = Column("id", Integer)
        _check_columns_equal(x, x)

    def test_names(self) -> None:
        x = Column("x", Integer)
        y = Column("y", Integer)
        with raises(UnequalTableOrColumnNamesError):
            _check_columns_equal(x, y)

    def test_column_types(self) -> None:
        x = Column("x", Integer)
        y = Column("x", String)
        with raises(UnequalColumnTypesError):
            _check_columns_equal(x, y)

    def test_primary_key_status(self) -> None:
        x = Column("id", Integer, primary_key=True)
        y = Column("id", Integer)
        with raises(UnequalPrimaryKeyStatusError):
            _check_columns_equal(x, y)

    def test_primary_key_status_skipped(self) -> None:
        x = Column("id", Integer, primary_key=True)
        y = Column("id", Integer, nullable=False)
        _check_columns_equal(x, y, primary_key=False)

    def test_nullable_status(self) -> None:
        x = Column("id", Integer)
        y = Column("id", Integer, nullable=False)
        with raises(UnequalNullableStatusError):
            _check_columns_equal(x, y)


class TestCheckColumnCollectionsEqual:
    def test_success(self) -> None:
        x = Table("x", MetaData(), Column("id", Integer, primary_key=True))
        _check_column_collections_equal(x.columns, x.columns)

    def test_snake(self) -> None:
        x = Table("x", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("y", MetaData(), Column("Id", Integer, primary_key=True))
        _check_column_collections_equal(x.columns, y.columns, snake=True)

    def test_allow_permutations(self) -> None:
        x = Table(
            "x",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        y = Table(
            "y",
            MetaData(),
            Column("id2", Integer, primary_key=True),
            Column("id1", Integer, primary_key=True),
        )
        _check_column_collections_equal(x.columns, y.columns, allow_permutations=True)

    def test_snake_and_allow_permutations(self) -> None:
        x = Table(
            "x",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        y = Table(
            "y",
            MetaData(),
            Column("Id2", Integer, primary_key=True),
            Column("Id1", Integer, primary_key=True),
        )
        _check_column_collections_equal(
            x.columns, y.columns, snake=True, allow_permutations=True
        )

    def test_unequal_number_of_columns(self) -> None:
        x = Table("x", MetaData(), Column("id", Integer, primary_key=True))
        y = Table(
            "y",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Integer),
        )
        with raises(UnequalNumberOfColumnsError):
            _check_column_collections_equal(x.columns, y.columns)

    def test_unequal_set_of_columns(self) -> None:
        x = Table("x", MetaData(), Column("id1", Integer, primary_key=True))
        y = Table("y", MetaData(), Column("id2", Integer, primary_key=True))
        with raises(UnequalSetOfColumnsError):
            _check_column_collections_equal(x.columns, y.columns)

    @mark.parametrize("allow_permutation", [param(True), param(False)])
    def test_unequal_column_types(self, *, allow_permutation: bool) -> None:
        x = Table("x", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("y", MetaData(), Column("id", String, primary_key=True))
        with raises(UnequalColumnTypesError):
            _check_column_collections_equal(
                x.columns, y.columns, allow_permutations=allow_permutation
            )


class TestCheckColumnTypesEqual:
    groups = (
        [BIGINT, INT, INTEGER, SMALLINT, BigInteger, Integer, SmallInteger],
        [BOOLEAN, Boolean],
        [DATE, Date],
        [DATETIME, TIMESTAMP, DateTime],
        [Interval],
        [BINARY, VARBINARY, LargeBinary],
        [
            DECIMAL,
            DOUBLE,
            DOUBLE_PRECISION,
            FLOAT,
            NUMERIC,
            REAL,
            Double,
            Float,
            Numeric,
        ],
        [
            CHAR,
            CLOB,
            NCHAR,
            NVARCHAR,
            TEXT,
            VARCHAR,
            String,
            Text,
            Unicode,
            UnicodeText,
            sqlalchemy.Enum,
        ],
        [TIME, Time],
        [UUID, Uuid],
    )

    @given(data=data())
    def test_equal(self, *, data: DataObject) -> None:
        group = data.draw(sampled_from(self.groups))
        cls = data.draw(sampled_from(group))
        elements = sampled_from([cls, cls()])
        x, y = data.draw(lists_fixed_length(elements, 2))
        _check_column_types_equal(x, y)

    @given(data=data())
    def test_unequal(self, *, data: DataObject) -> None:
        groups = self.groups
        i, j = data.draw(lists_fixed_length(integers(0, len(groups) - 1), 2))
        _ = assume(i != j)
        group_i, group_j = groups[i], groups[j]
        cls_x, cls_y = (data.draw(sampled_from(g)) for g in [group_i, group_j])
        x, y = (data.draw(sampled_from([c, c()])) for c in [cls_x, cls_y])
        with raises(UnequalColumnTypesError):
            _check_column_types_equal(x, y)

    @given(create_constraints=lists_fixed_length(booleans(), 2))
    def test_boolean_create_constraint(
        self, *, create_constraints: typing.Sequence[bool]
    ) -> None:
        create_constraint_x, create_constraint_y = create_constraints
        x, y = (Boolean(create_constraint=cs) for cs in create_constraints)
        if create_constraint_x is create_constraint_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalBooleanColumnCreateConstraintError):
                _check_column_types_equal(x, y)

    @given(names=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_boolean_name(self, *, names: typing.Sequence[str | None]) -> None:
        name_x, name_y = names
        x, y = (Boolean(name=n) for n in names)
        if name_x == name_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalBooleanColumnNameError):
                _check_column_types_equal(x, y)

    def test_camel_versus_upper(self) -> None:
        _check_column_types_equal(Boolean, BOOLEAN)

    @given(timezones=lists_fixed_length(booleans(), 2))
    def test_datetime_timezone(self, *, timezones: typing.Sequence[bool]) -> None:
        timezone_x, timezone_y = timezones
        x, y = (DateTime(timezone=tz) for tz in timezones)
        if timezone_x is timezone_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalDateTimeColumnTimezoneError):
                _check_column_types_equal(x, y)

    def test_enum_two_enum_classes(self) -> None:
        class EnumX(enum.Enum):
            member = auto()

        class EnumY(enum.Enum):
            member = auto()

        x, y = (sqlalchemy.Enum(e) for e in [EnumX, EnumY])
        with raises(UnequalEnumColumnTypesError):
            _check_column_types_equal(x, y)

    @given(data=data())
    def test_enum_one_enum_class(self, *, data: DataObject) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        x = sqlalchemy.Enum(MyEnum)
        y = data.draw(sampled_from([sqlalchemy.Enum, sqlalchemy.Enum()]))
        x, y = data.draw(permutations([x, y]))
        with raises(UnequalEnumColumnTypesError):
            _check_column_types_equal(x, y)

    @given(create_constraints=lists_fixed_length(booleans(), 2))
    def test_enum_create_constraint(
        self, *, create_constraints: typing.Sequence[bool]
    ) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        create_constraint_x, create_constraint_y = create_constraints
        x, y = (
            sqlalchemy.Enum(MyEnum, create_constraint=cs) for cs in create_constraints
        )
        if create_constraint_x is create_constraint_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalEnumColumnCreateConstraintError):
                _check_column_types_equal(x, y)

    @given(native_enums=lists_fixed_length(booleans(), 2))
    def test_enum_native_enum(self, *, native_enums: typing.Sequence[bool]) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        native_enum_x, native_enum_y = native_enums
        x, y = (sqlalchemy.Enum(MyEnum, native_enum=ne) for ne in native_enums)
        if native_enum_x is native_enum_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalEnumColumnNativeEnumError):
                _check_column_types_equal(x, y)

    @given(lengths=lists_fixed_length(integers(6, 10), 2))
    def test_enum_length(self, *, lengths: typing.Sequence[int]) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        length_x, length_y = lengths
        x, y = (sqlalchemy.Enum(MyEnum, length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalEnumColumnLengthError):
                _check_column_types_equal(x, y)

    @given(inherit_schemas=lists_fixed_length(booleans(), 2))
    def test_enum_inherit_schema(
        self, *, inherit_schemas: typing.Sequence[bool]
    ) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        inherit_schema_x, inherit_schema_y = inherit_schemas
        x, y = (sqlalchemy.Enum(MyEnum, inherit_schema=is_) for is_ in inherit_schemas)
        if inherit_schema_x is inherit_schema_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalEnumColumnInheritSchemaError):
                _check_column_types_equal(x, y)

    @given(
        cls=sampled_from([Float, Numeric]),
        precisions=lists_fixed_length(integers(0, 10) | none(), 2),
    )
    def test_float_precision(
        self,
        cls: type[Float[Any] | Numeric[Any]],
        precisions: typing.Sequence[int | None],
    ) -> None:
        precision_x, precision_y = precisions
        x, y = (cls(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalFloatColumnPrecisionsError):
                _check_column_types_equal(x, y)

    @given(
        cls=sampled_from([Float, Numeric]), asdecimals=lists_fixed_length(booleans(), 2)
    )
    def test_float_asdecimal(
        self, cls: type[Float[Any] | Numeric[Any]], asdecimals: typing.Sequence[bool]
    ) -> None:
        asdecimal_x, asdecimal_y = asdecimals
        x, y = (cls(asdecimal=cast(Any, a)) for a in asdecimals)
        if asdecimal_x is asdecimal_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalFloatColumnAsDecimalError):
                _check_column_types_equal(x, y)

    @given(
        cls=sampled_from([Float, Numeric]),
        dec_ret_scales=lists_fixed_length(integers(0, 10) | none(), 2),
    )
    def test_float_dec_ret_scale(
        self,
        cls: type[Float[Any] | Numeric[Any]],
        dec_ret_scales: typing.Sequence[int | None],
    ) -> None:
        dec_ret_scale_x, dec_ret_scale_y = dec_ret_scales
        x, y = (cls(decimal_return_scale=drs) for drs in dec_ret_scales)
        if dec_ret_scale_x == dec_ret_scale_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalFloatColumnDecimalReturnScaleError):
                _check_column_types_equal(x, y)

    @given(natives=lists_fixed_length(booleans(), 2))
    def test_interval_native(self, *, natives: typing.Sequence[bool]) -> None:
        native_x, native_y = natives
        x, y = (Interval(native=n) for n in natives)
        if native_x is native_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalIntervalColumnNativeError):
                _check_column_types_equal(x, y)

    @given(second_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_interval_second_precision(
        self, *, second_precisions: typing.Sequence[int | None]
    ) -> None:
        second_precision_x, second_precision_y = second_precisions
        x, y = (Interval(second_precision=sp) for sp in second_precisions)
        if second_precision_x == second_precision_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalIntervalColumnSecondPrecisionError):
                _check_column_types_equal(x, y)

    @given(day_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_interval_day_precision(
        self, *, day_precisions: typing.Sequence[int | None]
    ) -> None:
        day_precision_x, day_precision_y = day_precisions
        x, y = (Interval(day_precision=dp) for dp in day_precisions)
        if day_precision_x == day_precision_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalIntervalColumnDayPrecisionError):
                _check_column_types_equal(x, y)

    @given(lengths=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_large_binary_length(self, *, lengths: typing.Sequence[int | None]) -> None:
        length_x, length_y = lengths
        x, y = (LargeBinary(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalLargeBinaryColumnLengthError):
                _check_column_types_equal(x, y)

    @given(scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_numeric_scale(self, *, scales: typing.Sequence[int | None]) -> None:
        scale_x, scale_y = scales
        x, y = (Numeric(scale=s) for s in scales)
        if scale_x == scale_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalNumericScaleError):
                _check_column_types_equal(x, y)

    @given(
        cls=sampled_from([String, Unicode, UnicodeText]),
        lengths=lists_fixed_length(integers(0, 10) | none(), 2),
    )
    def test_string_length(
        self,
        cls: type[String | Unicode | UnicodeText],
        lengths: typing.Sequence[int | None],
    ) -> None:
        length_x, length_y = lengths
        x, y = (cls(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalStringLengthError):
                _check_column_types_equal(x, y)

    @given(collations=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_string_collation(self, *, collations: typing.Sequence[str | None]) -> None:
        collation_x, collation_y = collations
        x, y = (String(collation=c) for c in collations)
        if collation_x == collation_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalStringCollationError):
                _check_column_types_equal(x, y)

    @given(as_uuids=lists_fixed_length(booleans(), 2))
    def test_uuid_as_uuid(self, *, as_uuids: typing.Sequence[bool]) -> None:
        as_uuid_x, as_uuid_y = as_uuids
        x, y = (Uuid(as_uuid=cast(Any, au)) for au in as_uuids)
        if as_uuid_x is as_uuid_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalUUIDAsUUIDError):
                _check_column_types_equal(x, y)

    @given(native_uuids=lists_fixed_length(booleans(), 2))
    def test_uuid_native_uuid(self, *, native_uuids: typing.Sequence[bool]) -> None:
        native_uuid_x, native_uuid_y = native_uuids
        x, y = (Uuid(native_uuid=nu) for nu in native_uuids)
        if native_uuid_x is native_uuid_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalUUIDNativeUUIDError):
                _check_column_types_equal(x, y)


class TestCheckDataFrameSchemaAgainstTable:
    def test_default(self) -> None:
        df_schema = {"a": int, "b": float}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
        )
        result = check_dataframe_schema_against_table(df_schema, table, eq)
        expected = {"a": "a", "b": "b"}
        assert result == expected

    def test_snake(self) -> None:
        df_schema = {"a": int, "b": float}
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
            Column("A", Integer),
            Column("B", Float),
        )
        result = check_dataframe_schema_against_table(df_schema, table, eq, snake=True)
        expected = {"a": "A", "b": "B"}
        assert result == expected

    def test_df_has_extra_columns(self) -> None:
        df_schema = {"a": int, "b": float, "c": str}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
        )
        result = check_dataframe_schema_against_table(df_schema, table, eq)
        expected = {"a": "a", "b": "b"}
        assert result == expected

    def test_table_has_extra_columns(self) -> None:
        df_schema = {"a": int, "b": float}
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", Integer),
            Column("b", Float),
            Column("c", String),
        )
        result = check_dataframe_schema_against_table(df_schema, table, eq)
        expected = {"a": "a", "b": "b"}
        assert result == expected


class TestCheckEngine:
    @given(engine=sqlite_engines())
    def test_success(self, *, engine: Engine) -> None:
        check_engine(engine)

    @given(root=temp_paths())
    def test_engine_error(self, *, root: Path) -> None:
        engine = create_engine("sqlite", database=str(root))
        with raises(EngineError):
            check_engine(engine)

    @given(engine=sqlite_engines())
    def test_num_tables(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_error(self, *, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_rel_tol_correct(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_engine(engine, num_tables=2, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_num_tables_rel_tol_error(self, *, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=1, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_num_tables_abs_tol_correct(self, *, engine: Engine) -> None:
        check_engine(engine, num_tables=1, abs_tol=1)

    @given(engine=sqlite_engines())
    def test_num_tables_abs_tol_error(self, *, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=2, abs_tol=1)


class TestCheckTableAgainstReflection:
    @given(engine=sqlite_engines())
    def test_reflected(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_table_against_reflection(table, engine)

    @given(engine=sqlite_engines())
    def test_no_such_table(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        with raises(NoSuchTableError):
            _ = check_table_against_reflection(table, engine)


class TestCheckSelectableForDuplicates:
    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        sel = select(table.c.id, table.c.id)
        with raises(DuplicateColumnError):
            check_selectable_for_duplicate_columns(sel)


class TestCheckSeriesAgainstAgainstTable:
    def test_success(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        result = _check_series_against_against_table("b", float, table_schema, eq)
        assert result == "b"

    def test_fail(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        with raises(SeriesAndTableColumnIncompatibleError):
            _ = _check_series_against_against_table("b", int, table_schema, eq)


class TestCheckTablesEqual:
    def test_equal(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        check_tables_equal(table, table)

    def test_column_collections(self) -> None:
        x = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        y = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Integer),
        )
        with raises(UnequalNumberOfColumnsError):
            check_tables_equal(x, y)

    def test_snake_table(self) -> None:
        x = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("Example", MetaData(), Column("id", Integer, primary_key=True))
        check_tables_equal(x, y, snake_table=True)

    def test_snake_columns(self) -> None:
        x = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        check_tables_equal(x, y, snake_columns=True)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            Id = Column(Integer, primary_key=True)

        check_tables_equal(Example, Example)


class TestCheckTableOrColumnNamesEqual:
    @mark.parametrize(
        ("x", "y", "snake", "expected"),
        [
            param("x", "x", False, None),
            param("x", "x", True, None),
            param("x", "X", False, UnequalTableOrColumnNamesError),
            param("x", "X", True, None),
            param("x", "y", False, UnequalTableOrColumnNamesError),
            param("x", "y", True, UnequalTableOrColumnSnakeCaseNamesError),
        ],
    )
    def test_main(
        self, *, x: str, y: str, snake: bool, expected: type[Exception] | None
    ) -> None:
        if expected is None:
            _check_table_or_column_names_equal(x, y, snake=snake)
        else:
            with raises(expected):
                _check_table_or_column_names_equal(x, y, snake=snake)

    @mark.parametrize(("name", "expected"), [param(None, "Id"), param("x", "x")])
    def test_mapped_class(self, *, name: str | None, expected: str) -> None:
        class Kwargs(TypedDict, total=False):
            name: str

        class Example(declarative_base()):
            __tablename__ = "example"

            Id = Column(
                Integer,
                primary_key=True,
                **(cast(Kwargs, {} if name is None else {"name": name})),
            )

        _check_table_or_column_names_equal(Example.Id.name, expected)


class TestColumnwiseMinMax:
    @given(
        engine=sqlite_engines(),
        values=sets(tuples(integers(0, 10) | none(), integers(0, 10) | none())),
    )
    def test_main(
        self, *, engine: Engine, values: set[tuple[int | None, int | None]]
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        ensure_tables_created(engine, table)
        insert_items(engine, ([{"x": x, "y": y} for x, y in values], table))
        sel = select(
            table.c["x"],
            table.c["y"],
            columnwise_min(table.c["x"], table.c["y"]).label("min_xy"),
            columnwise_max(table.c["x"], table.c["y"]).label("max_xy"),
        )
        with engine.begin() as conn:
            res = conn.execute(sel).all()
        assert len(res) == len(values)
        for x, y, min_xy, max_xy in res:
            if (x is None) and (y is None):
                assert min_xy is None
                assert max_xy is None
            elif (x is not None) and (y is None):
                assert min_xy == x
                assert max_xy == x
            elif (x is None) and (y is not None):
                assert min_xy == y
                assert max_xy == y
            else:
                assert min_xy == min(x, y)
                assert max_xy == max(x, y)

    @given(engine=sqlite_engines())
    def test_label(self, *, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
        )
        ensure_tables_created(engine, table)

        sel = select(columnwise_min(table.c.x, table.c.x))
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestCreateEngine:
    @given(temp_path=temp_paths())
    def test_main(self, *, temp_path: Path) -> None:
        engine = create_engine("sqlite", database=temp_path.name)
        assert isinstance(engine, Engine)

    @given(temp_path=temp_paths())
    def test_query(self, *, temp_path: Path) -> None:
        engine = create_engine(
            "sqlite",
            database=temp_path.name,
            query={"arg1": "value1", "arg2": ["value2"]},
        )
        assert isinstance(engine, Engine)


class TestDialect:
    @mark.parametrize("dialect", Dialect)
    def test_max_params(self, *, dialect: Dialect) -> None:
        assert isinstance(dialect.max_params, int)


class TestEnsureEngine:
    @given(data=data(), engine=sqlite_engines())
    def test_main(self, *, data: DataObject, engine: Engine) -> None:
        maybe_engine = data.draw(
            sampled_from([engine, engine.url.render_as_string(hide_password=False)])
        )
        result = ensure_engine(maybe_engine)
        assert result.url == engine.url


class TestEnsureTablesCreated:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_table(self, *, engine: Engine, runs: int) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_mapped_class(self, *, engine: Engine, runs: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_mapped_class: Table | type[Any], engine: Engine, runs: int, /
    ) -> None:
        sel = get_table(table_or_mapped_class).select()
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)
        for _ in range(runs):
            ensure_tables_created(engine, table_or_mapped_class)
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestEnsureTablesDropped:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_table(self, *, engine: Engine, runs: int) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_mapped_class(self, *, engine: Engine, runs: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_mapped_class: Table | type[Any], engine: Engine, runs: int, /
    ) -> None:
        table = get_table(table_or_mapped_class)
        sel = table.select()
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(sel).all()
        for _ in range(runs):
            ensure_tables_dropped(engine, table_or_mapped_class)
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestGetColumnNames:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        assert get_column_names(table_or_mapped_class) == ["id_"]


class TestGetColumns:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        columns = get_columns(table_or_mapped_class)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @given(engine=sqlite_engines())
    def test_sqlite(self, *, engine: Engine) -> None:
        assert get_dialect(engine) is Dialect.sqlite

    @mark.parametrize(
        ("url", "expected"),
        [
            param(
                "mssql+pyodbc://scott:tiger@mydsn",
                Dialect.mssql,
                marks=skipif_not_linux,
            ),
            param(
                "mysql://scott:tiger@localhost/foo",
                Dialect.mysql,
                marks=skipif_not_linux,
            ),
            param("oracle://scott:tiger@127.0.0.1:1521/sidname", Dialect.oracle),
            param(
                "postgresql://scott:tiger@localhost/mydatabase",
                Dialect.postgresql,
                marks=skipif_not_linux,
            ),
        ],
    )
    def test_non_sqlite(self, *, url: str, expected: Dialect) -> None:
        assert get_dialect(_create_engine(url)) is expected


class TestGetTable:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table(table)
        assert result is table

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__

    def test_error(self) -> None:
        with raises(NotATableOrAMappedClassError):
            _ = get_table(type(None))


class TestGetTableName:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table_name(table)
        expected = "example"
        assert result == expected

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        result = get_table_name(Example)
        expected = "example"
        assert result == expected


class TestInsertItems:
    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_tuple_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ((id_,), self._table))

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_dict_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ({"id": id_}, self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), max_size=10))
    def test_pair_of_lists_of_tuples_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([((id_,)) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_pair_of_lists_of_dicts_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([({"id": id_}) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_list_of_pairs_of_tuples_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [(((id_,), self._table)) for id_ in ids])

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10)))
    def test_list_of_pairs_of_dicts_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [({"id": id_}, self._table) for id_ in ids])

    @given(
        engine=sqlite_engines(),
        ids=sets(integers(0, 10000), min_size=1000, max_size=1000),
    )
    def test_many_items(self, *, engine: Engine, ids: set[int]) -> None:
        self._run_test(engine, ids, [({"id": id_}, self._table) for id_ in ids])

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_mapped_class(self, *, engine: Engine, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id = Column(Integer, primary_key=True)  # noqa: A003

        self._run_test(engine, {id_}, Example(id=id_))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id", Integer, primary_key=True))

    def _run_test(self, engine: Engine, ids: set[int], /, *args: Any) -> None:
        ensure_tables_created(engine, self._table)
        insert_items(engine, *args)
        sel = select(self._table.c["id"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestInsertItemsCollect:
    def test_tuple_not_a_pair_error(self) -> None:
        with raises(TupleNotAPairError):
            _ = list(_insert_items_collect((None,)))

    def test_second_argument_not_a_table_or_mapped_class_error(self) -> None:
        with raises(SecondArgumentNotATableOrMappedClassError):
            _ = list(_insert_items_collect((None, None)))

    @given(id_=integers())
    def test_pair_with_tuple_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(((id_,), table)))
        expected = [_InsertionItem(values=(id_,), table=table)]
        assert result == expected

    @given(id_=integers())
    def test_pair_with_dict_data(self, *, id_: int) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(({"id": id_}, table)))
        expected = [_InsertionItem(values={"id": id_}, table=table)]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_tuple_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(([(id_,) for id_ in ids], table)))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_with_list_of_dict_data(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect(([{"id": id_} for id_ in ids], table)))
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    def test_first_argument_invalid_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(FirstArgumentInvalidError):
            _ = list(_insert_items_collect((None, table)))

    @given(ids=sets(integers()))
    def test_list(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect([((id_,), table) for id_ in ids]))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_set(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect({((id_,), table) for id_ in ids}))
        assert {one(r.values) for r in result} == ids

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        item = Example(id_=id_)
        result = list(_insert_items_collect(item))
        expected = [_InsertionItem(values={"id_": id_}, table=get_table(Example))]
        assert result == expected

    def test_invalid_item_error(self) -> None:
        with raises(InvalidItemError):
            _ = list(_insert_items_collect(None))


class TestInsertItemsCollectIterable:
    @given(ids=sets(integers()))
    def test_list_of_tuples(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(_insert_items_collect_iterable([(id_,) for id_ in ids], table))
        expected = [_InsertionItem(values=(id_,), table=table) for id_ in ids]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list_of_dicts(self, *, ids: set[int]) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        result = list(
            _insert_items_collect_iterable([{"id": id_} for id_ in ids], table)
        )
        expected = [_InsertionItem(values={"id": id_}, table=table) for id_ in ids]
        assert result == expected

    def test_invalid_item_in_iterable_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(InvalidItemInIterableError):
            _ = list(_insert_items_collect_iterable([None], table))


class TestIsMappedClass:
    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        assert is_mapped_class(Example)

    def test_other(self) -> None:
        assert not is_mapped_class(int)


class TestIsTableOrMappedClass:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert is_table_or_mapped_class(table)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        assert is_table_or_mapped_class(Example)

    def test_other(self) -> None:
        assert not is_table_or_mapped_class(int)


class TestMappedClassToDict:
    @given(id_=integers())
    def test_main(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"
            id_ = Column(Integer, primary_key=True)

        example = Example(id_=id_)
        assert mapped_class_to_dict(example) == {"id_": id_}

    @given(id_=integers())
    def test_explicitly_named_column(self, *, id_: int) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"
            ID = Column(Integer, primary_key=True, name="id")

        example = Example(ID=id_)
        assert mapped_class_to_dict(example) == {"id": id_}


class TestMatchSeriesNameToTableColumn:
    def test_match_default(self) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        result = _match_series_name_to_table_column("b", table_schema)
        assert result == ("b", float)

    @mark.parametrize("sr_name", [param("b"), param("B")])
    def test_match_snake(self, *, sr_name: str) -> None:
        table_schema = {"A": int, "B": float, "C": str}
        result = _match_series_name_to_table_column(sr_name, table_schema, snake=True)
        assert result == ("B", float)

    @mark.parametrize("snake", [param(True), param(False)])
    def test_series_matches_against_no_column_error(self, *, snake: bool) -> None:
        table_schema = {"a": int, "b": float, "c": str}
        with raises(SeriesMatchesAgainstNoColumnError):
            _ = _match_series_name_to_table_column("value", table_schema, snake=snake)

    def test_series_matches_against_multiple_columns_error(self) -> None:
        table_schema = {"a": int, "b": float, "B": float, "c": str}
        with raises(SeriesMatchesAgainstMultipleColumnsError):
            _ = _match_series_name_to_table_column("b", table_schema, snake=True)


class TestParseEngine:
    @given(engine=sqlite_engines())
    def test_str(self, *, engine: Engine) -> None:
        url = engine.url
        result = parse_engine(url.render_as_string(hide_password=False))
        assert result.url == url

    def test_error(self) -> None:
        with raises(ParseEngineError):
            _ = parse_engine("error")


class TestRedirectToNoSuchSequenceError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        seq = sqlalchemy.Sequence("example")
        with raises(NotImplementedError), engine.begin() as conn:
            _ = conn.scalar(seq)


class TestRedirectToNoSuchTableError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(select(table))
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestRedirectToTableAlreadyExistsError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with engine.begin() as conn:
            _ = table.create(conn)
        with raises(TableAlreadyExistsError), engine.begin() as conn:
            try:
                _ = table.create(conn)
            except DatabaseError as error:
                redirect_to_table_already_exists_error(engine, error)


class TestReflectTable:
    @given(
        engine=sqlite_engines(),
        col_type=sampled_from(
            [
                INTEGER,
                INTEGER(),
                NVARCHAR,
                NVARCHAR(),
                NVARCHAR(1),
                Integer,
                Integer(),
                String,
                String(),
                String(1),
            ]
        ),
    )
    def test_reflected(self, *, engine: Engine, col_type: Any) -> None:
        table = Table("example", MetaData(), Column("Id", col_type, primary_key=True))
        ensure_tables_created(engine, table)
        reflected = _reflect_table(table, engine)
        check_tables_equal(reflected, table)

    @given(engine=sqlite_engines())
    def test_no_such_table(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        with raises(NoSuchTableError):
            _ = _reflect_table(table, engine)


class TestSerializeEngine:
    @given(data=data())
    def test_main(self, *, data: DataObject) -> None:
        engine = data.draw(sqlite_engines())
        result = parse_engine(serialize_engine(engine))
        assert result.url == engine.url


class TestTablenameMixin:
    def test_main(self) -> None:
        class Example(declarative_base(cls=TablenameMixin)):
            Id = Column(Integer, primary_key=True)

        assert get_table_name(Example) == "example"


class TestYieldConnection:
    @given(engine=sqlite_engines())
    def test_engine(self, *, engine: Engine) -> None:
        with yield_connection(engine) as conn:
            assert isinstance(conn, Connection)

    @given(engine=sqlite_engines())
    def test_connection(self, *, engine: Engine) -> None:
        with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, Connection)
