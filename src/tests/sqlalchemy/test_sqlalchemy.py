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
from sqlalchemy.exc import DatabaseError, NoSuchTableError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import (
    lists_fixed_length,
    sqlite_engines,
    temp_paths,
    text_ascii,
)
from utilities.sqlalchemy import (
    CheckEngineError,
    ParseEngineError,
    TableAlreadyExistsError,
    TablenameMixin,
    _check_column_collections_equal,
    _check_column_types_boolean_equal,
    _check_column_types_datetime_equal,
    _check_column_types_enum_equal,
    _check_column_types_equal,
    _check_column_types_float_equal,
    _check_column_types_interval_equal,
    _check_column_types_large_binary_equal,
    _check_column_types_numeric_equal,
    _check_column_types_string_equal,
    _check_column_types_uuid_equal,
    _check_columns_equal,
    _check_table_or_column_names_equal,
    _check_tables_equal,
    _CheckColumnCollectionsEqualError,
    _CheckColumnsEqualError,
    _CheckColumnTypesBooleanEqualError,
    _CheckColumnTypesDateTimeEqualError,
    _CheckColumnTypesEnumEqualError,
    _CheckColumnTypesEqualError,
    _CheckColumnTypesFloatEqualError,
    _CheckColumnTypesIntervalEqualError,
    _CheckColumnTypesLargeBinaryEqualError,
    _CheckColumnTypesNumericEqualError,
    _CheckColumnTypesStringEqualError,
    _CheckColumnTypesUuidEqualError,
    _CheckTableOrColumnNamesEqualError,
    check_dataframe_schema_against_table,
    check_engine,
    check_table_against_reflection,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_engine,
    ensure_tables_created,
    ensure_tables_dropped,
    get_table,
    get_table_name,
    insert_items,
    parse_engine,
    redirect_to_no_such_table_error,
    redirect_to_table_already_exists_error,
    reflect_table,
    serialize_engine,
)


class TestCheckColumnCollectionsEqual:
    def test_main(self) -> None:
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

    @mark.parametrize(
        ("x", "y"),
        [
            param(
                Table("x", MetaData(), Column("id", Integer, primary_key=True)),
                Table(
                    "y",
                    MetaData(),
                    Column("id", Integer, primary_key=True),
                    Column("value", Integer),
                ),
            ),
            param(
                Table("x", MetaData(), Column("id1", Integer, primary_key=True)),
                Table("y", MetaData(), Column("id2", Integer, primary_key=True)),
            ),
        ],
    )
    def test_errors(self, *, x: Table, y: Table) -> None:
        with raises(_CheckColumnCollectionsEqualError):
            _check_column_collections_equal(x.columns, y.columns)


class TestCheckColumnsEqual:
    def test_equal(self) -> None:
        x = Column("id", Integer)
        _check_columns_equal(x, x)

    def test_snake(self) -> None:
        x = Column("id", Integer)
        y = Column("Id", Integer)
        _check_columns_equal(x, y, snake=True)

    def test_primary_key_off(self) -> None:
        x = Column("id", Integer, primary_key=True)
        y = Column("id", Integer, nullable=False)
        _check_columns_equal(x, y, primary_key=False)

    @mark.parametrize(
        ("x", "y"),
        [
            param(Column("id", Integer, primary_key=True), Column("id", Integer)),
            param(Column("id", Integer), Column("id", Integer, nullable=False)),
        ],
    )
    def test_errors(self, *, x: Any, y: Any) -> None:
        with raises(_CheckColumnsEqualError):
            _check_columns_equal(x, y)


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

    @mark.parametrize(
        "cls",
        [
            param(Boolean),
            param(DateTime),
            param(Float),
            param(Interval),
            param(LargeBinary),
            param(Numeric),
            param(String),
            param(Unicode),
            param(UnicodeText),
            param(Uuid),
        ],
    )
    def test_equal_for_primaries(self, *, cls: type[Any]) -> None:
        _check_column_types_equal(cls(), cls())

    def test_equal_for_primaries_enum(self) -> None:
        class Example(enum.Enum):
            member = auto()

        _check_column_types_equal(sqlalchemy.Enum(Example), sqlalchemy.Enum(Example))

    @given(data=data())
    def test_equal_across_groups(self, *, data: DataObject) -> None:
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
        with raises(_CheckColumnTypesEqualError):
            _check_column_types_equal(x, y)


class TestCheckColumnTypesBooleanEqual:
    @given(create_constraints=lists_fixed_length(booleans(), 2))
    def test_create_constraint(
        self, *, create_constraints: typing.Sequence[bool]
    ) -> None:
        create_constraint_x, create_constraint_y = create_constraints
        x, y = (Boolean(create_constraint=cs) for cs in create_constraints)
        if create_constraint_x is create_constraint_y:
            _check_column_types_boolean_equal(x, y)
        else:
            with raises(_CheckColumnTypesBooleanEqualError):
                _check_column_types_boolean_equal(x, y)

    @given(names=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_name(self, *, names: typing.Sequence[str | None]) -> None:
        name_x, name_y = names
        x, y = (Boolean(name=n) for n in names)
        if name_x == name_y:
            _check_column_types_boolean_equal(x, y)
        else:
            with raises(_CheckColumnTypesBooleanEqualError):
                _check_column_types_boolean_equal(x, y)


class TestCheckColumnTypesDateTimeEqual:
    @given(timezones=lists_fixed_length(booleans(), 2))
    def test_main(self, *, timezones: typing.Sequence[bool]) -> None:
        timezone_x, timezone_y = timezones
        x, y = (DateTime(timezone=tz) for tz in timezones)
        if timezone_x is timezone_y:
            _check_column_types_datetime_equal(x, y)
        else:
            with raises(_CheckColumnTypesDateTimeEqualError):
                _check_column_types_datetime_equal(x, y)


class TestCheckColumnTypesEnumEqual:
    def test_no_enum_classes(self) -> None:
        x = sqlalchemy.Enum()
        _check_column_types_enum_equal(x, x)

    @given(data=data())
    def test_one_enum_class(self, *, data: DataObject) -> None:
        class Example(enum.Enum):
            member = auto()

        x = sqlalchemy.Enum(Example)
        y = sqlalchemy.Enum()
        x, y = data.draw(permutations([x, y]))
        with raises(_CheckColumnTypesEnumEqualError):
            _check_column_types_enum_equal(x, y)

    def test_two_enum_classes(self) -> None:
        class EnumX(enum.Enum):
            member = auto()

        class EnumY(enum.Enum):
            member = auto()

        x, y = (sqlalchemy.Enum(e) for e in [EnumX, EnumY])
        with raises(_CheckColumnTypesEnumEqualError):
            _check_column_types_enum_equal(x, y)

    @given(create_constraints=lists_fixed_length(booleans(), 2))
    def test_create_constraint(
        self, *, create_constraints: typing.Sequence[bool]
    ) -> None:
        class Example(enum.Enum):
            member = auto()

        create_constraint_x, create_constraint_y = create_constraints
        x, y = (
            sqlalchemy.Enum(Example, create_constraint=cs) for cs in create_constraints
        )
        if create_constraint_x is create_constraint_y:
            _check_column_types_enum_equal(x, y)
        else:
            with raises(_CheckColumnTypesEnumEqualError):
                _check_column_types_enum_equal(x, y)

    @given(native_enums=lists_fixed_length(booleans(), 2))
    def test_native_enum(self, *, native_enums: typing.Sequence[bool]) -> None:
        class Example(enum.Enum):
            member = auto()

        native_enum_x, native_enum_y = native_enums
        x, y = (sqlalchemy.Enum(Example, native_enum=ne) for ne in native_enums)
        if native_enum_x is native_enum_y:
            _check_column_types_enum_equal(x, y)
        else:
            with raises(_CheckColumnTypesEnumEqualError):
                _check_column_types_enum_equal(x, y)

    @given(lengths=lists_fixed_length(integers(6, 10), 2))
    def test_length(self, *, lengths: typing.Sequence[int]) -> None:
        class Example(enum.Enum):
            member = auto()

        length_x, length_y = lengths
        x, y = (sqlalchemy.Enum(Example, length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_enum_equal(x, y)
        else:
            with raises(_CheckColumnTypesEnumEqualError):
                _check_column_types_enum_equal(x, y)

    @given(inherit_schemas=lists_fixed_length(booleans(), 2))
    def test_inherit_schema(self, *, inherit_schemas: typing.Sequence[bool]) -> None:
        class Example(enum.Enum):
            member = auto()

        inherit_schema_x, inherit_schema_y = inherit_schemas
        x, y = (sqlalchemy.Enum(Example, inherit_schema=is_) for is_ in inherit_schemas)
        if inherit_schema_x is inherit_schema_y:
            _check_column_types_enum_equal(x, y)
        else:
            with raises(_CheckColumnTypesEnumEqualError):
                _check_column_types_enum_equal(x, y)


class TestCheckColumnTypesFloatEqual:
    @given(precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_precision(self, *, precisions: typing.Sequence[int | None]) -> None:
        precision_x, precision_y = precisions
        x, y = (Float(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_float_equal(x, y)
        else:
            with raises(_CheckColumnTypesFloatEqualError):
                _check_column_types_float_equal(x, y)

    @given(asdecimals=lists_fixed_length(booleans(), 2))
    def test_asdecimal(self, *, asdecimals: typing.Sequence[bool]) -> None:
        asdecimal_x, asdecimal_y = asdecimals
        x, y = (Float(asdecimal=cast(Any, a)) for a in asdecimals)
        if asdecimal_x is asdecimal_y:
            _check_column_types_float_equal(x, y)
        else:
            with raises(_CheckColumnTypesFloatEqualError):
                _check_column_types_float_equal(x, y)

    @given(dec_ret_scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_decimal_return_scale(
        self, *, dec_ret_scales: typing.Sequence[int | None]
    ) -> None:
        dec_ret_scale_x, dec_ret_scale_y = dec_ret_scales
        x, y = (Float(decimal_return_scale=drs) for drs in dec_ret_scales)
        if dec_ret_scale_x == dec_ret_scale_y:
            _check_column_types_float_equal(x, y)
        else:
            with raises(_CheckColumnTypesFloatEqualError):
                _check_column_types_float_equal(x, y)


class TestCheckColumnTypesIntervalEqual:
    @given(natives=lists_fixed_length(booleans(), 2))
    def test_native(self, *, natives: typing.Sequence[bool]) -> None:
        native_x, native_y = natives
        x, y = (Interval(native=n) for n in natives)
        if native_x is native_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)

    @given(second_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_second_precision(
        self, *, second_precisions: typing.Sequence[int | None]
    ) -> None:
        second_precision_x, second_precision_y = second_precisions
        x, y = (Interval(second_precision=sp) for sp in second_precisions)
        if second_precision_x == second_precision_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)

    @given(day_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_day_precision(
        self, *, day_precisions: typing.Sequence[int | None]
    ) -> None:
        day_precision_x, day_precision_y = day_precisions
        x, y = (Interval(day_precision=dp) for dp in day_precisions)
        if day_precision_x == day_precision_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)


class TestCheckColumnTypesLargeBinaryEqual:
    @given(lengths=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_main(self, *, lengths: typing.Sequence[int | None]) -> None:
        length_x, length_y = lengths
        x, y = (LargeBinary(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_large_binary_equal(x, y)
        else:
            with raises(_CheckColumnTypesLargeBinaryEqualError):
                _check_column_types_large_binary_equal(x, y)


class TestCheckColumnTypesNumericEqual:
    @given(precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_precision(self, *, precisions: typing.Sequence[int | None]) -> None:
        precision_x, precision_y = precisions
        x, y = (Numeric(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(asdecimals=lists_fixed_length(booleans(), 2))
    def test_asdecimal(self, *, asdecimals: typing.Sequence[bool]) -> None:
        asdecimal_x, asdecimal_y = asdecimals
        x, y = (Numeric(asdecimal=cast(Any, a)) for a in asdecimals)
        if asdecimal_x is asdecimal_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_numeric_scale(self, *, scales: typing.Sequence[int | None]) -> None:
        scale_x, scale_y = scales
        x, y = (Numeric(scale=s) for s in scales)
        if scale_x == scale_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(dec_ret_scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_decimal_return_scale(
        self, *, dec_ret_scales: typing.Sequence[int | None]
    ) -> None:
        dec_ret_scale_x, dec_ret_scale_y = dec_ret_scales
        x, y = (Numeric(decimal_return_scale=drs) for drs in dec_ret_scales)
        if dec_ret_scale_x == dec_ret_scale_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)


class TestCheckColumnTypesStringEqual:
    @given(
        cls=sampled_from([String, Unicode, UnicodeText]),
        lengths=lists_fixed_length(integers(0, 10) | none(), 2),
    )
    def test_length(
        self,
        *,
        cls: type[String | Unicode | UnicodeText],
        lengths: typing.Sequence[int | None],
    ) -> None:
        length_x, length_y = lengths
        x, y = (cls(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_string_equal(x, y)
        else:
            with raises(_CheckColumnTypesStringEqualError):
                _check_column_types_string_equal(x, y)

    @given(collations=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_collation(self, *, collations: typing.Sequence[str | None]) -> None:
        collation_x, collation_y = collations
        x, y = (String(collation=c) for c in collations)
        if collation_x == collation_y:
            _check_column_types_string_equal(x, y)
        else:
            with raises(_CheckColumnTypesStringEqualError):
                _check_column_types_string_equal(x, y)


class TestCheckColumnTypesUuidEqual:
    @given(as_uuids=lists_fixed_length(booleans(), 2))
    def test_as_uuid(self, *, as_uuids: typing.Sequence[bool]) -> None:
        as_uuid_x, as_uuid_y = as_uuids
        x, y = (Uuid(as_uuid=cast(Any, au)) for au in as_uuids)
        if as_uuid_x is as_uuid_y:
            _check_column_types_uuid_equal(x, y)
        else:
            with raises(_CheckColumnTypesUuidEqualError):
                _check_column_types_uuid_equal(x, y)

    @given(native_uuids=lists_fixed_length(booleans(), 2))
    def test_native_uuid(self, *, native_uuids: typing.Sequence[bool]) -> None:
        native_uuid_x, native_uuid_y = native_uuids
        x, y = (Uuid(native_uuid=nu) for nu in native_uuids)
        if native_uuid_x is native_uuid_y:
            _check_column_types_uuid_equal(x, y)
        else:
            with raises(_CheckColumnTypesUuidEqualError):
                _check_column_types_uuid_equal(x, y)


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
    def test_main(self, *, engine: Engine) -> None:
        check_engine(engine)

    @given(engine=sqlite_engines())
    def test_num_tables(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_rel_tol(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_engine(engine, num_tables=2, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_num_tables_abs_tol(self, *, engine: Engine) -> None:
        check_engine(engine, num_tables=1, abs_tol=1)

    @given(root=temp_paths())
    def test_error_unable_to_open(self, *, root: Path) -> None:
        engine = create_engine("sqlite", database=str(root))
        with raises(CheckEngineError):
            check_engine(engine)

    @given(engine=sqlite_engines())
    def test_error_num_tables_int(self, *, engine: Engine) -> None:
        with raises(CheckEngineError):
            check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_error_num_tables_rel_tol(self, *, engine: Engine) -> None:
        with raises(CheckEngineError):
            check_engine(engine, num_tables=1, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_error_num_tables_abs_tol(self, *, engine: Engine) -> None:
        with raises(CheckEngineError):
            check_engine(engine, num_tables=2, abs_tol=1)


class TestCheckTableAgainstReflection:
    @given(engine=sqlite_engines())
    def test_reflected(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_table_against_reflection(table, engine)

    @given(engine=sqlite_engines())
    def test_error_no_such_table(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        with raises(NoSuchTableError):
            _ = check_table_against_reflection(table, engine)


class TestCheckTablesEqual:
    def test_main(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        _check_tables_equal(table, table)

    def test_snake_table(self) -> None:
        x = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("Example", MetaData(), Column("id", Integer, primary_key=True))
        _check_tables_equal(x, y, snake_table=True)

    def test_snake_columns(self) -> None:
        x = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        _check_tables_equal(x, y, snake_columns=True)

    def test_mapped_class(self) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            Id = Column(Integer, primary_key=True)

        _check_tables_equal(Example, Example)


class TestCheckTableOrColumnNamesEqual:
    @mark.parametrize(
        ("x", "y", "snake", "success"),
        [
            param("x", "x", False, True),
            param("x", "x", True, True),
            param("x", "X", False, False),
            param("x", "X", True, True),
            param("x", "y", False, False),
            param("x", "y", True, False),
        ],
    )
    def test_main(self, *, x: str, y: str, snake: bool, success: bool) -> None:
        if success:
            _check_table_or_column_names_equal(x, y, snake=snake)
        else:
            with raises(_CheckTableOrColumnNamesEqualError):
                _check_table_or_column_names_equal(x, y, snake=snake)

    @mark.parametrize(("name", "expected"), [param(None, "Id"), param("x", "x")])
    def test_quoted_name(self, *, name: str | None, expected: str) -> None:
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
    def test_main(self, *, engine: Engine, col_type: Any) -> None:
        table = Table("example", MetaData(), Column("Id", col_type, primary_key=True))
        ensure_tables_created(engine, table)
        reflected = reflect_table(table, engine)
        _check_tables_equal(reflected, table)

    @given(engine=sqlite_engines())
    def test_error(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("Id", Integer, primary_key=True))
        with raises(NoSuchTableError):
            _ = reflect_table(table, engine)


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
