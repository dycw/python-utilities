import enum
from enum import auto
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Union
from typing import cast

import sqlalchemy
from hypothesis import assume
from hypothesis import given
from hypothesis.strategies import DataObject
from hypothesis.strategies import booleans
from hypothesis.strategies import data
from hypothesis.strategies import fixed_dictionaries
from hypothesis.strategies import integers
from hypothesis.strategies import lists
from hypothesis.strategies import none
from hypothesis.strategies import permutations
from hypothesis.strategies import sampled_from
from hypothesis.strategies import sets
from hypothesis_sqlalchemy.sample import table_records_lists
from pytest import mark
from pytest import param
from pytest import raises
from sqlalchemy import BIGINT
from sqlalchemy import BINARY
from sqlalchemy import BOOLEAN
from sqlalchemy import CHAR
from sqlalchemy import CLOB
from sqlalchemy import DATE
from sqlalchemy import DATETIME
from sqlalchemy import DECIMAL
from sqlalchemy import DOUBLE
from sqlalchemy import DOUBLE_PRECISION
from sqlalchemy import FLOAT
from sqlalchemy import INT
from sqlalchemy import INTEGER
from sqlalchemy import NCHAR
from sqlalchemy import NUMERIC
from sqlalchemy import NVARCHAR
from sqlalchemy import REAL
from sqlalchemy import SMALLINT
from sqlalchemy import TEXT
from sqlalchemy import TIME
from sqlalchemy import TIMESTAMP
from sqlalchemy import UUID
from sqlalchemy import VARBINARY
from sqlalchemy import VARCHAR
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Double
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import LargeBinary
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import Time
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import Uuid
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import declarative_base

from utilities.hypothesis import lists_fixed_length
from utilities.hypothesis import temp_paths
from utilities.hypothesis import text_ascii
from utilities.hypothesis.sqlalchemy import sqlite_engines
from utilities.sqlalchemy import EngineError
from utilities.sqlalchemy import IncorrectNumberOfTablesError
from utilities.sqlalchemy import TableAlreadyExistsError
from utilities.sqlalchemy import UnequalBooleanColumnCreateConstraintError
from utilities.sqlalchemy import UnequalBooleanColumnNameError
from utilities.sqlalchemy import UnequalColumnTypesError
from utilities.sqlalchemy import UnequalDateTimeColumnTimezoneError
from utilities.sqlalchemy import UnequalEnumColumnCreateConstraintError
from utilities.sqlalchemy import UnequalEnumColumnInheritSchemaError
from utilities.sqlalchemy import UnequalEnumColumnLengthError
from utilities.sqlalchemy import UnequalEnumColumnNativeEnumError
from utilities.sqlalchemy import UnequalEnumColumnTypesError
from utilities.sqlalchemy import UnequalFloatColumnAsDecimalError
from utilities.sqlalchemy import UnequalFloatColumnDecimalReturnScaleError
from utilities.sqlalchemy import UnequalFloatColumnPrecisionsError
from utilities.sqlalchemy import UnequalIntervalColumnDayPrecisionError
from utilities.sqlalchemy import UnequalIntervalColumnNativeError
from utilities.sqlalchemy import UnequalIntervalColumnSecondPrecisionError
from utilities.sqlalchemy import UnequalLargeBinaryColumnLengthError
from utilities.sqlalchemy import UnequalNullableStatusError
from utilities.sqlalchemy import UnequalNumberOfColumnsError
from utilities.sqlalchemy import UnequalNumericScaleError
from utilities.sqlalchemy import UnequalPrimaryKeyStatusError
from utilities.sqlalchemy import UnequalSetOfColumnsError
from utilities.sqlalchemy import UnequalStringCollationError
from utilities.sqlalchemy import UnequalStringLengthError
from utilities.sqlalchemy import UnequalTableOrColumnNamesError
from utilities.sqlalchemy import UnequalTableOrColumnSnakeCaseNamesError
from utilities.sqlalchemy import UnequalUUIDAsUUIDError
from utilities.sqlalchemy import UnequalUUIDNativeUUIDError
from utilities.sqlalchemy import _check_column_collections_equal
from utilities.sqlalchemy import _check_column_types_equal
from utilities.sqlalchemy import _check_columns_equal
from utilities.sqlalchemy import _check_table_or_column_names_equal
from utilities.sqlalchemy import _reflect_table
from utilities.sqlalchemy import check_engine
from utilities.sqlalchemy import check_table_against_reflection
from utilities.sqlalchemy import check_tables_equal
from utilities.sqlalchemy import columnwise_max
from utilities.sqlalchemy import columnwise_min
from utilities.sqlalchemy import create_engine
from utilities.sqlalchemy import ensure_table_created
from utilities.sqlalchemy import ensure_table_dropped
from utilities.sqlalchemy import get_column_names
from utilities.sqlalchemy import get_columns
from utilities.sqlalchemy import get_dialect
from utilities.sqlalchemy import get_table
from utilities.sqlalchemy import get_table_name
from utilities.sqlalchemy import model_to_dict
from utilities.sqlalchemy import redirect_to_no_such_table_error
from utilities.sqlalchemy import redirect_to_table_already_exists_error
from utilities.sqlalchemy import yield_connection
from utilities.sqlalchemy import yield_in_clause_rows


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
        _check_column_collections_equal(
            x.columns,
            y.columns,
            snake=True,
        )

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
        _check_column_collections_equal(
            x.columns,
            y.columns,
            allow_permutations=True,
        )

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
            x.columns,
            y.columns,
            snake=True,
            allow_permutations=True,
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
    def test_unequal_column_types(self, allow_permutation: bool) -> None:
        x = Table("x", MetaData(), Column("id", Integer, primary_key=True))
        y = Table("y", MetaData(), Column("id", String, primary_key=True))
        with raises(UnequalColumnTypesError):
            _check_column_collections_equal(
                x.columns,
                y.columns,
                allow_permutations=allow_permutation,
            )


class TestCheckColumnTypesEqual:
    groups = [
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
    ]

    @given(data=data())
    def test_equal(self, data: DataObject) -> None:
        group = data.draw(sampled_from(self.groups))
        cls = data.draw(sampled_from(group))
        elements = sampled_from([cls, cls()])
        x, y = data.draw(lists_fixed_length(elements, 2))
        _check_column_types_equal(x, y)

    @given(data=data())
    def test_unequal(self, data: DataObject) -> None:
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
        self,
        create_constraints: list[bool],
    ) -> None:
        create_constraint_x, create_constraint_y = create_constraints
        x, y = (Boolean(create_constraint=cs) for cs in create_constraints)
        if create_constraint_x is create_constraint_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalBooleanColumnCreateConstraintError):
                _check_column_types_equal(x, y)

    @given(names=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_boolean_name(self, names: list[Optional[str]]) -> None:
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
    def test_datetime_timezone(self, timezones: list[bool]) -> None:
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
    def test_enum_one_enum_class(self, data: DataObject) -> None:
        class MyEnum(enum.Enum):
            member = auto()

        x = sqlalchemy.Enum(MyEnum)
        y = data.draw(sampled_from([sqlalchemy.Enum, sqlalchemy.Enum()]))
        x, y = data.draw(permutations([x, y]))
        with raises(UnequalEnumColumnTypesError):
            _check_column_types_equal(x, y)

    @given(create_constraints=lists_fixed_length(booleans(), 2))
    def test_enum_create_constraint(
        self,
        create_constraints: list[bool],
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
    def test_enum_native_enum(self, native_enums: list[bool]) -> None:
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
    def test_enum_length(self, lengths: list[int]) -> None:
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
    def test_enum_inherit_schema(self, inherit_schemas: list[bool]) -> None:
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
        cls: Union[type[Float], type[Numeric]],
        precisions: list[Optional[int]],
    ) -> None:
        precision_x, precision_y = precisions
        x, y = (cls(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalFloatColumnPrecisionsError):
                _check_column_types_equal(x, y)

    @given(
        cls=sampled_from([Float, Numeric]),
        asdecimals=lists_fixed_length(booleans(), 2),
    )
    def test_float_asdecimal(
        self,
        cls: Union[type[Float], type[Numeric]],
        asdecimals: list[bool],
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
        cls: Union[type[Float], type[Numeric]],
        dec_ret_scales: list[Optional[int]],
    ) -> None:
        dec_ret_scale_x, dec_ret_scale_y = dec_ret_scales
        x, y = (cls(decimal_return_scale=drs) for drs in dec_ret_scales)
        if dec_ret_scale_x == dec_ret_scale_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalFloatColumnDecimalReturnScaleError):
                _check_column_types_equal(x, y)

    @given(natives=lists_fixed_length(booleans(), 2))
    def test_interval_native(self, natives: list[bool]) -> None:
        native_x, native_y = natives
        x, y = (Interval(native=n) for n in natives)
        if native_x is native_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalIntervalColumnNativeError):
                _check_column_types_equal(x, y)

    @given(second_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_interval_second_precision(
        self,
        second_precisions: list[Optional[int]],
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
        self,
        day_precisions: list[Optional[int]],
    ) -> None:
        day_precision_x, day_precision_y = day_precisions
        x, y = (Interval(day_precision=dp) for dp in day_precisions)
        if day_precision_x == day_precision_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalIntervalColumnDayPrecisionError):
                _check_column_types_equal(x, y)

    @given(lengths=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_large_binary_length(
        self,
        lengths: list[Optional[int]],
    ) -> None:
        length_x, length_y = lengths
        x, y = (LargeBinary(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalLargeBinaryColumnLengthError):
                _check_column_types_equal(x, y)

    @given(scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_numeric_scale(self, scales: list[Optional[int]]) -> None:
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
        cls: Union[type[String], type[Unicode], type[UnicodeText]],
        lengths: list[Optional[int]],
    ) -> None:
        length_x, length_y = lengths
        x, y = (cls(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalStringLengthError):
                _check_column_types_equal(x, y)

    @given(collations=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_string_collation(self, collations: list[Optional[str]]) -> None:
        collation_x, collation_y = collations
        x, y = (String(collation=c) for c in collations)
        if collation_x == collation_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalStringCollationError):
                _check_column_types_equal(x, y)

    @given(as_uuids=lists_fixed_length(booleans(), 2))
    def test_uuid_as_uuid(self, as_uuids: list[bool]) -> None:
        as_uuid_x, as_uuid_y = as_uuids
        x, y = (Uuid(as_uuid=cast(Any, au)) for au in as_uuids)
        if as_uuid_x is as_uuid_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalUUIDAsUUIDError):
                _check_column_types_equal(x, y)

    @given(native_uuids=lists_fixed_length(booleans(), 2))
    def test_uuid_native_uuid(self, native_uuids: list[bool]) -> None:
        native_uuid_x, native_uuid_y = native_uuids
        x, y = (Uuid(native_uuid=nu) for nu in native_uuids)
        if native_uuid_x is native_uuid_y:
            _check_column_types_equal(x, y)
        else:
            with raises(UnequalUUIDNativeUUIDError):
                _check_column_types_equal(x, y)


class TestCheckEngine:
    @given(engine=sqlite_engines())
    def test_success(self, engine: Engine) -> None:
        check_engine(engine)

    @given(root=temp_paths())
    def test_engine_error(self, root: Path) -> None:
        engine = create_engine("sqlite", database=root.as_posix())
        with raises(EngineError):
            check_engine(engine)

    @given(engine=sqlite_engines())
    def test_num_tables(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_error(self, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_rel_tol_correct(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        check_engine(engine, num_tables=2, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_num_tables_rel_tol_error(self, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=1, rel_tol=0.5)

    @given(engine=sqlite_engines())
    def test_num_tables_abs_tol_correct(self, engine: Engine) -> None:
        check_engine(engine, num_tables=1, abs_tol=1)

    @given(engine=sqlite_engines())
    def test_num_tables_abs_tol_error(self, engine: Engine) -> None:
        with raises(IncorrectNumberOfTablesError):
            check_engine(engine, num_tables=2, abs_tol=1)


class TestCheckTableAgainstReflection:
    @given(engine=sqlite_engines())
    def test_reflected(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
        )
        ensure_table_created(table, engine)
        check_table_against_reflection(table, engine)

    @given(engine=sqlite_engines())
    def test_no_such_table(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
        )
        with raises(NoSuchTableError):
            _ = check_table_against_reflection(table, engine)


class TestCheckTablesEqual:
    def test_equal(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        check_tables_equal(table, table)

    def test_column_collections(self) -> None:
        x = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        y = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("value", Integer),
        )
        with raises(UnequalNumberOfColumnsError):
            check_tables_equal(x, y)

    def test_snake_table(self) -> None:
        x = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        y = Table(
            "Example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        check_tables_equal(x, y, snake_table=True)

    def test_snake_columns(self) -> None:
        x = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        y = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
        )
        check_tables_equal(x, y, snake_columns=True)

    def test_orm(self) -> None:
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
        self,
        x: str,
        y: str,
        snake: bool,
        expected: Optional[type[Exception]],
    ) -> None:
        if expected is None:
            _check_table_or_column_names_equal(x, y, snake=snake)
        else:
            with raises(expected):
                _check_table_or_column_names_equal(x, y, snake=snake)

    @mark.parametrize(
        ("name", "expected"),
        [param(None, "Id"), param("x", "x")],
    )
    def test_orm(self, name: Optional[str], expected: str) -> None:
        class Example(declarative_base()):
            __tablename__ = "example"

            Id = Column(
                Integer,
                primary_key=True,
                **({} if name is None else {"name": name}),
            )

        _check_table_or_column_names_equal(Example.Id.name, expected)


class TestColumnwiseMinMax:
    @given(
        values=lists(
            fixed_dictionaries(
                {
                    "x": integers(0, 100) | none(),
                    "y": integers(0, 100) | none(),
                },
            ),
            min_size=1,
            max_size=10,
        ),
        engine=sqlite_engines(),
    )
    def test_main(
        self,
        values: list[dict[str, Optional[int]]],
        engine: Engine,
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
            Column("y", Integer),
        )
        ensure_table_created(table, engine)
        with engine.begin() as conn:
            _ = conn.execute(table.insert(), values)
            res = conn.execute(table.select()).all()

        sel = select(
            table.c.x,
            table.c.y,
            columnwise_min(table.c.x, table.c.y).label("min_xy"),
            columnwise_max(table.c.x, table.c.y).label("max_xy"),
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
    def test_label(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True, autoincrement=True),
            Column("x", Integer),
        )
        ensure_table_created(table, engine)

        sel = select(columnwise_min(table.c.x, table.c.x))
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestCreateEngine:
    @given(temp_path=temp_paths())
    def test_main(self, temp_path: Path) -> None:
        engine = create_engine("sqlite", database=temp_path.name)
        assert isinstance(engine, Engine)


class TestEnsureTableCreated:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self,
        table_or_model: Any,
        engine: Engine,
        runs: int,
        /,
    ) -> None:
        sel = get_table(table_or_model).select()
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)
        for _ in range(runs):
            ensure_table_created(table_or_model, engine)
        with engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestEnsureTableDropped:
    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_core(self, engine: Engine, runs: int) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table, engine, runs)

    @given(engine=sqlite_engines())
    @mark.parametrize("runs", [param(1), param(2)])
    def test_orm(self, engine: Engine, runs: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self,
        table_or_model: Any,
        engine: Engine,
        runs: int,
        /,
    ) -> None:
        table = get_table(table_or_model)
        sel = table.select()
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(sel).all()
        for _ in range(runs):
            ensure_table_dropped(table_or_model, engine)
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(sel).all()
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestGetColumnNames:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        assert get_column_names(table_or_model) == ["id_"]


class TestGetColumns:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        self._run_test(table)

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_model: Any, /) -> None:
        columns = get_columns(table_or_model)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @given(engine=sqlite_engines())
    def test_sqlite(self, engine: Engine) -> None:
        assert get_dialect(engine) == "sqlite"

    @mark.parametrize(
        ("url", "expected"),
        [
            param("mssql+pyodbc://scott:tiger@mydsn", "mssql"),
            param("mysql://scott:tiger@localhost/foo", "mysql"),
            param("oracle://scott:tiger@127.0.0.1:1521/sidname", "oracle"),
            param(
                "postgresql://scott:tiger@localhost/mydatabase",
                "postgresql",
            ),
        ],
    )
    def test_non_sqlite(self, url: str, expected: str) -> None:
        assert get_dialect(_create_engine(url)) == expected


class TestGetTable:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        result = get_table(table)
        assert result is table

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__


class TestGetTableName:
    def test_core(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
        )
        result = get_table_name(table)
        expected = "example"
        assert result == expected

    def test_orm(self) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

        result = get_table_name(Example)
        expected = "example"
        assert result == expected


class TestModelToDict:
    @given(id_=integers())
    def test_main(self, id_: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            id_ = Column(Integer, primary_key=True)

        example = Example(id_=id_)
        assert model_to_dict(example) == {"id_": id_}

    @given(id_=integers())
    def test_explicitly_named_column(self, id_: int) -> None:
        class Example(cast(Any, declarative_base())):
            __tablename__ = "example"
            ID = Column(Integer, primary_key=True, name="id")

        example = Example(ID=id_)
        assert model_to_dict(example) == {"id": id_}


class TestRedirectToNoSuchTableError:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        with raises(NoSuchTableError), engine.begin() as conn:
            try:
                _ = conn.execute(select(table))
            except DatabaseError as error:
                redirect_to_no_such_table_error(engine, error)


class TestRedirectTableAlreadyExistsError:
    @given(engine=sqlite_engines())
    def test_main(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
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
            ],
        ),
    )
    def test_reflected(self, engine: Engine, col_type: Any) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("Id", col_type, primary_key=True),
        )
        ensure_table_created(table, engine)
        reflected = _reflect_table(table, engine)
        check_tables_equal(reflected, table)

    @given(engine=sqlite_engines())
    def test_no_such_table(self, engine: Engine) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("Id", Integer, primary_key=True),
        )
        with raises(NoSuchTableError):
            _ = _reflect_table(table, engine)


class TestYieldConnection:
    @given(engine=sqlite_engines())
    def test_engine(self, engine: Engine) -> None:
        with yield_connection(engine) as conn:
            assert isinstance(conn, Connection)

    @given(engine=sqlite_engines())
    def test_connection(self, engine: Engine) -> None:
        with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, Connection)


class TestYieldInClauseRows:
    @given(
        data=data(),
        engine=sqlite_engines(),
        chunk_size=integers(1, 10) | none(),
    )
    def test_main(
        self,
        data: DataObject,
        engine: Engine,
        chunk_size: Optional[int],
    ) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id", Integer, primary_key=True),
        )
        rows = data.draw(table_records_lists(table, min_size=1))
        num_rows = len(rows)
        with engine.begin() as conn:
            table.create(conn)
            _ = conn.execute(insert(table).values(rows))
            assert (
                conn.execute(select(func.count()).select_from(table)).scalar()
                == num_rows
            )
        row_vals = [row[0] for row in rows]
        values = data.draw(sets(sampled_from(row_vals)))
        result = list(
            yield_in_clause_rows(
                select(table.c.id),
                table.c.id,
                values,
                engine,
                chunk_size=chunk_size,
            ),
        )
        assert len(result) == len(values)
