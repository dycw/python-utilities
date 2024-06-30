from __future__ import annotations

import enum
import typing
from enum import auto
from re import escape
from time import sleep
from typing import Any, Literal, cast

import sqlalchemy
from hypothesis import Phase, assume, given, settings
from hypothesis.errors import InvalidArgument
from hypothesis.strategies import (
    DataObject,
    booleans,
    data,
    floats,
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
    Insert,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    Numeric,
    Row,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    Unicode,
    UnicodeText,
    Uuid,
    func,
    select,
)
from sqlalchemy.exc import DatabaseError, NoSuchTableError
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from tests.conftest import SKIPIF_CI
from utilities.hypothesis import (
    assume_does_not_raise,
    lists_fixed_length,
    sqlite_engines,
    temp_paths,
    text_ascii,
)
from utilities.iterables import OneEmptyError, one
from utilities.sqlalchemy import (
    CheckEngineError,
    Dialect,
    GetTableError,
    ParseEngineError,
    PostgresUpsertError,
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
    _insert_items_collect,
    _insert_items_collect_iterable,
    _insert_items_collect_valid,
    _InsertionItem,
    _InsertItemsCollectError,
    _InsertItemsCollectIterableError,
    check_engine,
    check_table_against_reflection,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_engine,
    ensure_tables_created,
    ensure_tables_dropped,
    get_chunk_size,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    get_table_does_not_exist_message,
    get_table_name,
    get_table_updated_column,
    insert_items,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    parse_engine,
    postgres_upsert,
    reflect_table,
    serialize_engine,
)
from utilities.types import get_class_name

if typing.TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path


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


class TestCheckEngine:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        check_engine(engine)

    @given(engine=sqlite_engines())
    def test_num_tables_pass(self, *, engine: Engine) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        ensure_tables_created(engine, table)
        check_engine(engine, num_tables=1)

    @given(engine=sqlite_engines())
    def test_num_tables_error(self, *, engine: Engine) -> None:
        with raises(CheckEngineError):
            check_engine(engine, num_tables=1)


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
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_ = Column(Integer, primary_key=True)

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

    def test_id(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        _check_table_or_column_names_equal(Example.id_.name, "id_")

    def test_id_as_x(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(
                Integer, kw_only=True, primary_key=True, name="name"
            )

        _check_table_or_column_names_equal(Example.id_.name, "name")


class TestColumnwiseMinMax:
    @given(
        engine=sqlite_engines(),
        values=sets(
            tuples(integers(0, 10) | none(), integers(0, 10) | none()), min_size=1
        ),
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
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_mapped_class: Table | type[Any], engine: Engine, runs: int, /
    ) -> None:
        for _ in range(runs):
            ensure_tables_created(engine, table_or_mapped_class)
        sel = get_table(table_or_mapped_class).select()
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
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example, engine, runs)

    def _run_test(
        self, table_or_mapped_class: Table | type[Any], engine: Engine, runs: int, /
    ) -> None:
        table = get_table(table_or_mapped_class)
        with engine.begin() as conn:
            table.create(conn)
        for _ in range(runs):
            ensure_tables_dropped(engine, table_or_mapped_class)
        sel = table.select()
        with raises(DatabaseError), engine.begin() as conn:
            _ = conn.execute(sel).all()


class TestGetChunkSize:
    @given(
        engine=sqlite_engines(),
        chunk_size_frac=floats(0.0, 1.0),
        scaling=floats(0.1, 10.0),
    )
    def test_main(
        self, *, engine: Engine, chunk_size_frac: float, scaling: float
    ) -> None:
        result = get_chunk_size(
            engine, chunk_size_frac=chunk_size_frac, scaling=scaling
        )
        assert result >= 1


class TestGetColumnNames:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        assert get_column_names(table_or_mapped_class) == ["id_"]


class TestGetColumns:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        self._run_test(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(Example)

    def _run_test(self, table_or_mapped_class: Table | type[Any], /) -> None:
        columns = get_columns(table_or_mapped_class)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        assert get_dialect(engine) is Dialect.sqlite


class TestGetTable:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table(table)
        assert result is table

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        table = get_table(Example)
        result = get_table(table)
        assert result is Example.__table__

    def test_error(self) -> None:
        with raises(
            GetTableError, match="Object .* must be a Table or mapped class; got .*"
        ):
            _ = get_table(type(None))


class TestGetTableDoesNotExistMessage:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        assert isinstance(get_table_does_not_exist_message(engine), str)


class TestGetTableName:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        result = get_table_name(table)
        expected = "example"
        assert result == expected

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        result = get_table_name(Example)
        expected = "example"
        assert result == expected


class TestGetTableUpdatedColumn:
    def test_main(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("created_at", DateTime(timezone=True), server_default=func.now()),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
            ),
        )
        assert get_table_updated_column(table) == "updated_at"

    def test_none(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("created_at", DateTime(timezone=True), server_default=func.now()),
        )
        assert get_table_updated_column(table) is None


class TestInsertItems:
    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_tuple_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ((id_,), self._table))

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_pair_of_dict_and_table(self, *, engine: Engine, id_: int) -> None:
        self._run_test(engine, {id_}, ({"id_": id_}, self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_pair_of_lists_of_tuples_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([((id_,)) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_pair_of_lists_of_dicts_and_table(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, ([({"id_": id_}) for id_ in ids], self._table))

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_list_of_pairs_of_tuples_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [(((id_,), self._table)) for id_ in ids])

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    def test_list_of_pairs_of_dicts_and_tables(
        self, *, engine: Engine, ids: set[int]
    ) -> None:
        self._run_test(engine, ids, [({"id_": id_}, self._table) for id_ in ids])

    @given(
        engine=sqlite_engines(), ids=sets(integers(0, 1000), min_size=10, max_size=100)
    )
    def test_many_items(self, *, engine: Engine, ids: set[int]) -> None:
        self._run_test(engine, ids, [({"id_": id_}, self._table) for id_ in ids])

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    def test_mapped_class(self, *, engine: Engine, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        self._run_test(engine, {id_}, Example(id_=id_))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    def _run_test(self, engine: Engine, ids: set[int], /, *args: Any) -> None:
        ensure_tables_created(engine, self._table)
        insert_items(engine, *args)
        sel = select(self._table.c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


class TestInsertItemsCollect:
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
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        item = Example(id_=id_)
        result = list(_insert_items_collect(item))
        expected = [_InsertionItem(values={"id_": id_}, table=get_table(Example))]
        assert result == expected

    @mark.parametrize(
        "item",
        [
            param((None,), id="tuple length"),
            param((None, None), id="second argument not a table or mapped class"),
            param(None, id="outright invalid"),
        ],
    )
    def test_errors(self, *, item: Any) -> None:
        with raises(_InsertItemsCollectError):
            _ = list(_insert_items_collect(item))

    def test_error_tuple_but_first_argument_invalid(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(_InsertItemsCollectError):
            _ = list(_insert_items_collect((None, table)))


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

    def test_error(self) -> None:
        table = Table("example", MetaData(), Column("id", Integer, primary_key=True))
        with raises(_InsertItemsCollectIterableError):
            _ = list(_insert_items_collect_iterable([None], table))


class TestInsertItemsCollectValid:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((1, 2, 3), True),
            param({"a": 1, "b": 2, "c": 3}, True),
            param({1: "a", 2: "b", 3: "c"}, False),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = _insert_items_collect_valid(obj)
        assert result is expected


class TestIsMappedClass:
    def test_mapped_class_instance(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_mapped_class(Example)
        assert is_mapped_class(Example(id_=1))

    def test_other(self) -> None:
        assert not is_mapped_class(None)


class TestIsTableOrMappedClass:
    def test_table(self) -> None:
        table = Table("example", MetaData(), Column("id_", Integer, primary_key=True))
        assert is_table_or_mapped_class(table)

    def test_mapped_class(self) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert is_table_or_mapped_class(Example)
        assert is_table_or_mapped_class(Example(id_=1))

    def test_other(self) -> None:
        assert not is_table_or_mapped_class(None)


class TestMappedClassToDict:
    @given(id_=integers())
    def test_main(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        example = Example(id_=id_)
        result = mapped_class_to_dict(example)
        expected = {"id_": id_}
        assert result == expected

    @given(id_=integers())
    def test_explicitly_named_column(self, *, id_: int) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            ID: Mapped[int] = mapped_column(
                Integer, kw_only=True, primary_key=True, name="id"
            )

        example = Example(ID=id_)
        result = mapped_class_to_dict(example)
        expected = {"id": id_}
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


@SKIPIF_CI
class TestPostgresEngine:
    @given(ids=sets(integers(0, 10)))
    @settings(max_examples=1, phases={Phase.generate})
    def test_main(
        self, *, create_postgres_engine: Callable[..., Engine], ids: set[int]
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresEngine)}",
            metadata,
            Column("id_", Integer, primary_key=True),
        )
        engine = create_postgres_engine(table)
        insert_items(engine, ([(id_,) for id_ in ids], table))
        sel = select(table.c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


@SKIPIF_CI
class TestPostgresUpsert:
    @given(id_=integers(0, 10), old=booleans(), new=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    def test_mapping(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        id_: int,
        old: bool,
        new: bool,
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_mapping.__name__}",
            metadata,
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        engine = create_postgres_engine(table)
        ups = postgres_upsert(table, values={"id_": id_, "value": old})
        assert one(self._run_upsert(engine, table, ups)) == (id_, old)
        ups = postgres_upsert(table, values={"id_": id_, "value": new})
        assert one(self._run_upsert(engine, table, ups)) == (id_, new)

    @given(data=data(), ids=sets(integers(0, 10)))
    @settings(max_examples=1, phases={Phase.generate})
    def test_mappings(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        data: DataObject,
        ids: set[int],
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_mappings.__name__}",
            metadata,
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )
        engine = create_postgres_engine(table)
        with assume_does_not_raise(InvalidArgument):
            id_ins = data.draw(sets(sampled_from(sorted(ids))))
        old = data.draw(lists_fixed_length(booleans(), len(id_ins)))
        values = [
            {"id_": id_, "value": v} for (id_, v) in zip(id_ins, old, strict=True)
        ]
        with assume_does_not_raise(OneEmptyError):
            ups = postgres_upsert(table, values=values)
        assert self._run_upsert(engine, table, ups) == [
            tuple(v.values()) for v in values
        ]
        new = data.draw(lists_fixed_length(booleans(), len(ids)))
        rows = list(zip(sorted(ids), new, strict=True))
        ups = postgres_upsert(
            table, values=[{"id_": id_, "value": v} for id_, v in rows]
        )
        assert self._run_upsert(engine, table, ups) == rows

    @given(id_=integers(0, 10), old=booleans(), new=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    def test_class_and_mapping(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        id_: int,
        old: bool,
        new: bool,
    ) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_class_and_mapping.__name__}"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        engine = create_postgres_engine(Example)
        ups = postgres_upsert(Example, values={"id_": id_, "value": old})
        assert one(self._run_upsert(engine, Example, ups)) == (id_, old)
        ups = postgres_upsert(Example, values={"id_": id_, "value": new})
        assert one(self._run_upsert(engine, Example, ups)) == (id_, new)

    @given(id_=integers(0, 10), old=booleans(), new=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    def test_class(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        id_: int,
        old: bool,
        new: bool,
    ) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_class.__name__}"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        engine = create_postgres_engine(Example)
        ups = postgres_upsert(Example(id_=id_, value=old))
        assert one(self._run_upsert(engine, Example, ups)) == (id_, old)
        ups = postgres_upsert(Example(id_=id_, value=new))
        assert one(self._run_upsert(engine, Example, ups)) == (id_, new)

    @given(data=data(), ids=sets(integers(0, 10)))
    @settings(max_examples=1, phases={Phase.generate})
    def test_classes(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        data: DataObject,
        ids: set[int],
    ) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_classes.__name__}"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        engine = create_postgres_engine(Example)
        with assume_does_not_raise(InvalidArgument):
            id_ins = data.draw(sets(sampled_from(sorted(ids))))
        old = data.draw(lists_fixed_length(booleans(), len(id_ins)))
        values = [
            Example(id_=id_, value=v) for (id_, v) in zip(id_ins, old, strict=True)
        ]
        with assume_does_not_raise(OneEmptyError):
            ups = postgres_upsert(values)
        assert self._run_upsert(engine, Example, ups) == [
            (v.id_, v.value) for v in values
        ]
        new = data.draw(lists_fixed_length(booleans(), len(ids)))
        rows = list(zip(sorted(ids), new, strict=True))
        ups = postgres_upsert([Example(id_=id_, value=v) for id_, v in rows])
        assert self._run_upsert(engine, Example, ups) == rows

    @given(id_=integers(0, 10), x_old=booleans(), x_new=booleans(), y=booleans())
    @mark.parametrize("selected_or_all", [param("selected"), param("all")])
    @settings(max_examples=1, phases={Phase.generate})
    def test_table_sel_or_all(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_old: bool,
        x_new: bool,
        y: bool,
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_table_sel_or_all.__name__}_{selected_or_all}",
            metadata,
            Column("id_", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
            Column("y", Boolean, nullable=True),
        )
        engine = create_postgres_engine(table)
        ups = postgres_upsert(table, values={"id_": id_, "x": x_old, "y": y})
        assert one(self._run_upsert(engine, table, ups)) == (id_, x_old, y)
        ups = postgres_upsert(
            table, values={"id_": id_, "x": x_new}, selected_or_all=selected_or_all
        )
        if selected_or_all == "selected":
            expected = (id_, x_new, y)
        else:
            expected = (id_, x_new, None)
        assert one(self._run_upsert(engine, table, ups)) == expected

    @given(id_=integers(0, 10), x_old=booleans(), x_new=booleans(), y=booleans())
    @mark.parametrize("selected_or_all", [param("selected"), param("all")])
    @settings(max_examples=1, phases={Phase.generate})
    def test_class_sel_or_all(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_old: bool,
        x_new: bool,
        y: bool,
    ) -> None:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_class_sel_or_all.__name__}_{selected_or_all}"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            x: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)
            y: Mapped[bool | None] = mapped_column(
                Boolean, default=None, kw_only=True, nullable=True
            )

        engine = create_postgres_engine(Example)
        ups = postgres_upsert(Example(id_=id_, x=x_old, y=y))
        assert one(self._run_upsert(engine, Example, ups)) == (id_, x_old, y)
        ups = postgres_upsert(
            Example(id_=id_, x=x_new), selected_or_all=selected_or_all
        )
        if selected_or_all == "selected":
            expected = (id_, x_new, y)
        else:
            expected = (id_, x_new, None)
        assert one(self._run_upsert(engine, Example, ups)) == expected

    @given(id_=integers(0, 10), old=booleans(), new=booleans())
    @settings(max_examples=1, phases={Phase.generate})
    def test_mapping_updated(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        id_: int,
        old: bool,
        new: bool,
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_mapping_updated.__name__}",
            metadata,
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
            ),
        )
        engine = create_postgres_engine(table)
        ups = postgres_upsert(table, values={"id_": id_, "value": old})
        _, _, update1 = one(self._run_upsert(engine, table, ups))
        sleep(0.1)
        ups = postgres_upsert(table, values={"id_": id_, "value": new})
        _, _, update2 = one(self._run_upsert(engine, table, ups))
        assert update1 < update2

    @given(data=data(), ids=sets(integers(0, 10)))
    @settings(max_examples=1, phases={Phase.generate})
    def test_mappings_updated(
        self,
        *,
        create_postgres_engine: Callable[..., Engine],
        data: DataObject,
        ids: set[int],
    ) -> None:
        metadata = MetaData()
        table = Table(
            f"test_{get_class_name(TestPostgresUpsert)}_{TestPostgresUpsert.test_mappings_updated.__name__}",
            metadata,
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
            ),
        )
        engine = create_postgres_engine(table)
        old = data.draw(lists_fixed_length(booleans(), len(ids)))
        old_values = [
            {"id_": id_, "value": v} for (id_, v) in zip(ids, old, strict=True)
        ]
        with assume_does_not_raise(OneEmptyError):
            ups = postgres_upsert(table, values=old_values)
        _, _, update1 = one(self._run_upsert(engine, table, ups))
        sleep(0.1)
        new = data.draw(lists_fixed_length(booleans(), len(ids)))
        new_values = [
            {"id_": id_, "value": v} for (id_, v) in zip(ids, new, strict=True)
        ]
        ups = postgres_upsert(table, values=new_values)
        _, _, update2 = one(self._run_upsert(engine, table, ups))
        assert update1 < update2

    def test_error(self) -> None:
        with raises(
            PostgresUpsertError,
            match=escape("Unsupported item and values; got None and []"),
        ):
            _ = postgres_upsert(cast(Any, None), values=[])

    def _run_upsert(
        self, engine: Engine, table: Any, ups: Insert
    ) -> Sequence[Row[Any]]:
        with engine.begin() as conn:
            _ = conn.execute(ups)
        with engine.begin() as conn:
            return conn.execute(select(table)).all()


class TestRedirectToNoSuchSequenceError:
    @given(engine=sqlite_engines())
    def test_main(self, *, engine: Engine) -> None:
        seq = sqlalchemy.Sequence("example")
        with raises(NotImplementedError), engine.begin() as conn:
            _ = conn.scalar(seq)


class TestReflectTable:
    @given(
        engine=sqlite_engines(),
        col_type=sampled_from([
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
        ]),
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
        class Base(DeclarativeBase, MappedAsDataclass, TablenameMixin): ...

        class Example(Base):
            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        assert get_table_name(Example) == "example"
