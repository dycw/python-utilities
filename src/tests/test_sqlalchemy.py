from __future__ import annotations

import asyncio
import enum
import time
from enum import auto
from typing import TYPE_CHECKING, Any, Literal, cast, overload

import sqlalchemy
from hypothesis import assume, given
from hypothesis.strategies import (
    DataObject,
    SearchStrategy,
    booleans,
    data,
    floats,
    integers,
    lists,
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
    Row,
    Select,
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
from sqlalchemy.exc import (
    DatabaseError,
    NoSuchTableError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

from tests.conftest import SKIPIF_CI
from utilities.hashlib import md5_hash
from utilities.hypothesis import (
    aiosqlite_engines,
    int32s,
    lists_fixed_length,
    sets_fixed_length,
    sqlite_engines,
    temp_paths,
    text_ascii,
)
from utilities.iterables import one
from utilities.modules import is_installed
from utilities.sqlalchemy import (
    AsyncEngineOrConnection,
    CheckEngineError,
    Dialect,
    EngineOrConnection,
    GetTableError,
    InsertItemsAsyncError,
    InsertItemsError,
    ParseEngineError,
    TablenameMixin,
    TableOrMappedClass,
    UpsertItemsAsyncError,
    UpsertItemsError,
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
    _insert_items_prepare,
    _InsertItem,
    _InsertItemsPrepareError,
    _normalize_insert_item,
    _normalize_upsert_item,
    _NormalizedInsertItem,
    _NormalizedUpsertItem,
    _NormalizeInsertItemError,
    _NormalizeUpsertItemError,
    _upsert_items_prepare,
    _UpsertItem,
    _UpsertItemsPrepareError,
    check_engine,
    check_table_against_reflection,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_engine,
    ensure_tables_created,
    ensure_tables_created_async,
    ensure_tables_dropped,
    ensure_tables_dropped_async,
    get_chunk_size,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    get_table_name,
    get_table_updated_column,
    insert_items,
    insert_items_async,
    is_insert_item_pair,
    is_mapped_class,
    is_table_or_mapped_class,
    mapped_class_to_dict,
    parse_engine,
    reflect_table,
    serialize_engine,
    upsert_items,
    upsert_items_async,
    yield_connection,
    yield_connection_async,
    yield_primary_key_columns,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from utilities.asyncio import Coroutine1


@overload
def _upsert_triples(
    *, nullable: Literal[True]
) -> SearchStrategy[tuple[int, bool, bool]]: ...
@overload
def _upsert_triples(
    *, nullable: bool = ...
) -> SearchStrategy[tuple[int, bool, bool | None]]: ...
def _upsert_triples(
    *, nullable: bool = False
) -> SearchStrategy[tuple[int, bool, bool | None]]:
    elements = booleans()
    if nullable:
        elements |= none()
    return tuples(integers(0, 10), booleans(), elements)


def _upsert_lists(
    *, nullable: bool = False, min_size: int = 0, max_size: int | None = None
) -> SearchStrategy[list[tuple[int, bool, bool | None]]]:
    return lists(
        _upsert_triples(nullable=nullable),
        min_size=min_size,
        max_size=max_size,
        unique_by=lambda x: x[0],
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
    def test_create_constraint(self, *, create_constraints: Sequence[bool]) -> None:
        create_constraint_x, create_constraint_y = create_constraints
        x, y = (Boolean(create_constraint=cs) for cs in create_constraints)
        if create_constraint_x is create_constraint_y:
            _check_column_types_boolean_equal(x, y)
        else:
            with raises(_CheckColumnTypesBooleanEqualError):
                _check_column_types_boolean_equal(x, y)

    @given(names=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_name(self, *, names: Sequence[str | None]) -> None:
        name_x, name_y = names
        x, y = (Boolean(name=n) for n in names)
        if name_x == name_y:
            _check_column_types_boolean_equal(x, y)
        else:
            with raises(_CheckColumnTypesBooleanEqualError):
                _check_column_types_boolean_equal(x, y)


class TestCheckColumnTypesDateTimeEqual:
    @given(timezones=lists_fixed_length(booleans(), 2))
    def test_main(self, *, timezones: Sequence[bool]) -> None:
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
    def test_create_constraint(self, *, create_constraints: Sequence[bool]) -> None:
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
    def test_native_enum(self, *, native_enums: Sequence[bool]) -> None:
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
    def test_length(self, *, lengths: Sequence[int]) -> None:
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
    def test_inherit_schema(self, *, inherit_schemas: Sequence[bool]) -> None:
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
    def test_precision(self, *, precisions: Sequence[int | None]) -> None:
        precision_x, precision_y = precisions
        x, y = (Float(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_float_equal(x, y)
        else:
            with raises(_CheckColumnTypesFloatEqualError):
                _check_column_types_float_equal(x, y)

    @given(asdecimals=lists_fixed_length(booleans(), 2))
    def test_asdecimal(self, *, asdecimals: Sequence[bool]) -> None:
        asdecimal_x, asdecimal_y = asdecimals
        x, y = (Float(asdecimal=cast(Any, a)) for a in asdecimals)
        if asdecimal_x is asdecimal_y:
            _check_column_types_float_equal(x, y)
        else:
            with raises(_CheckColumnTypesFloatEqualError):
                _check_column_types_float_equal(x, y)

    @given(dec_ret_scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_decimal_return_scale(
        self, *, dec_ret_scales: Sequence[int | None]
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
    def test_native(self, *, natives: Sequence[bool]) -> None:
        native_x, native_y = natives
        x, y = (Interval(native=n) for n in natives)
        if native_x is native_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)

    @given(second_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_second_precision(self, *, second_precisions: Sequence[int | None]) -> None:
        second_precision_x, second_precision_y = second_precisions
        x, y = (Interval(second_precision=sp) for sp in second_precisions)
        if second_precision_x == second_precision_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)

    @given(day_precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_day_precision(self, *, day_precisions: Sequence[int | None]) -> None:
        day_precision_x, day_precision_y = day_precisions
        x, y = (Interval(day_precision=dp) for dp in day_precisions)
        if day_precision_x == day_precision_y:
            _check_column_types_interval_equal(x, y)
        else:
            with raises(_CheckColumnTypesIntervalEqualError):
                _check_column_types_interval_equal(x, y)


class TestCheckColumnTypesLargeBinaryEqual:
    @given(lengths=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_main(self, *, lengths: Sequence[int | None]) -> None:
        length_x, length_y = lengths
        x, y = (LargeBinary(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_large_binary_equal(x, y)
        else:
            with raises(_CheckColumnTypesLargeBinaryEqualError):
                _check_column_types_large_binary_equal(x, y)


class TestCheckColumnTypesNumericEqual:
    @given(precisions=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_precision(self, *, precisions: Sequence[int | None]) -> None:
        precision_x, precision_y = precisions
        x, y = (Numeric(precision=p) for p in precisions)
        if precision_x == precision_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(asdecimals=lists_fixed_length(booleans(), 2))
    def test_asdecimal(self, *, asdecimals: Sequence[bool]) -> None:
        asdecimal_x, asdecimal_y = asdecimals
        x, y = (Numeric(asdecimal=cast(Any, a)) for a in asdecimals)
        if asdecimal_x is asdecimal_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_numeric_scale(self, *, scales: Sequence[int | None]) -> None:
        scale_x, scale_y = scales
        x, y = (Numeric(scale=s) for s in scales)
        if scale_x == scale_y:
            _check_column_types_numeric_equal(x, y)
        else:
            with raises(_CheckColumnTypesNumericEqualError):
                _check_column_types_numeric_equal(x, y)

    @given(dec_ret_scales=lists_fixed_length(integers(0, 10) | none(), 2))
    def test_decimal_return_scale(
        self, *, dec_ret_scales: Sequence[int | None]
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
        lengths: Sequence[int | None],
    ) -> None:
        length_x, length_y = lengths
        x, y = (cls(length=l_) for l_ in lengths)
        if length_x == length_y:
            _check_column_types_string_equal(x, y)
        else:
            with raises(_CheckColumnTypesStringEqualError):
                _check_column_types_string_equal(x, y)

    @given(collations=lists_fixed_length(text_ascii(min_size=1) | none(), 2))
    def test_collation(self, *, collations: Sequence[str | None]) -> None:
        collation_x, collation_y = collations
        x, y = (String(collation=c) for c in collations)
        if collation_x == collation_y:
            _check_column_types_string_equal(x, y)
        else:
            with raises(_CheckColumnTypesStringEqualError):
                _check_column_types_string_equal(x, y)


class TestCheckColumnTypesUuidEqual:
    @given(as_uuids=lists_fixed_length(booleans(), 2))
    def test_as_uuid(self, *, as_uuids: Sequence[bool]) -> None:
        as_uuid_x, as_uuid_y = as_uuids
        x, y = (Uuid(as_uuid=cast(Any, au)) for au in as_uuids)
        if as_uuid_x is as_uuid_y:
            _check_column_types_uuid_equal(x, y)
        else:
            with raises(_CheckColumnTypesUuidEqualError):
                _check_column_types_uuid_equal(x, y)

    @given(native_uuids=lists_fixed_length(booleans(), 2))
    def test_native_uuid(self, *, native_uuids: Sequence[bool]) -> None:
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

    @SKIPIF_CI
    def test_postgres(self, *, create_postgres_engine: Callable[..., Engine]) -> None:
        engine = create_postgres_engine()
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
    def test_sync(self, *, temp_path: Path) -> None:
        engine = create_engine("sqlite", database=temp_path.name)
        assert isinstance(engine, Engine)

    @given(temp_path=temp_paths())
    def test_async(self, *, temp_path: Path) -> None:
        engine = create_engine("sqlite+aiosqlite", database=temp_path.name, async_=True)
        assert isinstance(engine, AsyncEngine)

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
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_table(self, *, engine: Engine, use_conn: bool) -> None:
        self._run_test_sync(engine, self._table, use_conn=use_conn)

    @given(engine=sqlite_engines())
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_mapped_class(self, *, engine: Engine, use_conn: bool) -> None:
        self._run_test_sync(engine, self._mapped_class, use_conn=use_conn)

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_table(self, *, data: DataObject, use_conn: bool) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(engine, self._table, use_conn=use_conn)

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_mapped_class(
        self, *, data: DataObject, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(engine, self._mapped_class, use_conn=use_conn)

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example

    def _run_test_sync(
        self,
        engine_or_conn: EngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            with yield_connection(engine_or_conn) as conn:
                self._run_test_sync(conn, table_or_mapped_class)
            return
        for _ in range(2):
            ensure_tables_created(engine_or_conn, table_or_mapped_class)
        sel = self._get_select(table_or_mapped_class)
        with yield_connection(engine_or_conn) as conn:
            _ = conn.execute(sel).all()

    async def _run_test_async(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            async with yield_connection_async(engine_or_conn) as conn:
                await self._run_test_async(conn, table_or_mapped_class)
            return
        for _ in range(2):
            await ensure_tables_created_async(engine_or_conn, table_or_mapped_class)
        sel = self._get_select(table_or_mapped_class)
        async with yield_connection_async(engine_or_conn) as conn:
            _ = (await conn.execute(sel)).all()

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class))


class TestEnsureTablesDropped:
    @given(engine=sqlite_engines())
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_table(self, *, engine: Engine, use_conn: bool) -> None:
        self._run_test_sync(engine, self._table, use_conn=use_conn)

    @given(engine=sqlite_engines())
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_mapped_class(self, *, engine: Engine, use_conn: bool) -> None:
        self._run_test_sync(engine, self._mapped_class, use_conn=use_conn)

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_table(self, *, data: DataObject, use_conn: bool) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(engine, self._table, use_conn=use_conn)

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_mapped_class(
        self, *, data: DataObject, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(engine, self._mapped_class, use_conn=use_conn)

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example

    def _run_test_sync(
        self,
        engine_or_conn: EngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            with yield_connection(engine_or_conn) as conn:
                self._run_test_sync(conn, table_or_mapped_class)
            return
        for _ in range(2):
            ensure_tables_dropped(engine_or_conn, table_or_mapped_class)
        sel = self._get_select(table_or_mapped_class)
        with raises(DatabaseError), yield_connection(engine_or_conn) as conn:
            _ = conn.execute(sel).all()

    async def _run_test_async(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            async with yield_connection_async(engine_or_conn) as conn:
                await self._run_test_async(conn, table_or_mapped_class)
            return
        for _ in range(2):
            await ensure_tables_dropped_async(engine_or_conn, table_or_mapped_class)
        sel = self._get_select(table_or_mapped_class)
        with raises(DatabaseError):
            async with yield_connection_async(engine_or_conn) as conn:
                _ = await conn.execute(sel)

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class))


class TestGetChunkSize:
    @given(
        engine=sqlite_engines(),
        chunk_size_frac=floats(0.0, 1.0),
        scaling=floats(0.1, 10.0),
    )
    def test_sync(
        self, *, engine: Engine, chunk_size_frac: float, scaling: float
    ) -> None:
        result = get_chunk_size(
            engine, chunk_size_frac=chunk_size_frac, scaling=scaling
        )
        assert result >= 1

    @given(data=data(), chunk_size_frac=floats(0.0, 1.0), scaling=floats(0.1, 10.0))
    async def test_async(
        self, *, data: DataObject, chunk_size_frac: float, scaling: float
    ) -> None:
        engine = await aiosqlite_engines(data)
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

    def _run_test(self, table_or_mapped_class: TableOrMappedClass, /) -> None:
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

    def _run_test(self, table_or_mapped_class: TableOrMappedClass, /) -> None:
        columns = get_columns(table_or_mapped_class)
        assert isinstance(columns, list)
        assert len(columns) == 1
        assert isinstance(columns[0], Column)


class TestGetDialect:
    @mark.skipif(condition=not is_installed("pyodbc"), reason="'pyodbc' not installed")
    def test_mssql(self) -> None:
        engine = create_engine("mssql")
        assert get_dialect(engine) is Dialect.mssql

    @mark.skipif(
        condition=not is_installed("mysqldb"), reason="'mysqldb' not installed"
    )
    def test_mysql(self) -> None:
        engine = create_engine("mysql")
        assert get_dialect(engine) is Dialect.mysql

    @mark.skipif(
        condition=not is_installed("oracledb"), reason="'oracledb' not installed"
    )
    def test_oracle(self) -> None:
        engine = create_engine("oracle+oracledb")
        assert get_dialect(engine) is Dialect.oracle

    def test_postgres(self) -> None:
        engine = create_engine("postgresql")
        assert get_dialect(engine) is Dialect.postgresql

    @mark.skipif(
        condition=not is_installed("asyncpg"), reason="'asyncpg' not installed"
    )
    def test_postgres_async(self) -> None:
        engine = create_engine("postgresql+asyncpg")
        assert get_dialect(engine) is Dialect.postgresql

    @given(engine=sqlite_engines())
    def test_sqlite(self, *, engine: Engine) -> None:
        assert get_dialect(engine) is Dialect.sqlite

    @given(data=data())
    async def test_sqlite_async(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
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
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_pair_of_obj_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        engine: Engine,
        id_: int,
        use_conn: bool,
    ) -> None:
        match case:
            case "tuple":
                item = (id_,), self._table
            case "dict":
                item = {"id_": id_}, self._table
        self._run_test_sync(engine, {id_}, item, use_conn=use_conn)

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_pair_of_list_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        engine: Engine,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], self._table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], self._table
        self._run_test_sync(engine, ids, item, use_conn=use_conn)

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_list_of_pairs_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        engine: Engine,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        match case:
            case "tuple":
                item = [((id_,), self._table) for id_ in ids]
            case "dict":
                item = [({"id_": id_}, self._table) for id_ in ids]
        self._run_test_sync(engine, ids, item, use_conn=use_conn)

    @given(
        engine=sqlite_engines(), ids=sets(integers(0, 1000), min_size=10, max_size=100)
    )
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_many_items(
        self, *, engine: Engine, ids: set[int], use_conn: bool
    ) -> None:
        self._run_test_sync(
            engine, ids, [({"id_": id_}, self._table) for id_ in ids], use_conn=use_conn
        )

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_mapped_class(
        self, *, engine: Engine, id_: int, use_conn: bool
    ) -> None:
        self._run_test_sync(
            engine, {id_}, self._mapped_class(id_=id_), use_conn=use_conn
        )

    @given(engine=sqlite_engines(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_mapped_classes(
        self, *, engine: Engine, ids: set[int], use_conn: bool
    ) -> None:
        self._run_test_sync(
            engine, ids, [self._mapped_class(id_=id_) for id_ in ids], use_conn=use_conn
        )

    @given(engine=sqlite_engines(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_assume_table_exists(
        self, *, engine: Engine, id_: int, use_conn: bool
    ) -> None:
        self._run_test_sync(
            engine,
            {id_},
            self._mapped_class(id_=id_),
            assume_tables_exist=True,
            use_conn=use_conn,
        )

    @given(engine=sqlite_engines())
    @mark.parametrize("use_conn", [param(True), param(False)])
    def test_sync_error(self, *, engine: Engine, use_conn: bool) -> None:
        with raises(InsertItemsError, match="Item must be valid; got None"):
            self._run_test_sync(engine, set(), cast(Any, None), use_conn=use_conn)

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_pair_of_obj_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        id_: int,
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = (id_,), self._table
            case "dict":
                item = {"id_": id_}, self._table
        await self._run_test_async(engine, {id_}, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_pair_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], self._table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], self._table
        await self._run_test_async(engine, ids, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_list_of_pairs_of_objs_and_table(
        self,
        *,
        case: Literal["tuple", "dict"],
        data: DataObject,
        ids: set[int],
        use_conn: bool,
    ) -> None:
        engine = await aiosqlite_engines(data)
        match case:
            case "tuple":
                item = [((id_,), self._table) for id_ in ids]
            case "dict":
                item = [({"id_": id_}, self._table) for id_ in ids]
        await self._run_test_async(engine, ids, item, use_conn=use_conn)

    @given(data=data(), ids=sets(integers(0, 1000), min_size=10, max_size=100))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_many_items(
        self, *, data: DataObject, ids: set[int], use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(
            engine, ids, [({"id_": id_}, self._table) for id_ in ids], use_conn=use_conn
        )

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_mapped_class(
        self, *, data: DataObject, id_: int, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(
            engine, {id_}, self._mapped_class(id_=id_), use_conn=use_conn
        )

    @given(data=data(), ids=sets(integers(0, 10), min_size=1))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_mapped_classes(
        self, *, data: DataObject, ids: set[int], use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(
            engine, ids, [self._mapped_class(id_=id_) for id_ in ids], use_conn=use_conn
        )

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_assume_table_exists(
        self, *, data: DataObject, id_: int, use_conn: bool
    ) -> None:
        engine = await aiosqlite_engines(data)
        await self._run_test_async(
            engine,
            {id_},
            self._mapped_class(id_=id_),
            assume_tables_exist=True,
            use_conn=use_conn,
        )

    @given(data=data())
    @mark.parametrize("use_conn", [param(True), param(False)])
    async def test_async_error(self, *, data: DataObject, use_conn: bool) -> None:
        engine = await aiosqlite_engines(data)
        with raises(InsertItemsAsyncError, match="Item must be valid; got None"):
            await self._run_test_async(
                engine, set(), cast(Any, None), use_conn=use_conn
            )

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example

    def _run_test_sync(
        self,
        engine_or_conn: EngineOrConnection,
        ids: set[int],
        /,
        *items: _InsertItem,
        assume_tables_exist: bool = False,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            with yield_connection(engine_or_conn) as conn:
                self._run_test_sync(
                    conn, ids, *items, assume_tables_exist=assume_tables_exist
                )
            return
        if assume_tables_exist:
            with raises(OperationalError, match="no such table"):
                insert_items(
                    engine_or_conn, *items, assume_tables_exist=assume_tables_exist
                )
            return
        insert_items(engine_or_conn, *items, assume_tables_exist=assume_tables_exist)
        sel = self._get_select(self._table)
        with yield_connection(engine_or_conn) as conn:
            results = conn.execute(sel).scalars().all()
        self._assert_results(results, ids)

    async def _run_test_async(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        ids: set[int],
        /,
        *items: _InsertItem,
        assume_tables_exist: bool = False,
        use_conn: bool = False,
    ) -> None:
        if use_conn:
            async with yield_connection_async(engine_or_conn) as conn:
                await self._run_test_async(
                    conn, ids, *items, assume_tables_exist=assume_tables_exist
                )
            return
        if assume_tables_exist:
            with raises(OperationalError, match="no such table"):
                await insert_items_async(
                    engine_or_conn, *items, assume_tables_exist=assume_tables_exist
                )
            return
        await insert_items_async(
            engine_or_conn, *items, assume_tables_exist=assume_tables_exist
        )
        sel = self._get_select(self._table)
        async with yield_connection_async(engine_or_conn) as conn:
            results = (await conn.execute(sel)).scalars().all()
        self._assert_results(results, ids)

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class).c["id_"])

    def _assert_results(self, results: Sequence[Any], ids: set[int], /) -> None:
        assert set(results) == ids


class TestInsertItemPrepare:
    @given(engine=sqlite_engines())
    def test_error(self, *, engine: Engine) -> None:
        with raises(_InsertItemsPrepareError, match="Item must be valid; got None"):
            _ = _insert_items_prepare(engine, cast(Any, None))


class TestIsInsertItemPair:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((), False),
            param((1,), False),
            param((1, 2), False),
            param(((1, 2, 3), None), False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, None), False),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_insert_item_pair(obj)
        assert result is expected


class TestIsUpsertItemPair:
    @mark.parametrize(
        ("obj", "expected"),
        [
            param(None, False),
            param((), False),
            param((1,), False),
            param((1, 2), False),
            param(((1, 2, 3), None), False),
            param(((1, 2, 3), Table("example", MetaData())), True),
            param(({"a": 1, "b": 2, "c": 3}, None), False),
            param(({"a": 1, "b": 2, "c": 3}, Table("example", MetaData())), True),
        ],
    )
    def test_main(self, *, obj: Any, expected: bool) -> None:
        result = is_insert_item_pair(obj)
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


class TestNormalizeInsertItem:
    @given(id_=integers())
    @mark.parametrize("case", [param("tuple"), param("dict")])
    def test_pair_of_obj_and_table(
        self, *, case: Literal["tuple", "dict"], id_: int
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = (id_,), table
            case "dict":
                item = {"id": id_}, table
        result = one(_normalize_insert_item(item))
        expected = _NormalizedInsertItem(values=item[0], table=table)
        assert result == expected

    @given(ids=sets(integers()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    def test_pair_of_list_of_objs_and_table(
        self, *, case: Literal["tuple", "dict"], ids: set[int]
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = [((id_,)) for id_ in ids], table
            case "dict":
                item = [({"id_": id_}) for id_ in ids], table
        result = list(_normalize_insert_item(item))
        expected = [_NormalizedInsertItem(values=i, table=table) for i in item[0]]
        assert result == expected

    @given(ids=sets(integers()))
    @mark.parametrize("case", [param("tuple"), param("dict")])
    def test_list_of_pairs_of_objs_and_table(
        self, *, case: Literal["tuple", "dict"], ids: set[int]
    ) -> None:
        table = self._table
        match case:
            case "tuple":
                item = [(((id_,), table)) for id_ in ids]
            case "dict":
                item = [({"id_": id_}, table) for id_ in ids]
        result = list(_normalize_insert_item(item))
        expected = [_NormalizedInsertItem(values=i[0], table=table) for i in item]
        assert result == expected

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_insert_item(cls(id_=id_)))
        expected = _NormalizedInsertItem(values={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(integers(0, 10), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_insert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedInsertItem(values={"id_": id_}, table=get_table(cls))
            for id_ in ids
        ]
        assert result == expected

    @mark.parametrize(
        "item",
        [
            param((None,), id="tuple, not pair"),
            param(
                (None, Table("example", MetaData())), id="pair, first element invalid"
            ),
            param(((1, 2, 3), None), id="pair, second element invalid"),
            param([None], id="iterable, invalid"),
            param(None, id="outright invalid"),
        ],
    )
    def test_errors(self, *, item: Any) -> None:
        with raises(_NormalizeInsertItemError, match="Item must be valid; got .*"):
            _ = list(_normalize_insert_item(item))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example


class TestNormalizeUpsertItem:
    @given(id_=integers())
    def test_pair_of_dict_and_table(self, *, id_: int) -> None:
        table = self._table
        item = {"id": id_}, table
        result = one(_normalize_upsert_item(item))
        expected = _NormalizedUpsertItem(values=item[0], table=table)
        assert result == expected

    @given(ids=sets(integers()))
    def test_pair_of_list_of_dicts_and_table(self, *, ids: set[int]) -> None:
        table = self._table
        item = [({"id_": id_}) for id_ in ids], table
        result = list(_normalize_upsert_item(item))
        expected = [_NormalizedUpsertItem(values=i, table=table) for i in item[0]]
        assert result == expected

    @given(ids=sets(integers()))
    def test_list_of_pairs_of_dicts_and_table(self, *, ids: set[int]) -> None:
        table = self._table
        item = [({"id_": id_}, table) for id_ in ids]
        result = list(_normalize_upsert_item(item))
        expected = [_NormalizedUpsertItem(values=i[0], table=table) for i in item]
        assert result == expected

    @given(id_=integers())
    def test_mapped_class(self, *, id_: int) -> None:
        cls = self._mapped_class
        result = one(_normalize_upsert_item(cls(id_=id_)))
        expected = _NormalizedUpsertItem(values={"id_": id_}, table=get_table(cls))
        assert result == expected

    @given(ids=sets(integers(0, 10), min_size=1))
    def test_mapped_classes(self, *, ids: set[int]) -> None:
        cls = self._mapped_class
        result = list(_normalize_upsert_item([cls(id_=id_) for id_ in ids]))
        expected = [
            _NormalizedUpsertItem(values={"id_": id_}, table=get_table(cls))
            for id_ in ids
        ]
        assert result == expected

    @mark.parametrize(
        "item",
        [
            param((None,), id="tuple, not pair"),
            param(
                (None, Table("example", MetaData())), id="pair, first element invalid"
            ),
            param(((1, 2, 3), None), id="pair, second element invalid"),
            param([None], id="iterable, invalid"),
            param(None, id="outright invalid"),
        ],
    )
    def test_errors(self, *, item: Any) -> None:
        with raises(_NormalizeUpsertItemError, match="Item must be valid; got .*"):
            _ = list(_normalize_upsert_item(item))

    @property
    def _table(self) -> Table:
        return Table("example", MetaData(), Column("id_", Integer, primary_key=True))

    @property
    def _mapped_class(self) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = "example"

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)

        return Example


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
    @given(ids=sets(integers(0, 10), min_size=1))
    def test_main(
        self, *, create_postgres_engine: Callable[..., Engine], ids: set[int]
    ) -> None:
        key = TestPostgresEngine.test_main.__qualname__
        name = f"test_{md5_hash(key)}"
        table = Table(name, MetaData(), Column("id_", Integer, primary_key=True))
        engine = create_postgres_engine(table)
        insert_items(engine, ([(id_,) for id_ in ids], table))
        sel = select(table.c["id_"])
        with engine.begin() as conn:
            res = conn.execute(sel).scalars().all()
        assert set(res) == ids


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


class TestUpsertItems:
    @given(sqlite_engine=sqlite_engines(), triple=_upsert_triples(nullable=True))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_pair_of_dict_and_table(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_sync_pair_of_dict_and_table.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        id_, init, post = triple
        _ = self._run_test_sync(
            engine, table, ({"id_": id_, "value": init}, table), expected={(id_, init)}
        )
        _ = self._run_test_sync(
            engine,
            table,
            ({"id_": id_, "value": post}, table),
            expected={(id_, init if post is None else post)},
        )

    @given(
        sqlite_engine=sqlite_engines(), triples=_upsert_lists(nullable=True, min_size=1)
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_pair_of_list_of_dicts_and_table(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_sync_pair_of_list_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        _ = self._run_test_sync(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = (
            [
                {"id_": id_, "value": post}
                for id_, _, post in triples
                if post is not None
            ],
            table,
        )
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = self._run_test_sync(engine, table, items, expected=expected)

    @given(
        sqlite_engine=sqlite_engines(), triples=_upsert_lists(nullable=True, min_size=1)
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_list_of_pairs_of_dicts_and_table(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_sync_list_of_pairs_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        _ = self._run_test_sync(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            ({"id_": id_, "value": post}, table)
            for id_, _, post in triples
            if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = self._run_test_sync(engine, table, items, expected=expected)

    @given(sqlite_engine=sqlite_engines(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_mapped_class(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool],
    ) -> None:
        key = TestUpsertItems.test_sync_mapped_class.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, cls, dialect=dialect
        )
        id_, init, post = triple
        _ = self._run_test_sync(
            engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = self._run_test_sync(
            engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @given(
        sqlite_engine=sqlite_engines(), triples=_upsert_lists(nullable=True, min_size=1)
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_mapped_classes(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = TestUpsertItems.test_sync_mapped_classes.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, cls, dialect=dialect
        )
        _ = self._run_test_sync(
            engine,
            cls,
            [cls(id_=id_, value=init) for id_, init, _ in triples],
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            cls(id_=id_, value=post) for id_, _, post in triples if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = self._run_test_sync(engine, cls, items, expected=expected)

    @given(
        sqlite_engine=sqlite_engines(),
        id_=integers(0, 10),
        x_init=booleans(),
        x_post=booleans(),
        y=booleans(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("selected_or_all", [param("selected"), param("all")])
    def test_sync_sel_or_all(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
    ) -> None:
        key = (
            TestUpsertItems.test_sync_sel_or_all.__qualname__,
            dialect,
            selected_or_all,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table_sel_or_all(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        _ = self._run_test_sync(
            engine,
            table,
            ({"id_": id_, "x": x_init, "y": y}, table),
            selected_or_all=selected_or_all,
            expected={(id_, x_init, y)},
        )
        match selected_or_all:
            case "selected":
                expected = (id_, x_post, y)
            case "all":
                expected = (id_, x_post, None)
        _ = self._run_test_sync(
            engine,
            table,
            ({"id_": id_, "x": x_post}, table),
            selected_or_all=selected_or_all,
            expected={expected},
        )

    @given(sqlite_engine=sqlite_engines(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("single_or_list", [param("single"), param("list")])
    def test_sync_updated(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        single_or_list: Literal["single", "list"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_sync_updated.__qualname__, dialect, single_or_list
        name = f"test_{md5_hash(key)}"
        table = self._get_table_updated(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        id_, init, post = triple
        match single_or_list:
            case "single":
                item1 = ({"id_": id_, "value": init}, table)
            case "list":
                item1 = [({"id_": id_, "value": init}, table)]
        ((_, _, updated1),) = self._run_test_sync(engine, table, item1)
        time.sleep(0.01)
        match single_or_list:
            case "single":
                item2 = ({"id_": id_, "value": post}, table)
            case "list":
                item2 = [({"id_": id_, "value": post}, table)]
        ((_, _, updated2),) = self._run_test_sync(engine, table, item2)
        assert updated1 < updated2

    @given(sqlite_engine=sqlite_engines(), id_=integers(0, 10))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_assume_table_exists(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        id_: int,
    ) -> None:
        key = TestUpsertItems.test_sync_assume_table_exists.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        _ = self._run_test_sync(
            engine, table, ({"id_": id_}, table), assume_tables_exist=True
        )

    @given(
        sqlite_engine=sqlite_engines(),
        ids=sets_fixed_length(int32s(), 2).map(tuple),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_both_nulls_and_non_nulls(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
        ids: tuple[int, int],
        value1: bool | None,
        value2: bool | None,
    ) -> None:
        key = TestUpsertItems.test_sync_both_nulls_and_non_nulls.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        id1, id2 = ids
        upsert_items(
            engine,
            ([{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table),
        )

    @given(sqlite_engine=sqlite_engines())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    def test_sync_error(
        self,
        *,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        dialect: Literal["sqlite", "postgres"],
    ) -> None:
        key = TestUpsertItems.test_sync_error.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = self._get_engine_sync(
            sqlite_engine, create_postgres_engine, table, dialect=dialect
        )
        with raises(UpsertItemsError, match="Item must be valid; got None"):
            _ = self._run_test_sync(engine, table, cast(Any, None))

    @given(data=data(), triple=_upsert_triples(nullable=True))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_pair_of_dict_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_async_pair_of_dict_and_table.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id_, init, post = triple
        _ = await self._run_test_async(
            engine, table, ({"id_": id_, "value": init}, table), expected={(id_, init)}
        )
        _ = await self._run_test_async(
            engine,
            table,
            ({"id_": id_, "value": post}, table),
            expected={(id_, init if post is None else post)},
        )

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_pair_of_list_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_async_pair_of_list_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = (
            [
                {"id_": id_, "value": post}
                for id_, _, post in triples
                if post is not None
            ],
            table,
        )
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, table, items, expected=expected)

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_list_of_pairs_of_dicts_and_table(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = (
            TestUpsertItems.test_async_list_of_pairs_of_dicts_and_table.__qualname__,
            dialect,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            table,
            ([{"id_": id_, "value": init} for id_, init, _ in triples], table),
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            ({"id_": id_, "value": post}, table)
            for id_, _, post in triples
            if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, table, items, expected=expected)

    @given(data=data(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_mapped_class(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triple: tuple[int, bool, bool],
    ) -> None:
        key = TestUpsertItems.test_async_mapped_class.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, cls, dialect=dialect
        )
        id_, init, post = triple
        _ = await self._run_test_async(
            engine, cls, cls(id_=id_, value=init), expected={(id_, init)}
        )
        _ = await self._run_test_async(
            engine, cls, cls(id_=id_, value=post), expected={(id_, post)}
        )

    @given(data=data(), triples=_upsert_lists(nullable=True, min_size=1))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_mapped_classes(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        triples: list[tuple[int, bool, bool | None]],
    ) -> None:
        key = TestUpsertItems.test_async_mapped_classes.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        cls = self._get_mapped_class(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, cls, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            cls,
            [cls(id_=id_, value=init) for id_, init, _ in triples],
            expected={(id_, init) for id_, init, _ in triples},
        )
        items = [
            cls(id_=id_, value=post) for id_, _, post in triples if post is not None
        ]
        expected = {
            (id_, init if post is None else post) for id_, init, post in triples
        }
        _ = await self._run_test_async(engine, cls, items, expected=expected)

    @given(
        data=data(),
        id_=integers(0, 10),
        x_init=booleans(),
        x_post=booleans(),
        y=booleans(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("selected_or_all", [param("selected"), param("all")])
    async def test_async_sel_or_all(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        selected_or_all: Literal["selected", "all"],
        id_: int,
        x_init: bool,
        x_post: bool,
        y: bool,
    ) -> None:
        key = (
            TestUpsertItems.test_async_sel_or_all.__qualname__,
            dialect,
            selected_or_all,
        )
        name = f"test_{md5_hash(key)}"
        table = self._get_table_sel_or_all(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine,
            table,
            ({"id_": id_, "x": x_init, "y": y}, table),
            selected_or_all=selected_or_all,
            expected={(id_, x_init, y)},
        )
        match selected_or_all:
            case "selected":
                expected = (id_, x_post, y)
            case "all":
                expected = (id_, x_post, None)
        _ = await self._run_test_async(
            engine,
            table,
            ({"id_": id_, "x": x_post}, table),
            selected_or_all=selected_or_all,
            expected={expected},
        )

    @given(data=data(), triple=_upsert_triples())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    @mark.parametrize("single_or_list", [param("single"), param("list")])
    async def test_async_updated(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        single_or_list: Literal["single", "list"],
        triple: tuple[int, bool, bool | None],
    ) -> None:
        key = TestUpsertItems.test_async_updated.__qualname__, dialect, single_or_list
        name = f"test_{md5_hash(key)}"
        table = self._get_table_updated(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id_, init, post = triple
        match single_or_list:
            case "single":
                item1 = ({"id_": id_, "value": init}, table)
            case "list":
                item1 = [({"id_": id_, "value": init}, table)]
        ((_, _, updated1),) = await self._run_test_async(engine, table, item1)
        await asyncio.sleep(0.01)
        match single_or_list:
            case "single":
                item2 = ({"id_": id_, "value": post}, table)
            case "list":
                item2 = [({"id_": id_, "value": post}, table)]
        ((_, _, updated2),) = await self._run_test_async(engine, table, item2)
        assert updated1 < updated2

    @given(data=data(), id_=integers(0, 10))
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_assume_table_exists(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        id_: int,
    ) -> None:
        key = TestUpsertItems.test_async_assume_table_exists.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        _ = await self._run_test_async(
            engine, table, ({"id_": id_}, table), assume_tables_exist=True
        )

    @given(
        data=data(),
        ids=sets_fixed_length(int32s(), 2).map(tuple),
        value1=booleans() | none(),
        value2=booleans() | none(),
    )
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_both_nulls_and_non_nulls(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
        ids: tuple[int, int],
        value1: bool | None,
        value2: bool | None,
    ) -> None:
        key = TestUpsertItems.test_async_both_nulls_and_non_nulls.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        id1, id2 = ids
        await upsert_items_async(
            engine,
            ([{"id_": id1, "value": value1}, {"id_": id2, "value": value2}], table),
        )

    @given(data=data())
    @mark.parametrize("dialect", [param("sqlite"), param("postgres", marks=SKIPIF_CI)])
    async def test_async_error(
        self,
        *,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        dialect: Literal["sqlite", "postgres"],
    ) -> None:
        key = TestUpsertItems.test_async_error.__qualname__, dialect
        name = f"test_{md5_hash(key)}"
        table = self._get_table(name)
        engine = await self._get_engine_async(
            data, create_postgres_engine_async, table, dialect=dialect
        )
        with raises(UpsertItemsAsyncError, match="Item must be valid; got None"):
            _ = await self._run_test_async(engine, table, cast(Any, None))

    def _get_table(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=True),
        )

    def _get_table_sel_or_all(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("x", Boolean, nullable=False),
            Column("y", Boolean, nullable=True),
        )

    def _get_table_updated(self, name: str, /) -> Table:
        return Table(
            name,
            MetaData(),
            Column("id_", Integer, primary_key=True),
            Column("value", Boolean, nullable=False),
            Column(
                "updated_at",
                DateTime(timezone=True),
                server_default=func.now(),
                onupdate=func.now(),
            ),
        )

    def _get_mapped_class(self, name: str, /) -> type[DeclarativeBase]:
        class Base(DeclarativeBase, MappedAsDataclass): ...

        class Example(Base):
            __tablename__ = name

            id_: Mapped[int] = mapped_column(Integer, kw_only=True, primary_key=True)
            value: Mapped[bool] = mapped_column(Boolean, kw_only=True, nullable=False)

        return Example

    def _get_engine_sync(
        self,
        sqlite_engine: Engine,
        create_postgres_engine: Callable[..., Engine],
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        dialect: Literal["sqlite", "postgres"],
    ) -> Engine:
        match dialect:
            case "sqlite":
                return sqlite_engine
            case "postgres":
                return create_postgres_engine(table_or_mapped_class)

    async def _get_engine_async(
        self,
        data: DataObject,
        create_postgres_engine_async: Callable[..., Coroutine1[AsyncEngine]],
        table_or_mapped_class: TableOrMappedClass,
        /,
        *,
        dialect: Literal["sqlite", "postgres"],
    ) -> AsyncEngine:
        match dialect:
            case "sqlite":
                return await aiosqlite_engines(data)
            case "postgres":
                return await create_postgres_engine_async(table_or_mapped_class)

    def _run_test_sync(
        self,
        engine_or_conn: EngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *items: _UpsertItem,
        assume_tables_exist: bool = False,
        selected_or_all: Literal["selected", "all"] = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> Sequence[Row[Any]]:
        if assume_tables_exist:
            with raises((OperationalError, ProgrammingError)):
                upsert_items(
                    engine_or_conn,
                    *items,
                    assume_tables_exist=assume_tables_exist,
                    selected_or_all=selected_or_all,
                )
            return []
        upsert_items(
            engine_or_conn,
            *items,
            assume_tables_exist=assume_tables_exist,
            selected_or_all=selected_or_all,
        )
        sel = self._get_select(table_or_mapped_class)
        with yield_connection(engine_or_conn) as conn:
            results = conn.execute(sel).all()
        if expected is not None:
            self._assert_results(results, expected)
        return results

    async def _run_test_async(
        self,
        engine_or_conn: AsyncEngineOrConnection,
        table_or_mapped_class: TableOrMappedClass,
        /,
        *items: _UpsertItem,
        assume_tables_exist: bool = False,
        selected_or_all: Literal["selected", "all"] = "selected",
        expected: set[tuple[Any, ...]] | None = None,
    ) -> Sequence[Row[Any]]:
        if assume_tables_exist:
            with raises((OperationalError, ProgrammingError)):
                await upsert_items_async(
                    engine_or_conn,
                    *items,
                    assume_tables_exist=assume_tables_exist,
                    selected_or_all=selected_or_all,
                )
            return []
        await upsert_items_async(
            engine_or_conn,
            *items,
            assume_tables_exist=assume_tables_exist,
            selected_or_all=selected_or_all,
        )
        sel = self._get_select(table_or_mapped_class)
        async with yield_connection_async(engine_or_conn) as conn:
            results = (await conn.execute(sel)).all()
        if expected is not None:
            self._assert_results(results, expected)
        return results

    def _get_select(self, table_or_mapped_class: TableOrMappedClass, /) -> Select[Any]:
        return select(get_table(table_or_mapped_class))

    def _assert_results(self, results: Sequence[Any], ids: set[Any], /) -> None:
        assert set(results) == ids


class TestUpsertItemPrepare:
    @given(engine=sqlite_engines())
    def test_error(self, *, engine: Engine) -> None:
        with raises(_UpsertItemsPrepareError, match="Item must be valid; got None"):
            _ = _upsert_items_prepare(engine, cast(Any, None))


class TestYieldConnection:
    @given(engine=sqlite_engines())
    def test_sync_engine(self, *, engine: Engine) -> None:
        with yield_connection(engine) as conn:
            assert isinstance(conn, Connection)

    @given(engine=sqlite_engines())
    def test_sync_connection(self, *, engine: Engine) -> None:
        with engine.begin() as conn1, yield_connection(conn1) as conn2:
            assert isinstance(conn2, Connection)

    @given(data=data())
    async def test_async_engine(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        async with yield_connection_async(engine) as conn:
            assert isinstance(conn, AsyncConnection)

    @given(data=data())
    async def test_async_conn(self, *, data: DataObject) -> None:
        engine = await aiosqlite_engines(data)
        async with engine.begin() as conn1, yield_connection_async(conn1) as conn2:
            assert isinstance(conn2, AsyncConnection)


class TestYieldPrimaryKeyColumns:
    def test_main(self) -> None:
        table = Table(
            "example",
            MetaData(),
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )
        columns = list(yield_primary_key_columns(table))
        expected = [
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        ]
        for c, e in zip(columns, expected, strict=True):
            _check_columns_equal(c, e)
