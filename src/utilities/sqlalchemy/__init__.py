from __future__ import annotations

from utilities.sqlalchemy.sqlalchemy import (
    CHUNK_SIZE_FRAC,
    Dialect,
    EngineError,
    FirstArgumentInvalidError,
    FirstArgumentListItemInvalidError,
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
    UnsupportedDialectError,
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

__all__ = [
    "check_dataframe_schema_against_table",
    "check_engine",
    "check_selectable_for_duplicate_columns",
    "check_table_against_reflection",
    "check_tables_equal",
    "CHUNK_SIZE_FRAC",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "Dialect",
    "EngineError",
    "ensure_engine",
    "ensure_tables_created",
    "ensure_tables_dropped",
    "FirstArgumentInvalidError",
    "FirstArgumentListItemInvalidError",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table_name",
    "get_table",
    "IncorrectNumberOfTablesError",
    "insert_items",
    "InvalidItemError",
    "InvalidItemInIterableError",
    "is_mapped_class",
    "is_table_or_mapped_class",
    "mapped_class_to_dict",
    "NotATableOrAMappedClassError",
    "parse_engine",
    "ParseEngineError",
    "redirect_to_no_such_table_error",
    "redirect_to_table_already_exists_error",
    "SecondArgumentNotATableOrMappedClassError",
    "serialize_engine",
    "SeriesAndTableColumnIncompatibleError",
    "SeriesMatchesAgainstMultipleColumnsError",
    "SeriesMatchesAgainstNoColumnError",
    "TableAlreadyExistsError",
    "TablenameMixin",
    "TupleNotAPairError",
    "UnequalBooleanColumnCreateConstraintError",
    "UnequalBooleanColumnNameError",
    "UnequalColumnTypesError",
    "UnequalDateTimeColumnTimezoneError",
    "UnequalEnumColumnCreateConstraintError",
    "UnequalEnumColumnInheritSchemaError",
    "UnequalEnumColumnLengthError",
    "UnequalEnumColumnNativeEnumError",
    "UnequalEnumColumnTypesError",
    "UnequalFloatColumnAsDecimalError",
    "UnequalFloatColumnDecimalReturnScaleError",
    "UnequalFloatColumnPrecisionsError",
    "UnequalIntervalColumnDayPrecisionError",
    "UnequalIntervalColumnNativeError",
    "UnequalIntervalColumnSecondPrecisionError",
    "UnequalLargeBinaryColumnLengthError",
    "UnequalNullableStatusError",
    "UnequalNumberOfColumnsError",
    "UnequalNumericScaleError",
    "UnequalPrimaryKeyStatusError",
    "UnequalSetOfColumnsError",
    "UnequalStringCollationError",
    "UnequalStringLengthError",
    "UnequalTableOrColumnNamesError",
    "UnequalTableOrColumnSnakeCaseNamesError",
    "UnequalUUIDAsUUIDError",
    "UnequalUUIDNativeUUIDError",
    "UnsupportedDialectError",
    "yield_connection",
]


try:
    from utilities.sqlalchemy.fastparquet import select_to_parquet
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["select_to_parquet"]


try:
    from utilities.sqlalchemy.pandas import (
        ColumnToPandasDTypeError,
        NonPositiveStreamError,
        PandasDataFrameYieldsNoRowsError,
        insert_pandas_dataframe,
        select_to_pandas_dataframe,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "ColumnToPandasDTypeError",
        "insert_pandas_dataframe",
        "NonPositiveStreamError",
        "PandasDataFrameYieldsNoRowsError",
        "select_to_pandas_dataframe",
    ]


try:
    from utilities.sqlalchemy.polars import (
        ColumnToPolarsExprError,
        PolarsDataFrameYieldsNoRowsError,
        insert_polars_dataframe,
        select_to_polars_dataframe,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "ColumnToPolarsExprError",
        "insert_polars_dataframe",
        "PolarsDataFrameYieldsNoRowsError",
        "select_to_polars_dataframe",
    ]

try:
    from utilities.sqlalchemy.timeout_decorator import (
        NoSuchSequenceError,
        SQLiteDoesNotSupportSequencesError,
        next_from_sequence,
        redirect_to_no_such_sequence_error,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "next_from_sequence",
        "NoSuchSequenceError",
        "redirect_to_no_such_sequence_error",
        "SQLiteDoesNotSupportSequencesError",
    ]
