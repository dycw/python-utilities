from __future__ import annotations

from utilities.sqlalchemy.sqlalchemy import (
    Dialect,
    EngineError,
    IncorrectNumberOfTablesError,
    ParseEngineError,
    TableAlreadyExistsError,
    TablenameMixin,
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
    check_engine,
    check_table_against_reflection,
    check_tables_equal,
    columnwise_max,
    columnwise_min,
    create_engine,
    ensure_engine,
    ensure_table_created,
    ensure_table_dropped,
    get_column_names,
    get_columns,
    get_dialect,
    get_table,
    get_table_name,
    model_to_dict,
    parse_engine,
    redirect_to_no_such_table_error,
    redirect_to_table_already_exists_error,
    serialize_engine,
    yield_connection,
    yield_in_clause_rows,
)

__all__ = [
    "check_engine",
    "check_table_against_reflection",
    "check_tables_equal",
    "columnwise_max",
    "columnwise_min",
    "create_engine",
    "Dialect",
    "EngineError",
    "ensure_engine",
    "ensure_table_created",
    "ensure_table_dropped",
    "get_column_names",
    "get_columns",
    "get_dialect",
    "get_table_name",
    "get_table",
    "IncorrectNumberOfTablesError",
    "model_to_dict",
    "parse_engine",
    "ParseEngineError",
    "redirect_to_no_such_table_error",
    "redirect_to_table_already_exists_error",
    "serialize_engine",
    "TableAlreadyExistsError",
    "TablenameMixin",
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
    "UnsupportedDialectError",
    "yield_connection",
    "yield_in_clause_rows",
]


try:
    from utilities.sqlalchemy.fastparquet import select_to_parquet
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["select_to_parquet"]


try:
    from utilities.sqlalchemy.pandas import (
        DatesWithTimeComponentsError,
        NonPositiveStreamError,
        SeriesAgainstTableColumnError,
        SeriesNameNotInTableError,
        SeriesNameSnakeCaseNotInTableError,
        insert_dataframe,
        insert_items,
        select_to_dataframe,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "DatesWithTimeComponentsError",
        "insert_dataframe",
        "insert_items",
        "NonPositiveStreamError",
        "select_to_dataframe",
        "SeriesAgainstTableColumnError",
        "SeriesNameNotInTableError",
        "SeriesNameSnakeCaseNotInTableError",
    ]


try:
    from utilities.sqlalchemy.timeout_decorator import (
        NoSuchSequenceError,
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
    ]
