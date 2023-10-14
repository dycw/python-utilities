from __future__ import annotations

from .sqlalchemy import Dialect
from .sqlalchemy import EngineError
from .sqlalchemy import IncorrectNumberOfTablesError
from .sqlalchemy import ParseEngineError
from .sqlalchemy import TableAlreadyExistsError
from .sqlalchemy import TablenameMixin
from .sqlalchemy import UnequalBooleanColumnCreateConstraintError
from .sqlalchemy import UnequalBooleanColumnNameError
from .sqlalchemy import UnequalColumnTypesError
from .sqlalchemy import UnequalDateTimeColumnTimezoneError
from .sqlalchemy import UnequalEnumColumnCreateConstraintError
from .sqlalchemy import UnequalEnumColumnInheritSchemaError
from .sqlalchemy import UnequalEnumColumnLengthError
from .sqlalchemy import UnequalEnumColumnNativeEnumError
from .sqlalchemy import UnequalEnumColumnTypesError
from .sqlalchemy import UnequalFloatColumnAsDecimalError
from .sqlalchemy import UnequalFloatColumnDecimalReturnScaleError
from .sqlalchemy import UnequalFloatColumnPrecisionsError
from .sqlalchemy import UnequalIntervalColumnDayPrecisionError
from .sqlalchemy import UnequalIntervalColumnNativeError
from .sqlalchemy import UnequalIntervalColumnSecondPrecisionError
from .sqlalchemy import UnequalLargeBinaryColumnLengthError
from .sqlalchemy import UnequalNullableStatusError
from .sqlalchemy import UnequalNumberOfColumnsError
from .sqlalchemy import UnequalNumericScaleError
from .sqlalchemy import UnequalPrimaryKeyStatusError
from .sqlalchemy import UnequalSetOfColumnsError
from .sqlalchemy import UnequalStringCollationError
from .sqlalchemy import UnequalStringLengthError
from .sqlalchemy import UnequalTableOrColumnNamesError
from .sqlalchemy import UnequalTableOrColumnSnakeCaseNamesError
from .sqlalchemy import UnequalUUIDAsUUIDError
from .sqlalchemy import UnequalUUIDNativeUUIDError
from .sqlalchemy import UnsupportedDialectError
from .sqlalchemy import check_engine
from .sqlalchemy import check_table_against_reflection
from .sqlalchemy import check_tables_equal
from .sqlalchemy import columnwise_max
from .sqlalchemy import columnwise_min
from .sqlalchemy import create_engine
from .sqlalchemy import ensure_engine
from .sqlalchemy import ensure_table_created
from .sqlalchemy import ensure_table_dropped
from .sqlalchemy import get_column_names
from .sqlalchemy import get_columns
from .sqlalchemy import get_dialect
from .sqlalchemy import get_table
from .sqlalchemy import get_table_name
from .sqlalchemy import model_to_dict
from .sqlalchemy import parse_engine
from .sqlalchemy import redirect_to_no_such_table_error
from .sqlalchemy import redirect_to_table_already_exists_error
from .sqlalchemy import serialize_engine
from .sqlalchemy import yield_connection
from .sqlalchemy import yield_in_clause_rows

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
    from .fastparquet import select_to_parquet
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["select_to_parquet"]


try:
    from .pandas import DatesWithTimeComponentsError
    from .pandas import NonPositiveStreamError
    from .pandas import SeriesAgainstTableColumnError
    from .pandas import SeriesNameNotInTableError
    from .pandas import SeriesNameSnakeCaseNotInTableError
    from .pandas import insert_dataframe
    from .pandas import insert_items
    from .pandas import select_to_dataframe
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
    from .timeout_decorator import NoSuchSequenceError
    from .timeout_decorator import next_from_sequence
    from .timeout_decorator import redirect_to_no_such_sequence_error
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "next_from_sequence",
        "NoSuchSequenceError",
        "redirect_to_no_such_sequence_error",
    ]
