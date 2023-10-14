from __future__ import annotations

from .git import git_repos
from .hypothesis import MaybeSearchStrategy
from .hypothesis import Shape
from .hypothesis import assume_does_not_raise
from .hypothesis import datetimes_utc
from .hypothesis import floats_extra
from .hypothesis import hashables
from .hypothesis import lift_draw
from .hypothesis import lists_fixed_length
from .hypothesis import setup_hypothesis_profiles
from .hypothesis import slices
from .hypothesis import temp_dirs
from .hypothesis import temp_paths
from .hypothesis import text_ascii
from .hypothesis import text_clean
from .hypothesis import text_printable

__all__ = [
    "assume_does_not_raise",
    "datetimes_utc",
    "floats_extra",
    "git_repos",
    "hashables",
    "lift_draw",
    "lists_fixed_length",
    "MaybeSearchStrategy",
    "setup_hypothesis_profiles",
    "Shape",
    "slices",
    "temp_dirs",
    "temp_paths",
    "text_ascii",
    "text_clean",
    "text_printable",
]


try:
    from .luigi import namespace_mixins
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["namespace_mixins"]


try:
    from .numpy import bool_arrays
    from .numpy import concatenated_arrays
    from .numpy import datetime64_arrays
    from .numpy import datetime64_dtypes
    from .numpy import datetime64_indexes
    from .numpy import datetime64_kinds
    from .numpy import datetime64_units
    from .numpy import datetime64D_indexes
    from .numpy import datetime64s
    from .numpy import datetime64us_indexes
    from .numpy import float_arrays
    from .numpy import int32s
    from .numpy import int64s
    from .numpy import int_arrays
    from .numpy import str_arrays
    from .numpy import uint32s
    from .numpy import uint64s
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "bool_arrays",
        "concatenated_arrays",
        "datetime64_arrays",
        "datetime64_dtypes",
        "datetime64_indexes",
        "datetime64_kinds",
        "datetime64_units",
        "datetime64D_indexes",
        "datetime64s",
        "datetime64us_indexes",
        "float_arrays",
        "int_arrays",
        "int32s",
        "int64s",
        "str_arrays",
        "uint32s",
        "uint64s",
    ]


try:
    from .pandas import dates_pd
    from .pandas import datetimes_pd
    from .pandas import indexes
    from .pandas import int_indexes
    from .pandas import str_indexes
    from .pandas import timestamps
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "dates_pd",
        "datetimes_pd",
        "indexes",
        "int_indexes",
        "str_indexes",
        "timestamps",
    ]


try:
    from .semver import versions
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["versions"]


try:
    from .sqlalchemy import sqlite_engines
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["sqlite_engines"]


try:
    from .xarray import bool_data_arrays
    from .xarray import dicts_of_indexes
    from .xarray import float_data_arrays
    from .xarray import int_data_arrays
    from .xarray import str_data_arrays
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "bool_data_arrays",
        "dicts_of_indexes",
        "float_data_arrays",
        "int_data_arrays",
        "str_data_arrays",
    ]
