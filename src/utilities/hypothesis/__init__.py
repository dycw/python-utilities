from __future__ import annotations

from utilities.hypothesis.git import git_repos
from utilities.hypothesis.hypothesis import (
    MaybeSearchStrategy,
    Shape,
    assume_does_not_raise,
    datetimes_utc,
    floats_extra,
    hashables,
    lift_draw,
    lists_fixed_length,
    setup_hypothesis_profiles,
    slices,
    temp_dirs,
    temp_paths,
    text_ascii,
    text_clean,
    text_printable,
)

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
    from utilities.hypothesis.luigi import namespace_mixins
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["namespace_mixins"]


try:
    from utilities.hypothesis.numpy import (
        bool_arrays,
        concatenated_arrays,
        datetime64_arrays,
        datetime64_dtypes,
        datetime64_indexes,
        datetime64_kinds,
        datetime64_units,
        datetime64D_indexes,
        datetime64s,
        datetime64us_indexes,
        float_arrays,
        int32s,
        int64s,
        int_arrays,
        str_arrays,
        uint32s,
        uint64s,
    )
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
    from utilities.hypothesis.pandas import (
        dates_pd,
        datetimes_pd,
        indexes,
        int_indexes,
        str_indexes,
        timestamps,
    )
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
    from utilities.hypothesis.semver import versions
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["versions"]


try:
    from utilities.hypothesis.sqlalchemy import sqlite_engines
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["sqlite_engines"]


try:
    from utilities.hypothesis.xarray import (
        bool_data_arrays,
        dicts_of_indexes,
        float_data_arrays,
        int_data_arrays,
        str_data_arrays,
    )
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
