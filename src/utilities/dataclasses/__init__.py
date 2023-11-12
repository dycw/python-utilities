from __future__ import annotations

from utilities.dataclasses.dataclasses import (
    NotADataClassNorADataClassInstanceError,
    get_dataclass_class,
    is_dataclass_class,
    is_dataclass_instance,
    yield_field_names,
)

__all__ = [
    "get_dataclass_class",
    "is_dataclass_class",
    "is_dataclass_instance",
    "NotADataClassNorADataClassInstanceError",
    "yield_field_names",
]


try:
    from utilities.dataclasses.xarray import (
        rename_data_arrays,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "rename_data_arrays",
    ]
