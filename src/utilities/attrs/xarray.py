from typing import Any

from attrs import asdict
from xarray import DataArray


def rename_data_arrays(obj: Any, /) -> None:
    """Rename the arrays on a field."""
    for key, value in asdict(obj).items():
        if isinstance(value, DataArray) and (value.name != key):
            setattr(obj, key, value.rename(key))
