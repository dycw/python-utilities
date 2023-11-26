from __future__ import annotations

from dataclasses import asdict
from typing import Any

from xarray import DataArray


def rename_data_arrays(obj: Any, /) -> None:
    """Rename the arrays on a field."""
    for key, value in asdict(obj).items():
        if isinstance(value, DataArray) and (value.name != key):
            setattr(obj, key, value.rename(key))


__all__ = ["rename_data_arrays"]
