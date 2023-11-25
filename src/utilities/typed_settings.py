from __future__ import annotations

from collections.abc import Iterable
from typing import Any, TypeVar, cast

from typed_settings import load_settings as _load_settings
from typed_settings.types import AUTO, _Auto

from utilities._typed_settings.common import (
    CONFIG_FILES,
    ExtendedTSConverter,
    GetLoadersError,
    get_loaders,
)
from utilities.pathlib import PathLike

_T = TypeVar("_T")


def load_settings(
    cls: type[_T],
    /,
    *,
    appname: str = "appname",
    config_files: Iterable[PathLike] = CONFIG_FILES,
    config_file_section: str | _Auto = AUTO,
    config_files_var: None | str | _Auto = AUTO,
    env_prefix: None | str | _Auto = AUTO,
) -> _T:
    """Load a settings object with the extended converter."""
    loaders = get_loaders(
        appname=appname,
        config_files=config_files,
        config_file_section=config_file_section,
        config_files_var=config_files_var,
        env_prefix=env_prefix,
    )
    converter = ExtendedTSConverter()
    return _load_settings(cast(Any, cls), loaders, converter=converter)


__all__ = [
    "CONFIG_FILES",
    "ExtendedTSConverter",
    "get_loaders",
    "GetLoadersError",
    "load_settings",
]


try:
    from utilities._typed_settings.click import click_field, click_options
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += ["click_field", "click_options"]
