from __future__ import annotations

from utilities.typed_settings.typed_settings import (
    AppNameContainsUnderscoreError,
    get_repo_root_config,
    load_settings,
)

__all__ = [
    "AppNameContainsUnderscoreError",
    "get_repo_root_config",
    "load_settings",
]


try:
    from utilities.typed_settings.click import (
        click_field,
        click_options,
    )
except ModuleNotFoundError:  # pragma: no cover
    pass
else:
    __all__ += [
        "click_field",
        "click_options",
    ]
