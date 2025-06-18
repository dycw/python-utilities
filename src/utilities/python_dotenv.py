from __future__ import annotations

from dataclasses import dataclass
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, override

from dotenv import dotenv_values

from utilities.dataclasses import _ParseDataClassMissingValuesError, parse_dataclass
from utilities.iterables import MergeStrMappingsError, merge_str_mappings
from utilities.pathlib import get_root
from utilities.reprlib import get_repr
from utilities.types import Dataclass

if TYPE_CHECKING:
    from collections.abc import Mapping
    from collections.abc import Set as AbstractSet

    from utilities.types import MaybeCallablePathLike, ParseObjectExtra, StrMapping


def load_settings[T: Dataclass](
    cls: type[T],
    /,
    *,
    path: MaybeCallablePathLike | None = Path.cwd,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
    warn_name_errors: bool = False,
    head: bool = False,
    case_sensitive: bool = False,
    extra_parsers: ParseObjectExtra | None = None,
) -> T:
    """Load a set of settings from the `.env` file."""
    path = get_root(path=path).joinpath(".env")
    if not path.exists():
        raise _LoadSettingsFileNotFoundError(path=path) from None
    maybe_values_dotenv = dotenv_values(path)
    try:
        maybe_values: Mapping[str, str | None] = merge_str_mappings(
            maybe_values_dotenv, environ, case_sensitive=case_sensitive
        )
    except MergeStrMappingsError as error:
        raise _LoadSettingsDuplicateKeysError(
            path=path,
            values=error.mapping,
            counts=error.counts,
            case_sensitive=case_sensitive,
        ) from None
    values = {k: v for k, v in maybe_values.items() if v is not None}
    try:
        return parse_dataclass(
            values,
            cls,
            globalns=globalns,
            localns=localns,
            warn_name_errors=warn_name_errors,
            head=head,
            case_sensitive=case_sensitive,
            allow_extra_keys=True,
            extra_parsers=extra_parsers,
        )
    except _ParseDataClassMissingValuesError as error:
        raise _LoadSettingsMissingKeysError(path=path, fields=error.fields) from None


@dataclass(kw_only=True, slots=True)
class LoadSettingsError(Exception):
    path: Path


@dataclass(kw_only=True, slots=True)
class _LoadSettingsDuplicateKeysError(LoadSettingsError):
    values: StrMapping
    counts: Mapping[str, int]
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(dict(self.values))} keys must not contain duplicates (modulo case); got {get_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsFileNotFoundError(LoadSettingsError):
    @override
    def __str__(self) -> str:
        return f"Path {str(self.path)!r} must exist"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsMissingKeysError(LoadSettingsError):
    fields: AbstractSet[str]

    @override
    def __str__(self) -> str:
        desc = ", ".join(map(repr, sorted(self.fields)))
        return f"Unable to load {str(self.path)!r}; missing value(s) for {desc}"


__all__ = ["LoadSettingsError", "load_settings"]
