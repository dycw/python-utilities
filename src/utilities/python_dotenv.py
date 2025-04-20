from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from os import environ
from typing import TYPE_CHECKING, Any, override

from dotenv import dotenv_values

from utilities.dataclasses import (
    _MappingToDataclassEmptyError,
    _YieldFieldsClass,
    mapping_to_dataclass,
)
from utilities.git import get_repo_root
from utilities.iterables import MergeStrMappingsError, merge_str_mappings
from utilities.parse import ParseTextError, parse_text
from utilities.pathlib import PWD
from utilities.reprlib import get_repr

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from utilities.types import PathLike, StrMapping, TDataclass


def load_settings(
    cls: type[TDataclass],
    /,
    *,
    cwd: PathLike = PWD,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> TDataclass:
    """Load a set of settings from the `.env` file."""
    path = get_repo_root(cwd=cwd).joinpath(".env")
    if not path.exists():
        raise _LoadSettingsFileNotFoundError(path=path) from None
    maybe_values_dotenv = dotenv_values(path)
    try:
        maybe_values = merge_str_mappings(maybe_values_dotenv, environ)
    except MergeStrMappingsError as error:
        raise _LoadSettingsDuplicateKeysError(
            path=path, values=error.mapping, counts=error.counts
        ) from None
    values = {k: v for k, v in maybe_values.items() if v is not None}
    try:
        return mapping_to_dataclass(
            cls,
            values,
            globalns=globalns,
            localns=localns,
            post=partial(_load_settings_post, path=path, values=values),
        )
    except _MappingToDataclassEmptyError as error:
        raise _LoadSettingsEmptyError(
            path=path, values=error.mapping, field=error.field
        ) from None


def _load_settings_post(
    field: _YieldFieldsClass[Any], text: str, /, *, path: Path, values: StrMapping
) -> Any:
    try:
        return parse_text(field.type_, text)
    except ParseTextError:
        raise _LoadSettingsParseTextError(
            path=path, values=values, field=field, text=text
        ) from None


@dataclass(kw_only=True, slots=True)
class LoadSettingsError(Exception):
    path: Path


@dataclass(kw_only=True, slots=True)
class _LoadSettingsDuplicateKeysError(LoadSettingsError):
    values: StrMapping
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(dict(self.values))} keys must not contain duplicates (modulo case); got {get_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsEmptyError(LoadSettingsError):
    values: StrMapping
    field: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist (modulo case)"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsFileNotFoundError(LoadSettingsError):
    @override
    def __str__(self) -> str:
        return f"Path {str(self.path)!r} must exist"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsParseTextError(LoadSettingsError):
    values: StrMapping
    field: _YieldFieldsClass[Any]
    text: str

    @override
    def __str__(self) -> str:
        return f"Unable to parse field {self.field.name!r} of type {self.field.type_!r}; got {self.text!r}"


__all__ = ["LoadSettingsError", "load_settings"]
