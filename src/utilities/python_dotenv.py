from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import Enum
from functools import partial
from os import environ
from pathlib import Path
from re import IGNORECASE, search
from typing import TYPE_CHECKING, Any, override

from dotenv import dotenv_values

from utilities.dataclasses import (
    _MappingToDataclassEmptyError,
    _YieldFieldsClass,
    mapping_to_dataclass,
)
from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import get_class_name
from utilities.git import get_repo_root
from utilities.iterables import MergeStrMappingsError, merge_str_mappings, one, one_str
from utilities.pathlib import PWD
from utilities.reprlib import get_repr
from utilities.typing import get_args, is_literal_type, is_optional_type

if TYPE_CHECKING:
    from collections.abc import Mapping

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
        maybe_values = merge_str_mappings(
            maybe_values_dotenv, environ, case_sensitive=False
        )
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
            case_sensitive=False,
            post=partial(_load_settings_post, path=path, values=values),
        )
    except _MappingToDataclassEmptyError as error:
        raise _LoadSettingsEmptyError(
            path=path, values=error.mapping, field=error.field
        ) from None


def _load_settings_post(
    field: _YieldFieldsClass[Any], value: Any, /, *, path: Path, values: StrMapping
) -> Any:
    type_ = field.type_
    if type_ is str:
        return value
    if type_ is bool:
        if value == "0" or search("false", value, flags=IGNORECASE):
            return False
        if value == "1" or search("true", value, flags=IGNORECASE):
            return True
        raise _LoadSettingsInvalidBoolError(
            path=path, values=values, field=field.name, value=value
        )
    if type_ is float:
        try:
            return float(value)
        except ValueError:
            raise _LoadSettingsInvalidFloatError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is int:
        try:
            return int(value)
        except ValueError:
            raise _LoadSettingsInvalidIntError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is Path:
        return Path(value).expanduser()
    if type_ is dt.date:
        from utilities.whenever import ParseDateError, parse_date

        try:
            return parse_date(value)
        except ParseDateError:
            raise _LoadSettingsInvalidDateError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if type_ is dt.timedelta:
        from utilities.whenever import ParseTimedeltaError, parse_timedelta

        try:
            return parse_timedelta(value)
        except ParseTimedeltaError:
            raise _LoadSettingsInvalidTimeDeltaError(
                path=path, values=values, field=field.name, value=value
            ) from None
    if isinstance(type_, type) and issubclass(type_, Enum):
        try:
            return ensure_enum(value, type_)
        except EnsureEnumError:
            raise _LoadSettingsInvalidEnumError(
                path=path, values=values, field=field.name, type_=type_, value=value
            ) from None
    if is_literal_type(type_):
        return one_str(get_args(type_), value, case_sensitive=False)
    if is_optional_type(type_) and (one(get_args(type_)) is int):
        if (value is None) or (value == "") or search("none", value, flags=IGNORECASE):
            return None
        try:
            return int(value)
        except ValueError:
            raise _LoadSettingsInvalidNullableIntError(
                path=path, values=values, field=field.name, value=value
            ) from None
    raise _LoadSettingsTypeError(path=path, field=field.name, type=type_)


@dataclass(kw_only=True, slots=True)
class LoadSettingsError(Exception):
    path: Path


@dataclass(kw_only=True, slots=True)
class _LoadSettingsFileNotFoundError(LoadSettingsError):
    @override
    def __str__(self) -> str:
        return f"Path {str(self.path)!r} must exist"


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
class _LoadSettingsInvalidBoolError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid boolean; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidDateError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid date; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidEnumError(LoadSettingsError):
    values: StrMapping
    field: str
    type_: type[Enum]
    value: str

    @override
    def __str__(self) -> str:
        type_ = get_class_name(self.type_)
        return f"Field {self.field!r} must contain a valid member of {type_!r}; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidFloatError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid float; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidIntError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid integer; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidNullableIntError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid nullable integer; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidTimeDeltaError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return (
            f"Field {self.field!r} must contain a valid timedelta; got {self.value!r}"
        )


@dataclass(kw_only=True, slots=True)
class _LoadSettingsTypeError(LoadSettingsError):
    field: str
    type: Any

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} has unsupported type {self.type!r}"


__all__ = ["LoadSettingsError", "load_settings"]
