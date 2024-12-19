from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import partial
from os import environ
from re import IGNORECASE, search
from typing import TYPE_CHECKING, Any, TypeVar

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import (
    _MappingToDataclassCaseInsensitiveBijectionError,
    _MappingToDataclassCaseInsensitiveEmptyError,
    _YieldFieldsClass,
    mapping_to_dataclass,
)
from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import get_class_name
from utilities.git import get_repo_root
from utilities.iterables import merge_str_mappings, one_str
from utilities.pathlib import PWD
from utilities.reprlib import get_repr
from utilities.types import Dataclass
from utilities.typing import get_args, is_literal_type

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from utilities.types import PathLike, StrMapping

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


def load_settings(
    cls: type[_TDataclass],
    /,
    *,
    cwd: PathLike = PWD,
    globalns: StrMapping | None = None,
    localns: StrMapping | None = None,
) -> _TDataclass:
    """Load a set of settings from the `.env` file."""
    path = get_repo_root(cwd=cwd).joinpath(".env")
    if not path.exists():
        raise _LoadSettingsFileNotFoundError(path=path) from None
    maybe_values_dotenv = dotenv_values(path)
    assert 0, maybe_values_dotenv
    maybe_values = merge_str_mappings(
        maybe_values_dotenv, environ, case_sensitive=False
    )
    assert 0
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
    except _MappingToDataclassCaseInsensitiveEmptyError as error:
        raise _LoadSettingsEmptyError(
            path=path, values=error.mapping, field=error.field
        ) from None
    except _MappingToDataclassCaseInsensitiveBijectionError as error:
        raise _LoadSettingsNonUniqueError(
            path=path, values=error.mapping, field=error.field, counts=error.counts
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
    if type_ is int:
        try:
            return int(value)
        except ValueError:
            raise _LoadSettingsInvalidIntError(
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
class _LoadSettingsEmptyError(LoadSettingsError):
    values: StrMapping
    field: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist (case insensitive)"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsNonUniqueError(LoadSettingsError):
    values: StrMapping
    field: str
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Mapping {get_repr(self.values)} must not contain duplicates (case insensitive); got {get_repr(self.counts)}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidBoolError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid boolean; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidIntError(LoadSettingsError):
    values: StrMapping
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid integer; got {self.value!r}"


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
class _LoadSettingsTypeError(LoadSettingsError):
    field: str
    type: Any

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} has unsupported type {self.type!r}"


__all__ = ["LoadSettingsError", "load_settings"]
