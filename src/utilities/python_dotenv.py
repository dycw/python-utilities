from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from os import environ
from re import IGNORECASE, search
from typing import TYPE_CHECKING, Any, TypeVar

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import yield_fields
from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import get_class_name
from utilities.git import get_repo_root
from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    one_str,
)
from utilities.pathlib import PWD
from utilities.types import Dataclass
from utilities.typing import get_args, get_type_hints, is_literal_type

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
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
    maybe_values = {**dotenv_values(path), **environ}
    values = {k: v for k, v in maybe_values.items() if v is not None}
    return cls(**dict(yield_items()))


def _load_settings_yield_items() -> Iterator[tuple[str, Any]]:
    for field in yield_fields(cls, globalns=globalns, localns=localns):
        field.type_
        try:
            key = one_str(values, field.name, case_sensitive=False)
        except _OneStrCaseInsensitiveEmptyError:
            raise _LoadSettingsEmptyError(path=path, field=field.name) from None
        except _OneStrCaseInsensitiveBijectionError as error:
            raise _LoadSettingsNonUniqueError(
                path=path, field=field.name, counts=error.counts
            ) from None
        raw_value = values[key]
        if field.type_ is str:
            value = raw_value
        elif field.type_ is bool:
            if raw_value == "0" or search("false", raw_value, flags=IGNORECASE):
                value = False
            elif raw_value == "1" or search("true", raw_value, flags=IGNORECASE):
                value = True
            else:
                raise _LoadSettingsInvalidBoolError(
                    path=path, field=field.name, value=raw_value
                )
        elif field.type_ is int:
            try:
                value = int(raw_value)
            except ValueError:
                raise _LoadSettingsInvalidIntError(
                    path=path, field=field.name, value=raw_value
                ) from None
        elif isinstance(field.type_, type) and issubclass(field.type_, Enum):
            try:
                value = ensure_enum(raw_value, field.type_)
            except EnsureEnumError:
                raise _LoadSettingsInvalidEnumError(
                    path=path, field=field.name, type_=field.type_, value=raw_value
                ) from None
        elif is_literal_type(field.type_):
            value = one_str(get_args(field.type_), raw_value, case_sensitive=False)
        else:
            raise _LoadSettingsTypeError(path=path, field=field.name, type=field.type_)
        yield field.name, value


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
    field: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsNonUniqueError(LoadSettingsError):
    field: str
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist exactly once; got {self.counts}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidBoolError(LoadSettingsError):
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid boolean; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidIntError(LoadSettingsError):
    field: str
    value: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must contain a valid integer; got {self.value!r}"


@dataclass(kw_only=True, slots=True)
class _LoadSettingsInvalidEnumError(LoadSettingsError):
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
