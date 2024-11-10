from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar, get_type_hints

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import Dataclass
from utilities.enum import EnsureEnumError, ensure_enum
from utilities.functions import get_class_name
from utilities.git import get_repo_root
from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    one_str,
)
from utilities.pathlib import PWD
from utilities.typing import get_args, is_literal_type

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
    hints = get_type_hints(
        cls,
        globalns=globals() if globalns is None else dict(globalns),
        localns=locals() if localns is None else dict(localns),
    )
    path = get_repo_root(cwd=cwd).joinpath(".env")
    if not path.exists():
        raise _LoadSettingsFileNotFoundError(path=path) from None
    maybe_values = dotenv_values(path)
    values = {k: v for k, v in maybe_values.items() if v is not None}

    def yield_items() -> Iterator[tuple[str, Any]]:
        for fld in fields(cls):
            type_ = hints[fld.name]
            try:
                key = one_str(values, fld.name, case_sensitive=False)
            except _OneStrCaseInsensitiveEmptyError:
                raise _LoadSettingsEmptyError(path=path, field=fld.name) from None
            except _OneStrCaseInsensitiveBijectionError as error:
                raise _LoadSettingsNonUniqueError(
                    path=path, field=fld.name, counts=error.counts
                ) from None
            raw_value = values[key]
            if type_ is str:
                value = raw_value
            elif type_ is int:
                try:
                    value = int(raw_value)
                except ValueError:
                    raise _LoadSettingsInvalidIntError(
                        path=path, field=fld.name, value=raw_value
                    ) from None
            elif isinstance(type_, type) and issubclass(type_, Enum):
                try:
                    value = ensure_enum(raw_value, type_)
                except EnsureEnumError:
                    raise _LoadSettingsInvalidEnumError(
                        path=path, field=fld.name, type_=type_, value=raw_value
                    ) from None
            elif is_literal_type(type_):
                value = one_str(get_args(type_), raw_value, case_sensitive=False)
            else:
                raise _LoadSettingsTypeError(path=path, field=fld.name, type=type_)
            yield fld.name, value

    return cls(**dict(yield_items()))


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
