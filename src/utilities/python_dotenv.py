from __future__ import annotations

from dataclasses import dataclass, fields
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import Dataclass
from utilities.enum import ensure_enum
from utilities.git import get_repo_root
from utilities.iterables import (
    _OneStrCaseInsensitiveBijectionError,
    _OneStrCaseInsensitiveEmptyError,
    one_str,
)
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from pathlib import Path

    from utilities.types import PathLike

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


def load_settings(cls: type[_TDataclass], /, *, cwd: PathLike = PWD) -> _TDataclass:
    """Load a set of settings from the `.env` file."""
    path = get_repo_root(cwd=cwd).joinpath(".env")
    if not path.exists():
        raise _LoadSettingsFileNotFoundError(path=path) from None
    maybe_values = dotenv_values(path)
    values = {k: v for k, v in maybe_values.items() if v is not None}

    def yield_items() -> Iterator[tuple[str, Any]]:
        for fld in fields(cls):
            try:
                key = one_str(values, fld.name, case_sensitive=False)
            except _OneStrCaseInsensitiveEmptyError:
                raise _LoadSettingsEmptyError(path=path, field=fld.name) from None
            except _OneStrCaseInsensitiveBijectionError as error:
                raise _LoadSettingsNonUniqueError(
                    path=path, field=fld.name, counts=error.counts
                ) from None
            else:
                raw_value = values[key]
                if fld.type is str:
                    value = raw_value
                elif fld.type is int:
                    value = int(raw_value)
                elif isinstance(fld.type, type) and issubclass(fld.type, Enum):
                    value = ensure_enum(raw_value, fld.type)
                else:
                    raise _LoadSettingsTypeError(
                        path=path, field=fld.name, type=fld.type
                    )
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
class _LoadSettingsTypeError(LoadSettingsError):
    field: str
    type: Any

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} has unsupported type {self.type!r}"


__all__ = ["LoadSettingsError", "load_settings"]
