from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, TypeVar

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import Dataclass
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

    def yield_items() -> Iterator[tuple[str, str]]:
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
                yield fld.name, values[key]

    return cls(**dict(yield_items()))


@dataclass(kw_only=True)
class LoadSettingsError(Exception):
    path: Path


@dataclass(kw_only=True)
class _LoadSettingsFileNotFoundError(LoadSettingsError):
    @override
    def __str__(self) -> str:
        return f"Path {str(self.path)!r} must exist."


@dataclass(kw_only=True)
class _LoadSettingsEmptyError(LoadSettingsError):
    field: str

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist."


@dataclass(kw_only=True)
class _LoadSettingsNonUniqueError(LoadSettingsError):
    field: str
    counts: Mapping[str, int]

    @override
    def __str__(self) -> str:
        return f"Field {self.field!r} must exist exactly once; got {self.counts}."


__all__ = ["LoadSettingsError", "load_settings"]
