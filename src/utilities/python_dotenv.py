from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, TypeVar

from dotenv import dotenv_values
from typing_extensions import override

from utilities.dataclasses import Dataclass
from utilities.git import get_repo_root
from utilities.iterables import OneEmptyError, one
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from utilities.types import PathLike

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


def load_settings(cls: type[_TDataclass], /, *, cwd: PathLike = PWD) -> _TDataclass:
    """Load a set of settings from the `.env` file."""
    path = get_repo_root(cwd=cwd).joinpath(".env")
    if not path.exists():
        raise LoadSettingsError(path=path) from None
        msg = f"{str(path)!r} not found"
        raise FileNotFoundError(msg)
    maybe_values = dotenv_values(path)
    values = {k: v for k, v in maybe_values.items() if v is not None}

    def yield_items() -> Iterator[tuple[str, str]]:
        for fld in fields(cls):
            try:
                key, value = one(
                    (fld.name, v)
                    for k, v in values.items()
                    if k.casefold() == fld.name.casefold()
                )
            except OneEmptyError:
                pass
            else:
                yield key, value

    return cls(**dict(yield_items()))


@dataclass(kw_only=True)
class LoadSettingsError(Exception):
    path: Path

    @override
    def __str__(self) -> str:
        return f"Unable to load settings; path {str(self.path)!r} must exist"


__all__ = ["LoadSettingsError", "load_settings"]
