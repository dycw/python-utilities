from __future__ import annotations

from dataclasses import fields
from typing import TypeVar

from dotenv import dotenv_values

from utilities.dataclasses import Dataclass
from utilities.git import get_repo_root

_TDataclass = TypeVar("_TDataclass", bound=Dataclass)


def load_settings(cls: type[_TDataclass], /) -> _TDataclass:
    """Load a set of settings from the `.env` file."""
    path = get_repo_root().joinpath(".env")
    if not path.exists():
        msg = "'.env' is missing; please copy from an existing repo or adapt '.env.example'"
        raise FileNotFoundError(msg)
    maybe_values = dotenv_values(get_repo_root().joinpath(".env"))
    values = {k.lower(): v for k, v in maybe_values.items() if v is not None}
    flds = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in values.items() if k in flds})
