from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from git import Repo

from utilities.pathlib import to_path

if TYPE_CHECKING:
    from utilities.types import MaybeCallablePathLike


def get_repo(path: MaybeCallablePathLike = Path.cwd, /) -> Repo:
    """Get the repo object."""
    return Repo(to_path(path), search_parent_directories=True)


__all__ = ["get_repo"]
