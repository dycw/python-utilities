from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload, override

if TYPE_CHECKING:
    from utilities.types import FileOrDir, PathLike


# pathlib


@overload
def file_or_dir(path: PathLike, /, *, exists: Literal[True]) -> FileOrDir: ...
@overload
def file_or_dir(path: PathLike, /, *, exists: bool = False) -> FileOrDir | None: ...
def file_or_dir(path: PathLike, /, *, exists: bool = False) -> FileOrDir | None:
    """Classify a path as a file, directory or non-existent."""
    path = Path(path)
    match path.exists(), path.is_file(), path.is_dir(), exists:
        case True, True, False, _:
            return "file"
        case True, False, True, _:
            return "dir"
        case False, False, False, True:
            raise _FileOrDirMissingError(path=path)
        case False, False, False, False:
            return None
        case _:
            raise _FileOrDirTypeError(path=path)


@dataclass(kw_only=True, slots=True)
class FileOrDirError(Exception):
    path: Path


@dataclass(kw_only=True, slots=True)
class _FileOrDirMissingError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path does not exist: {str(self.path)!r}"


@dataclass(kw_only=True, slots=True)
class _FileOrDirTypeError(FileOrDirError):
    @override
    def __str__(self) -> str:
        return f"Path is neither a file nor a directory: {str(self.path)!r}"


# tempfile


__all__ = ["FileOrDirError", "file_or_dir"]
