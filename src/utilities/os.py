from __future__ import annotations

import shutil
from contextlib import suppress
from dataclasses import dataclass
from os import environ, getenv
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.constants import CPU_COUNT
from utilities.contextlib import enhanced_context_manager
from utilities.iterables import OneStrEmptyError, one_str

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from utilities.types import PathLike


##


def copy(src: PathLike, dest: PathLike, /, *, overwrite: bool = False) -> None:
    """Copy/replace a file/directory atomically."""
    try:
        _move_or_copy(src, dest, overwrite=overwrite, delete_src=False)
    except _MoveOrCopySourceNotFoundError as error:
        raise _CopySourceNotFoundError(src=error.src) from None
    except _MoveOrCopyDestinationExistsError as error:
        raise _CopyDestinationExistsError(src=error.src, dest=error.dest) from None


@dataclass(kw_only=True, slots=True)
class CopyError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _CopySourceNotFoundError(CopyError):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {str(self.src)!r} does not exist"


@dataclass(kw_only=True, slots=True)
class _CopyDestinationExistsError(CopyError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot copy {str(self.src)!r} as destination {str(self.dest)!r} already exists"


def move(src: PathLike, dest: PathLike, /, *, overwrite: bool = False) -> None:
    """Move/replace a file/directory atomically."""
    try:
        _move_or_copy(src, dest, overwrite=overwrite, delete_src=True)
    except _MoveOrCopySourceNotFoundError as error:
        raise _MoveSourceNotFoundError(src=error.src) from None
    except _MoveOrCopyDestinationExistsError as error:
        raise _MoveDestinationExistsError(src=error.src, dest=error.dest) from None


@dataclass(kw_only=True, slots=True)
class MoveError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _MoveSourceNotFoundError(MoveError):
    src: Path

    @override
    def __str__(self) -> str:
        return f"Source {str(self.src)!r} does not exist"


@dataclass(kw_only=True, slots=True)
class _MoveDestinationExistsError(MoveError):
    src: Path
    dest: Path

    @override
    def __str__(self) -> str:
        return f"Cannot move {str(self.src)!r} as destination {str(self.dest)!r} already exists"


def _move_or_copy(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    overwrite: bool = False,
    delete_src: bool = False,
) -> None:
    src, dest = map(Path, [src, dest])
    if not src.exists():
        raise _MoveOrCopySourceNotFoundError(src=src)
    if dest.exists() and not overwrite:
        raise _MoveOrCopyDestinationExistsError(src=src, dest=dest)
    if src.is_file():
        _move_or_copy_file(src, dest, overwrite=overwrite, delete_src=delete_src)
    elif src.is_dir():
        _move_or_copy_dir(src, dest, overwrite=overwrite, delete_src=delete_src)
    else:  # pragma: no cover
        raise TypeError(src)


def _move_or_copy_file(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    overwrite: bool = False,
    delete_src: bool = False,
) -> None:
    src, dest = map(Path, [src, dest])
    name, dir_ = dest.name, dest.parent
    if (not dest.exists()) or (dest.is_file() and overwrite):
        ...
    elif dest.is_dir() and overwrite:
        rmtree(dest, ignore_errors=True)
    else:  # pragma: no cover
        raise RuntimeError(dest, overwrite)
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=dir_) as temp_dir:
        temp_file = Path(temp_dir, src.name)
        _ = shutil.copyfile(src, temp_file)
        _ = temp_file.replace(dest)
    if delete_src:
        src.unlink(missing_ok=True)


def _move_or_copy_dir(
    src: PathLike,
    dest: PathLike,
    /,
    *,
    overwrite: bool = False,
    delete_src: bool = False,
) -> None:
    src, dest = map(Path, [src, dest])
    name, dir_ = dest.name, dest.parent
    if (not dest.exists()) or (dest.is_dir() and overwrite):
        ...
    elif dest.is_file() and overwrite:
        dest.unlink(missing_ok=True)
    else:  # pragma: no cover
        raise RuntimeError(dest, overwrite)
    with TemporaryDirectory(suffix=".tmp", prefix=name, dir=dir_) as temp_dir:
        temp_file = Path(temp_dir, src.name)
        _ = shutil.copyfile(src, temp_file)
        _ = temp_file.replace(dest)
    if delete_src:
        rmtree(src, ignore_errors=True)


@dataclass(kw_only=True, slots=True)
class _MoveOrCopyError(Exception): ...


@dataclass(kw_only=True, slots=True)
class _MoveOrCopySourceNotFoundError(_MoveOrCopyError):
    src: Path


@dataclass(kw_only=True, slots=True)
class _MoveOrCopyDestinationExistsError(_MoveOrCopyError):
    src: Path
    dest: Path


##


type IntOrAll = int | Literal["all"]


def get_cpu_use(*, n: IntOrAll = "all") -> int:
    """Resolve for the number of CPUs to use."""
    match n:
        case int():
            if n >= 1:
                return n
            raise GetCPUUseError(n=n)
        case "all":
            return CPU_COUNT
        case never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class GetCPUUseError(Exception):
    n: int

    @override
    def __str__(self) -> str:
        return f"Invalid number of CPUs to use: {self.n}"


##


@overload
def get_env_var(
    key: str, /, *, case_sensitive: bool = False, default: str, nullable: bool = False
) -> str: ...
@overload
def get_env_var(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: None = None,
    nullable: Literal[False] = False,
) -> str: ...
@overload
def get_env_var(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: str | None = None,
    nullable: bool = False,
) -> str | None: ...
def get_env_var(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: str | None = None,
    nullable: bool = False,
) -> str | None:
    """Get an environment variable."""
    try:
        key_use = one_str(environ, key, case_sensitive=case_sensitive)
    except OneStrEmptyError:
        match default, nullable:
            case None, False:
                raise GetEnvVarError(key=key, case_sensitive=case_sensitive) from None
            case None, True:
                return None
            case str(), _:
                return default
            case never:
                assert_never(never)
    return environ[key_use]


@dataclass(kw_only=True, slots=True)
class GetEnvVarError(Exception):
    key: str
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"No environment variable {self.key!r}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


##


def is_debug() -> bool:
    """Check if we are in `DEBUG` mode."""
    return get_env_var("DEBUG", nullable=True) is not None


##


def is_pytest() -> bool:
    """Check if `pytest` is running."""
    return get_env_var("PYTEST_VERSION", nullable=True) is not None


##


@enhanced_context_manager
def temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    mapping: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    prev = {key: getenv(key) for key in mapping}

    def apply(mapping: Mapping[str, str | None], /) -> None:
        for key, value in mapping.items():
            if value is None:
                with suppress(KeyError):
                    del environ[key]
            else:
                environ[key] = value

    apply(mapping)
    try:
        yield
    finally:
        apply(prev)


__all__ = [
    "CopyError",
    "GetCPUUseError",
    "IntOrAll",
    "MoveError",
    "copy",
    "get_cpu_use",
    "get_env_var",
    "is_debug",
    "is_pytest",
    "move",
    "temp_environ",
]
