from __future__ import annotations

import re
import reprlib
import shutil
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from functools import _lru_cache_wrapper, partial
from itertools import chain
from os import chdir, environ, getenv
from pathlib import Path
from re import findall
from tempfile import NamedTemporaryFile as _NamedTemporaryFile
from types import (
    BuiltinFunctionType,
    FunctionType,
    MethodDescriptorType,
    MethodType,
    MethodWrapperType,
    WrapperDescriptorType,
)
from typing import TYPE_CHECKING, Any, Literal, assert_never, cast, overload, override
from warnings import catch_warnings, filterwarnings

from typing_extensions import TypeIs

from utilities.constants import (
    RICH_EXPAND_ALL,
    RICH_INDENT_SIZE,
    RICH_MAX_DEPTH,
    RICH_MAX_LENGTH,
    RICH_MAX_STRING,
    RICH_MAX_WIDTH,
    Sentinel,
    sentinel,
)
from utilities.types import SupportsRichComparison

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from types import TracebackType

    from utilities.types import FileOrDir, MaybeIterable, PathLike


###############################################################################
#### builtins #################################################################
###############################################################################


@overload
def get_class[T](obj: type[T], /) -> type[T]: ...
@overload
def get_class[T](obj: T, /) -> type[T]: ...
def get_class[T](obj: T | type[T], /) -> type[T]:
    """Get the class of an object, unless it is already a class."""
    return obj if isinstance(obj, type) else type(obj)


def get_class_name(obj: Any, /, *, qual: bool = False) -> str:
    """Get the name of the class of an object, unless it is already a class."""
    cls = get_class(obj)
    return f"{cls.__module__}.{cls.__qualname__}" if qual else cls.__name__


##


def get_func_name(obj: Callable[..., Any], /) -> str:
    """Get the name of a callable."""
    if isinstance(obj, BuiltinFunctionType):
        return obj.__name__
    if isinstance(obj, FunctionType):
        name = obj.__name__
        pattern = r"^.+\.([A-Z]\w+\." + name + ")$"
        try:
            (full_name,) = findall(pattern, obj.__qualname__)
        except ValueError:
            return name
        return full_name
    if isinstance(obj, MethodType):
        return f"{get_class_name(obj.__self__)}.{obj.__name__}"
    if isinstance(
        obj,
        MethodType | MethodDescriptorType | MethodWrapperType | WrapperDescriptorType,
    ):
        return obj.__qualname__
    if isinstance(obj, _lru_cache_wrapper):
        return cast("Any", obj).__name__
    if isinstance(obj, partial):
        return get_func_name(obj.func)
    return get_class_name(obj)


##


@overload
def min_nullable[T: SupportsRichComparison](
    iterable: Iterable[T | None], /, *, default: Sentinel = ...
) -> T: ...
@overload
def min_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U = ...
) -> T | U: ...
def min_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U | Sentinel = sentinel
) -> T | U:
    """Compute the minimum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if is_sentinel(default):
        try:
            return min(values)
        except ValueError:
            raise MinNullableError(iterable=iterable) from None
    return min(values, default=default)


@dataclass(kw_only=True, slots=True)
class MinNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Minimum of {repr_(self.iterable)} is undefined"


@overload
def max_nullable[T: SupportsRichComparison](
    iterable: Iterable[T | None], /, *, default: Sentinel = ...
) -> T: ...
@overload
def max_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U = ...
) -> T | U: ...
def max_nullable[T: SupportsRichComparison, U](
    iterable: Iterable[T | None], /, *, default: U | Sentinel = sentinel
) -> T | U:
    """Compute the maximum of a set of values; ignoring nulls."""
    values = (i for i in iterable if i is not None)
    if is_sentinel(default):
        try:
            return max(values)
        except ValueError:
            raise MaxNullableError(iterable=iterable) from None
    return max(values, default=default)


@dataclass(kw_only=True, slots=True)
class MaxNullableError[T: SupportsRichComparison](Exception):
    iterable: Iterable[T | None]

    @override
    def __str__(self) -> str:
        return f"Maximum of {repr_(self.iterable)} is undefined"


###############################################################################
#### constants ################################################################
###############################################################################


def is_none(obj: Any, /) -> TypeIs[None]:
    """Check if an object is `None`."""
    return obj is None


def is_not_none(obj: Any, /) -> bool:
    """Check if an object is not `None`."""
    return obj is not None


##


def is_sentinel(obj: Any, /) -> TypeIs[Sentinel]:
    """Check if an object is the sentinel."""
    return obj is sentinel


###############################################################################
#### contextlib ###############################################################
###############################################################################


@contextmanager
def suppress_super_attribute_error() -> Iterator[None]:
    """Suppress the super() attribute error, for mix-ins."""
    try:
        yield
    except AttributeError as error:
        if not _suppress_super_attribute_error_pattern.search(error.args[0]):
            raise


_suppress_super_attribute_error_pattern = re.compile(
    r"'super' object has no attribute '\w+'"
)


###############################################################################
#### functions ################################################################
###############################################################################


def identity[T](obj: T, /) -> T:
    """Return the object itself."""
    return obj
##

@overload
def first[T](tup: tuple[T], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any, Any], /) -> T: ...
@overload
def first[T](tup: tuple[T, Any, Any, Any], /) -> T: ...
def first(tup: tuple[Any, ...], /) -> Any:
    """Get the first element in a tuple."""
    return tup[0]


@overload
def second[T](tup: tuple[Any, T], /) -> T: ...
@overload
def second[T](tup: tuple[Any, T, Any], /) -> T: ...
@overload
def second[T](tup: tuple[Any, T, Any, Any], /) -> T: ...
def second(tup: tuple[Any, ...], /) -> Any:
    """Get the second element in a tuple."""
    return tup[1]


@overload
def last[T](tup: tuple[T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, Any, T], /) -> T: ...
@overload
def last[T](tup: tuple[Any, Any, Any, T], /) -> T: ...
def last(objtupuple[Any, ...], /) -> Any:
    """Get the last element in a tuple."""
    return tup[-1]


###############################################################################
#### itertools ################################################################
###############################################################################


def always_iterable[T](obj: MaybeIterable[T], /) -> Iterable[T]:
    """Typed version of `always_iterable`."""
    obj = cast("Any", obj)
    if isinstance(obj, str | bytes):
        return cast("list[T]", [obj])
    try:
        return iter(cast("Iterable[T]", obj))
    except TypeError:
        return cast("list[T]", [obj])


def one[T](*iterables: Iterable[T]) -> T:
    """Return the unique value in a set of iterables."""
    it = chain(*iterables)
    try:
        first = next(it)
    except StopIteration:
        raise OneEmptyError(iterables=iterables) from None
    try:
        second = next(it)
    except StopIteration:
        return first
    raise OneNonUniqueError(iterables=iterables, first=first, second=second)


@dataclass(kw_only=True, slots=True)
class OneError[T](Exception):
    iterables: tuple[Iterable[T], ...]


@dataclass(kw_only=True, slots=True)
class OneEmptyError[T](OneError[T]):
    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must not be empty"


@dataclass(kw_only=True, slots=True)
class OneNonUniqueError[T](OneError):
    first: T
    second: T

    @override
    def __str__(self) -> str:
        return f"Iterable(s) {repr_(self.iterables)} must contain exactly one item; got {self.first}, {self.second} and perhaps more"


##


def one_str(
    iterable: Iterable[str],
    text: str,
    /,
    *,
    head: bool = False,
    case_sensitive: bool = False,
) -> str:
    """Find the unique string in an iterable."""
    as_list = list(iterable)
    match head, case_sensitive:
        case False, True:
            it = (t for t in as_list if t == text)
        case False, False:
            it = (t for t in as_list if t.lower() == text.lower())
        case True, True:
            it = (t for t in as_list if t.startswith(text))
        case True, False:
            it = (t for t in as_list if t.lower().startswith(text.lower()))
        case never:
            assert_never(never)
    try:
        return one(it)
    except OneEmptyError:
        raise OneStrEmptyError(
            iterable=as_list, text=text, head=head, case_sensitive=case_sensitive
        ) from None
    except OneNonUniqueError as error:
        raise OneStrNonUniqueError(
            iterable=as_list,
            text=text,
            head=head,
            case_sensitive=case_sensitive,
            first=error.first,
            second=error.second,
        ) from None


@dataclass(kw_only=True, slots=True)
class OneStrError(Exception):
    iterable: Iterable[str]
    text: str
    head: bool = False
    case_sensitive: bool = False


@dataclass(kw_only=True, slots=True)
class OneStrEmptyError(OneStrError):
    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} does not contain"
        match self.head, self.case_sensitive:
            case False, True:
                tail = repr(self.text)
            case False, False:
                tail = f"{repr_(self.text)} (modulo case)"
            case True, True:
                tail = f"any string starting with {repr_(self.text)}"
            case True, False:
                tail = f"any string starting with {repr_(self.text)} (modulo case)"
            case never:
                assert_never(never)
        return f"{head} {tail}"


@dataclass(kw_only=True, slots=True)
class OneStrNonUniqueError(OneStrError):
    first: str
    second: str

    @override
    def __str__(self) -> str:
        head = f"Iterable {repr_(self.iterable)} must contain"
        match self.head, self.case_sensitive:
            case False, True:
                mid = f"{repr_(self.text)} exactly once"
            case False, False:
                mid = f"{repr_(self.text)} exactly once (modulo case)"
            case True, True:
                mid = f"exactly one string starting with {repr_(self.text)}"
            case True, False:
                mid = (
                    f"exactly one string starting with {repr_(self.text)} (modulo case)"
                )
            case never:
                assert_never(never)
        return f"{head} {mid}; got {repr_(self.first)}, {repr_(self.second)} and perhaps more"


###############################################################################
#### os #######################################################################
###############################################################################


@overload
def get_env(
    key: str, /, *, case_sensitive: bool = False, default: str, nullable: bool = False
) -> str: ...
@overload
def get_env(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: None = None,
    nullable: Literal[False] = False,
) -> str: ...
@overload
def get_env(
    key: str,
    /,
    *,
    case_sensitive: bool = False,
    default: str | None = None,
    nullable: bool = False,
) -> str | None: ...
def get_env(
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
                raise GetEnvError(key=key, case_sensitive=case_sensitive) from None
            case None, True:
                return None
            case str(), _:
                return default
            case never:
                assert_never(never)
    return environ[key_use]


@dataclass(kw_only=True, slots=True)
class GetEnvError(Exception):
    key: str
    case_sensitive: bool = False

    @override
    def __str__(self) -> str:
        desc = f"No environment variable {repr_(self.key)}"
        return desc if self.case_sensitive else f"{desc} (modulo case)"


##


@contextmanager
def yield_temp_environ(
    env: Mapping[str, str | None] | None = None, **env_kwargs: str | None
) -> Iterator[None]:
    """Context manager with temporary environment variable set."""
    mapping: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    prev = {key: getenv(key) for key in mapping}
    _yield_temp_environ_apply(mapping)
    try:
        yield
    finally:
        _yield_temp_environ_apply(prev)


def _yield_temp_environ_apply(mapping: Mapping[str, str | None], /) -> None:
    for key, value in mapping.items():
        if value is None:
            with suppress(KeyError):
                del environ[key]
        else:
            environ[key] = value


###############################################################################
#### pathlib ##################################################################
###############################################################################


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


##


@contextmanager
def yield_temp_cwd(path: PathLike, /) -> Iterator[None]:
    """Context manager with temporary current working directory set."""
    prev = Path.cwd()
    chdir(path)
    try:
        yield
    finally:
        chdir(prev)


###############################################################################
#### reprlib ##################################################################
###############################################################################


def repr_(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the representation of an object."""
    try:
        from rich.pretty import pretty_repr
    except ModuleNotFoundError:  # pragma: no cover
        return reprlib.repr(obj)
    return pretty_repr(
        obj,
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


##


def repr_str(
    obj: Any,
    /,
    *,
    max_width: int = RICH_MAX_WIDTH,
    indent_size: int = RICH_INDENT_SIZE,
    max_length: int | None = RICH_MAX_LENGTH,
    max_string: int | None = RICH_MAX_STRING,
    max_depth: int | None = RICH_MAX_DEPTH,
    expand_all: bool = RICH_EXPAND_ALL,
) -> str:
    """Get the representation of the string of an object."""
    return repr_(
        str(obj),
        max_width=max_width,
        indent_size=indent_size,
        max_length=max_length,
        max_string=max_string,
        max_depth=max_depth,
        expand_all=expand_all,
    )


###############################################################################
#### tempfile #################################################################
###############################################################################


class TemporaryDirectory:
    """Wrapper around `TemporaryDirectory` with a `Path` attribute."""

    def __init__(
        self,
        *,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: PathLike | None = None,  # noqa: A002
        ignore_cleanup_errors: bool = False,
        delete: bool = True,
    ) -> None:
        super().__init__()
        self._temp_dir = _TemporaryDirectoryNoResourceWarning(
            suffix=suffix,
            prefix=prefix,
            dir=dir,
            ignore_cleanup_errors=ignore_cleanup_errors,
            delete=delete,
        )
        self.path = Path(self._temp_dir.name)

    def __enter__(self) -> Path:
        return Path(self._temp_dir.__enter__())

    def __exit__(
        self,
        exc: type[BaseException] | None,
        val: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._temp_dir.__exit__(exc, val, tb)


class _TemporaryDirectoryNoResourceWarning(tempfile.TemporaryDirectory):
    @classmethod
    @override
    def _cleanup(  # pyright: ignore[reportGeneralTypeIssues]
        cls,
        name: str,
        warn_message: str,
        ignore_errors: bool = False,
        delete: bool = True,
    ) -> None:
        with catch_warnings():
            filterwarnings("ignore", category=ResourceWarning)
            return super()._cleanup(  # pyright: ignore[reportAttributeAccessIssue]
                name, warn_message, ignore_errors=ignore_errors, delete=delete
            )


##


@contextmanager
def TemporaryFile(  # noqa: N802
    *,
    dir: PathLike | None = None,  # noqa: A002
    suffix: str | None = None,
    prefix: str | None = None,
    ignore_cleanup_errors: bool = False,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    """Yield a temporary file."""
    if dir is None:
        with (
            TemporaryDirectory(
                suffix=suffix,
                prefix=prefix,
                dir=dir,
                ignore_cleanup_errors=ignore_cleanup_errors,
                delete=delete,
            ) as temp_dir,
            _temporary_file_outer(
                temp_dir,
                suffix=suffix,
                prefix=prefix,
                delete=delete,
                name=name,
                data=data,
                text=text,
            ) as temp,
        ):
            yield temp
    else:
        with _temporary_file_outer(
            dir,
            suffix=suffix,
            prefix=prefix,
            delete=delete,
            name=name,
            data=data,
            text=text,
        ) as temp:
            yield temp


@contextmanager
def _temporary_file_outer(
    path: PathLike,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
    data: bytes | None = None,
    text: str | None = None,
) -> Iterator[Path]:
    with _temporary_file_inner(
        path, suffix=suffix, prefix=prefix, delete=delete, name=name
    ) as temp:
        if data is not None:
            _ = temp.write_bytes(data)
        if text is not None:
            _ = temp.write_text(text)
        yield temp


@contextmanager
def _temporary_file_inner(
    path: PathLike,
    /,
    *,
    suffix: str | None = None,
    prefix: str | None = None,
    delete: bool = True,
    name: str | None = None,
) -> Iterator[Path]:
    path = Path(path)
    with _NamedTemporaryFile(
        suffix=suffix, prefix=prefix, dir=path, delete=delete, delete_on_close=False
    ) as temp:
        if name is None:
            yield Path(path, temp.name)
        else:
            _ = shutil.move(path / temp.name, path / name)
            yield path / name


##


@contextmanager
def yield_temp_dir_at(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary dir for a target path."""

    path = Path(path)
    with TemporaryDirectory(suffix=".tmp", prefix=path.name, dir=path.parent) as temp:
        yield temp


@contextmanager
def yield_temp_file_at(path: PathLike, /) -> Iterator[Path]:
    """Yield a temporary file for a target path."""

    path = Path(path)
    with TemporaryFile(dir=path.parent, suffix=".tmp", prefix=path.name) as temp:
        yield temp


###############################################################################
#### text #####################################################################
###############################################################################

__all__ = [
    "FileOrDirError",
    "GetEnvError",
    "MaxNullableError",
    "MinNullableError",
    "OneEmptyError",
    "OneError",
    "OneNonUniqueError",
    "OneStrEmptyError",
    "OneStrError",
    "OneStrNonUniqueError",
    "TemporaryDirectory",
    "TemporaryFile",
    "always_iterable",
    "file_or_dir",
    "get_class",
    "get_class_name",
    "get_env",
    "get_func_name",
    "is_none",
    "is_not_none",
    "is_sentinel",
    "max_nullable",
    "min_nullable",
    "one",
    "one_str",
    "repr_",
    "repr_str",
    "suppress_super_attribute_error",
    "yield_temp_cwd",
    "yield_temp_dir_at",
    "yield_temp_environ",
    "yield_temp_file_at",
]
