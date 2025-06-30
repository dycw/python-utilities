from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from subprocess import check_output
from typing import TYPE_CHECKING, assert_never, overload, override

from utilities.atomicwrites import writer
from utilities.gzip import write_binary

if TYPE_CHECKING:
    from utilities.types import PathLike


##


@overload
def run_prettier(source: bytes, /) -> bytes: ...
@overload
def run_prettier(source: str, /) -> str: ...
@overload
def run_prettier(source: Path, /) -> None: ...
def run_prettier(source: bytes | str | Path, /) -> bytes | str | None:
    """Run `prettier` on a string/path."""
    match source:  # skipif-ci
        case bytes() as data:
            return _run_prettier_core(data, text=False)
        case str() as text:
            if (path := Path(text)).is_file():
                return run_prettier(path)
            return _run_prettier_core(text, text=True)
        case Path() as path:
            result = run_prettier(path.read_bytes())
            with writer(path, overwrite=True) as temp:
                _ = temp.write_bytes(result)
            return None
        case _ as never:
            assert_never(never)


def _run_prettier_core(data: bytes | str, /, *, text: bool) -> bytes | str:
    """Run `prettier` on a string/path."""
    try:  # skipif-ci
        return check_output(["prettier", "--parser=json"], input=data, text=text)
    except FileNotFoundError:  # pragma: no cover
        raise RunPrettierError from None


@dataclass(kw_only=True, slots=True)
class RunPrettierError(Exception):
    @override
    def __str__(self) -> str:
        return "Unable to find 'prettier'"  # pragma: no cover


##


def write_formatted_json(
    data: bytes, path: PathLike, /, *, compress: bool = False, overwrite: bool = False
) -> None:
    """Write a formatted byte string to disk."""
    with suppress(RunPrettierError):
        data = run_prettier(data)
    write_binary(data, path, compress=compress, overwrite=overwrite)


__all__ = ["RunPrettierError", "run_prettier", "write_formatted_json"]
