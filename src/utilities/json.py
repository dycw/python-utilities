from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from subprocess import check_output
from typing import assert_never, overload, override

from utilities.atomicwrites import writer


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
    except FileNotFoundError:
        raise RunPrettierError from None


@dataclass(kw_only=True, slots=True)
class RunPrettierError(Exception):
    @override
    def __str__(self) -> str:
        return "Unable to find 'prettier'"  # pragma: no cover


__all__ = ["RunPrettierError", "run_prettier"]
