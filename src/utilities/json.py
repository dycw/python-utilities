from __future__ import annotations

from pathlib import Path
from subprocess import check_output
from typing import assert_never, overload

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
            return check_output(["prettier", "--parser=json"], input=data)
        case str() as text:
            if (path := Path(text)).is_file():
                return run_prettier(path)
            return check_output(["prettier", "--parser=json"], input=text, text=True)
        case Path() as path:
            result = run_prettier(path.read_bytes())
            with writer(path, overwrite=True) as temp:
                _ = temp.write_bytes(result)
            return None
        case _ as never:
            assert_never(never)


__all__ = ["run_prettier"]
