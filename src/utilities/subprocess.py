from __future__ import annotations

import sys
from contextlib import contextmanager
from io import StringIO
from subprocess import PIPE, CalledProcessError, Popen
from threading import Thread
from typing import IO, TYPE_CHECKING, Literal, assert_never, overload

from utilities.errors import ImpossibleCaseError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import StrStrMapping


@overload
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
) -> None: ...
@overload
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
) -> str | None: ...
def run(
    cmd: str,
    /,
    *cmds: str,
    executable: str | None = None,
    shell: bool = False,
    cwd: str | None = None,
    env: StrStrMapping | None = None,
    user: str | int | None = None,
    group: str | int | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
) -> str | None:
    buffer = StringIO()
    stdout = StringIO()
    stderr = StringIO()
    with Popen(
        [cmd, *cmds],
        bufsize=1,
        executable=executable,
        stdout=PIPE,
        stderr=PIPE,
        shell=shell,
        cwd=cwd,
        env=env,
        text=True,
        user=user,
        group=group,
    ) as proc:
        if proc.stdout is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdout=}"])
        if proc.stderr is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stderr=}"])
        with (
            _yield_write(
                proc.stdout,
                buffer,
                stdout,
                *([sys.stdout] if print or print_stdout else []),
            ),
            _yield_write(
                proc.stderr,
                buffer,
                stderr,
                *([sys.stderr] if print or print_stderr else []),
            ),
        ):
            return_code = proc.wait()
        match return_code, return_, return_stdout, return_stderr:
            case (0, True, _, _) | (0, False, True, True):
                _ = buffer.seek(0)
                return buffer.read()
            case 0, False, True, False:
                _ = stdout.seek(0)
                return stdout.read()
            case 0, False, False, True:
                _ = stderr.seek(0)
                return stderr.read()
            case 0, False, False, False:
                return None
            case _, _, _, _:
                _ = stdout.seek(0)
                _ = stderr.seek(0)
                raise CalledProcessError(
                    return_code, cmd, output=stdout.read(), stderr=stderr.read()
                )
            case never:
                assert_never(never)


@contextmanager
def _yield_write(input_: IO[str], /, *outputs: IO[str]) -> Iterator[None]:
    t = Thread(target=_run_target, args=(input_, *outputs))
    t.daemon = True
    t.start()
    try:
        yield
    finally:
        t.join()


def _run_target(infile: IO[str], /, *outputs: IO[str]) -> None:
    with infile:
        for line in iter(infile.readline, ""):
            for f in outputs:
                _ = f.write(line)


__all__ = ["run"]
