from __future__ import annotations

import sys
from io import StringIO
from subprocess import PIPE, CalledProcessError, Popen
from typing import TYPE_CHECKING, Literal, assert_never, overload

from utilities.errors import ImpossibleCaseError

if TYPE_CHECKING:
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
    capture: Literal[True],
    capture_stdout: bool = False,
    capture_stderr: bool = False,
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
    capture: bool = False,
    capture_stdout: Literal[True],
    capture_stderr: bool = False,
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
    capture: bool = False,
    capture_stdout: bool = False,
    capture_stderr: Literal[True],
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
    capture: Literal[False] = False,
    capture_stdout: Literal[False] = False,
    capture_stderr: Literal[False] = False,
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
    capture: bool = False,
    capture_stdout: bool = False,
    capture_stderr: bool = False,
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
    capture: bool = False,
    capture_stdout: bool = False,
    capture_stderr: bool = False,
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
        if proc.stdout is None:
            raise ImpossibleCaseError(case=[f"{proc.stdout=}"])
        if proc.stderr is None:
            raise ImpossibleCaseError(case=[f"{proc.stderr=}"])
        while True:
            stdout_i = proc.stdout.readline()
            stderr_i = proc.stderr.readline()
            if (proc.poll() is not None) and (stdout_i == "") and (stderr_i == ""):
                return_code = proc.wait()
                match return_code, capture, capture_stdout, capture_stderr:
                    case (0, True, _, _) | (0, False, True, True):
                        return buffer.read()
                    case 0, False, True, False:
                        return stdout.read()
                    case 0, False, False, True:
                        return stderr.read()
                    case 0, False, False, False:
                        return None
                    case _, _, _, _:
                        raise CalledProcessError(
                            return_code, cmd, output=stdout.read(), stderr=stderr.read()
                        )
                    case never:
                        assert_never(never)
            if stdout_i != "":
                _ = buffer.write(stdout_i)
                _ = stdout.write(stdout_i)
                if print or print_stdout:
                    _ = sys.stdout.write(stdout_i)
                    _ = sys.stdout.flush()
            if stderr_i != "":
                _ = buffer.write(stderr_i)
                _ = stderr.write(stderr_i)
                if print or print_stderr:
                    _ = sys.stderr.write(stderr_i)
                    _ = sys.stderr.flush()


__all__ = ["run"]
