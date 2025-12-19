from __future__ import annotations

from functools import partial
from subprocess import PIPE, CalledProcessError, Popen, check_call, check_output
from typing import TYPE_CHECKING, Literal, assert_never, overload

if TYPE_CHECKING:
    from utilities.types import PathLike, StrStrMapping


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
    Popen(
        cmd,
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
    )
    cmd_use = bash_cmd(
        cmd,
        *cmds,
        user=user,
        env=env,
        cd=cd,
        bashrc=bashrc,
        homebrew=homebrew,
        direnv=direnv,
    )
    if capture:
        func = partial(check_output, text=True)
    else:
        func = partial(check_call, stdout=None if show else PIPE)
    if retry is not None:
        attempts, wait = retry
        func = tenacity.retry(
            stop=stop_after_attempt(attempts),
            wait=wait_fixed(wait),
            retry=retry_if_exception_type(CalledProcessError),
            reraise=True,
        )(func)
    match func(cmd_use, shell=True, stderr=None if show else PIPE):  # noqa: S604
        case int():
            return None
        case str() as text:
            return text.rstrip("\n")
        case never:
            assert_never(never)
    return None
