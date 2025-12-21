from __future__ import annotations

import sys
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from string import Template
from subprocess import PIPE, CalledProcessError, Popen
from threading import Thread
from typing import IO, TYPE_CHECKING, Literal, assert_never, overload

from utilities.errors import ImpossibleCaseError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike, StrMapping, StrStrMapping


def echo_cmd(text: str, /) -> list[str]:
    return ["echo", text]


def expand_path(
    path: PathLike, /, *, subs: StrMapping | None = None, sudo: bool = False
) -> Path:
    if subs is not None:
        path = Template(str(path)).substitute(**subs)
    if sudo:  # pragma: no cover
        return Path(run(*sudo_cmd(*echo_cmd(str(path))), return_=True))
    return Path(path).expanduser()


def maybe_sudo_cmd(cmd: str, /, *args: str, sudo: bool = False) -> list[str]:
    parts: list[str] = [cmd, *args]
    return sudo_cmd(*parts) if sudo else parts


def mkdir(path: PathLike, /, *, sudo: bool = False, parent: bool = False) -> None:
    if sudo:  # pragma: no cover
        run(*sudo_cmd(*mkdir_cmd(path, parent=parent)))
    else:
        path = expand_path(path)
        path_use = path.parent if parent else path
        path_use.mkdir(parents=True, exist_ok=True)


def mkdir_cmd(path: PathLike, /, *, parent: bool = False) -> list[str]:
    path_use = f"$(dirname {path})" if parent else path
    return ["mkdir", "-p", str(path_use)]


def rm_cmd(path: PathLike, /) -> list[str]:
    return ["rm", "-rf", str(path)]


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
        match return_code, return_ or return_stdout, return_ or return_stderr:
            case 0, True, True:
                _ = buffer.seek(0)
                return buffer.read()
            case 0, True, False:
                _ = stdout.seek(0)
                return stdout.read()
            case 0, False, True:
                _ = stderr.seek(0)
                return stderr.read()
            case 0, False, False:
                return None
            case _, _, _:
                _ = stdout.seek(0)
                _ = stderr.seek(0)
                raise CalledProcessError(
                    return_code, cmd, output=stdout.read(), stderr=stderr.read()
                )
            case never:
                assert_never(never)


@contextmanager
def _yield_write(input_: IO[str], /, *outputs: IO[str]) -> Iterator[None]:
    thread = Thread(target=_run_target, args=(input_, *outputs), daemon=True)
    thread.start()
    try:
        yield
    finally:
        thread.join()


def _run_target(input_: IO[str], /, *outputs: IO[str]) -> None:
    with input_:
        for line in iter(input_.readline, ""):
            for output in outputs:
                _ = output.write(line)


def sudo_cmd(cmd: str, /, *args: str) -> list[str]:
    return ["sudo", cmd, *args]


def touch_cmd(path: PathLike, /) -> list[str]:
    return ["touch", str(path)]


__all__ = [
    "echo_cmd",
    "maybe_sudo_cmd",
    "mkdir",
    "mkdir_cmd",
    "rm_cmd",
    "run",
    "sudo_cmd",
    "touch_cmd",
]
