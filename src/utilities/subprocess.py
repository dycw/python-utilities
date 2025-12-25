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
from utilities.logging import to_logger
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import LoggerLike, PathLike, StrMapping, StrStrMapping


_HOST_KEY_ALGORITHMS = ["ssh-ed25519"]
MKTEMP_DIR_CMD = ["mktemp", "-d"]


def bash_cmd_and_args(cmd: str, /, *cmds: str) -> list[str]:
    return ["bash", "-lc", "\n".join([cmd, *cmds])]


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
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> str | None: ...
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
    bash: bool = False,
    user: str | int | None = None,
    executable: str | None = None,
    shell: bool = False,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    input: str | None = None,  # noqa: A002
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> str | None:
    match bash, user:
        case False, user_use:
            args: list[str] = [cmd, *cmds_or_args]
        case True, None:
            args: list[str] = [cmd, *cmds_or_args]
            # args: list[str] = bash_cmd_and_args(cmd, *cmds_or_args)
            # assert 0, args
            user_use = None
        case True, str() | int():  # skipif-ci-or-mac
            args: list[str] = [
                "su",
                "-",
                str(user),
                *bash_cmd_and_args(cmd, *cmds_or_args),
            ]
            user_use = None
        case never:
            assert_never(never)
    buffer = StringIO()
    stdout = StringIO()
    stderr = StringIO()
    stdout_outputs: list[IO[str]] = [buffer, stdout]
    if print or print_stdout:
        stdout_outputs.append(sys.stdout)
    stderr_outputs: list[IO[str]] = [buffer, stderr]
    if print or print_stderr:
        stderr_outputs.append(sys.stderr)
    with Popen(
        args,
        bufsize=1,
        executable=executable,
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        shell=shell,
        cwd=cwd,
        env=env,
        text=True,
        user=user_use,
    ) as proc:
        if proc.stdin is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdin=}"])
        if proc.stdout is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdout=}"])
        if proc.stderr is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stderr=}"])
        with (
            _yield_write(proc.stdout, *stdout_outputs, desc="proc.stdout"),
            _yield_write(proc.stderr, *stderr_outputs, desc="proc.stderr"),
        ):
            if input is not None:
                _ = proc.stdin.write(input)
                proc.stdin.flush()
                proc.stdin.close()
            return_code = proc.wait()
        match return_code, return_ or return_stdout, return_ or return_stderr:
            case 0, True, True:
                _ = buffer.seek(0)
                return buffer.read().rstrip("\n")
            case 0, True, False:
                _ = stdout.seek(0)
                return stdout.read().rstrip("\n")
            case 0, False, True:
                _ = stderr.seek(0)
                return stderr.read().rstrip("\n")
            case 0, False, False:
                return None
            case _, _, _:
                _ = stdout.seek(0)
                stdout_text = stdout.read()
                _ = stderr.seek(0)
                stderr_text = stderr.read()
                if logger is not None:
                    msg = strip_and_dedent(f"""
'run' failed with:
 - cmd          = {cmd}
 - cmds_or_args = {cmds_or_args}
 - bash         = {bash}
 - user         = {user}
 - executable   = {executable}
 - shell        = {shell}
 - cwd          = {cwd}
 - env          = {env}
 - input        = {input}

-- stdout ---------------------------------------------------------------------
{stdout_text}-------------------------------------------------------------------------------
-- stderr ---------------------------------------------------------------------
{stderr_text}-------------------------------------------------------------------------------
""")
                    to_logger(logger).error(msg)
                raise CalledProcessError(
                    return_code, args, output=stdout_text, stderr=stderr_text
                )
            case never:
                assert_never(never)


@contextmanager
def _yield_write(
    input_: IO[str], /, *outputs: IO[str], desc: str | None = None
) -> Iterator[None]:
    thread = Thread(
        target=_run_target, args=(input_, *outputs), kwargs={"desc": desc}, daemon=True
    )
    thread.start()
    try:
        yield
    finally:
        thread.join()


def _run_target(input_: IO[str], /, *outputs: IO[str], desc: str | None = None) -> None:
    try:
        with input_:
            for text in iter(input_.readline, ""):
                _write_to_streams(text, *outputs)
    except ValueError:
        if desc is None:
            raise
        _ = sys.stderr.write(f"Failed to write to {desc!r}...")
        raise


def _write_to_streams(text: str, /, *outputs: IO[str]) -> None:
    for output in outputs:
        _ = output.write(text)


def ssh_cmd(
    user: str,
    hostname: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    bash: bool = False,
) -> list[str]:
    args: list[str] = ["ssh", "-T"]
    if batch_mode:
        args.extend(["-o", "BatchMode=yes"])
    args.extend(["-o", f"HostKeyAlgorithms={','.join(host_key_algorithms)}"])
    if strict_host_key_checking:
        args.extend(["-o", "StrictHostKeyChecking=yes"])
    args.append(f"{user}@{hostname}")
    if bash:
        args.extend(bash_cmd_and_args(cmd, *cmds_or_args))
    else:
        args.extend([cmd, *cmds_or_args])
    return args


def sudo_cmd(cmd: str, /, *args: str) -> list[str]:
    return ["sudo", cmd, *args]


def touch_cmd(path: PathLike, /) -> list[str]:
    return ["touch", str(path)]


__all__ = [
    "MKTEMP_DIR_CMD",
    "bash_cmd_and_args",
    "echo_cmd",
    "expand_path",
    "maybe_sudo_cmd",
    "mkdir",
    "mkdir_cmd",
    "rm_cmd",
    "run",
    "ssh_cmd",
    "sudo_cmd",
    "touch_cmd",
]
