from __future__ import annotations

import sys
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from string import Template
from subprocess import PIPE, CalledProcessError, Popen
from threading import Thread
from time import sleep
from typing import IO, TYPE_CHECKING, Literal, assert_never, overload, override

from utilities.errors import ImpossibleCaseError
from utilities.logging import to_logger
from utilities.text import strip_and_dedent
from utilities.whenever import to_seconds

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import LoggerLike, PathLike, Retry, StrMapping, StrStrMapping


_HOST_KEY_ALGORITHMS = ["ssh-ed25519"]
APT_UPDATE = ["apt", "update", "-y"]
BASH_LC = ["bash", "-lc"]
BASH_LS = ["bash", "-ls"]
MKTEMP_DIR_CMD = ["mktemp", "-d"]
RESTART_SSHD = ["systemctl", "restart", "sshd"]
UPDATE_CA_CERTIFICATES: str = "update-ca-certificates"


def apt_install_cmd(package: str, /) -> list[str]:
    return ["apt", "install", "-y", package]


def cat_cmd(path: PathLike, /) -> list[str]:
    return ["cat", str(path)]


def cd_cmd(path: PathLike, /) -> list[str]:
    return ["cd", str(path)]


def chmod_cmd(path: PathLike, mode: str, /) -> list[str]:
    return ["chmod", mode, str(path)]


def chown_cmd(
    path: PathLike, /, *, user: str | None = None, group: str | None = None
) -> list[str]:
    match user, group:
        case None, None:
            raise ChownCmdError
        case str(), None:
            ownership = "user"
        case None, str():
            ownership = f":{group}"
        case str(), str():
            ownership = f"{user}:{group}"
        case never:
            assert_never(never)
    return ["chown", ownership, str(path)]


@dataclass(kw_only=True, slots=True)
class ChownCmdError(Exception):
    @override
    def __str__(self) -> str:
        return "At least one of 'user' and/or 'group' must be given; got None"


def cp_cmd(src: PathLike, dest: PathLike, /) -> list[str]:
    return ["cp", "-r", str(src), str(dest)]


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


def git_clone_cmd(url: str, path: PathLike, /) -> list[str]:
    return ["git", "clone", "--recurse-submodules", url, str(path)]


def git_hard_reset_cmd(*, branch: str | None = None) -> list[str]:
    branch_use = "master" if branch is None else branch
    return ["git", "hard-reset", branch_use]


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


def mv_cmd(src: PathLike, dest: PathLike, /) -> list[str]:
    return ["mv", str(src), str(dest)]


def rm_cmd(path: PathLike, /) -> list[str]:
    return ["rm", "-rf", str(path)]


@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def run(
    cmd: str,
    /,
    *cmds_or_args: str,
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
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    args: list[str] = []
    if user is not None:  # pragma: no cover
        args.extend(["su", "-", str(user)])
    args.extend([cmd, *cmds_or_args])
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
        user=user,
    ) as proc:
        if proc.stdin is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdin=}"])
        if proc.stdout is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stdout=}"])
        if proc.stderr is None:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{proc.stderr=}"])
        with (
            _run_yield_write(proc.stdout, *stdout_outputs),
            _run_yield_write(proc.stderr, *stderr_outputs),
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
                if retry is None:
                    attempts = delta = None
                else:
                    attempts, delta = retry
                _ = stdout.seek(0)
                stdout_text = stdout.read()
                _ = stderr.seek(0)
                stderr_text = stderr.read()
                if logger is not None:
                    msg = strip_and_dedent(f"""
'run' failed with:
 - cmd          = {cmd}
 - cmds_or_args = {cmds_or_args}
 - user         = {user}
 - executable   = {executable}
 - shell        = {shell}
 - cwd          = {cwd}
 - env          = {env}

-- stdin ----------------------------------------------------------------------
{"" if input is None else input}-------------------------------------------------------------------------------
-- stdout ---------------------------------------------------------------------
{stdout_text}-------------------------------------------------------------------------------
-- stderr ---------------------------------------------------------------------
{stderr_text}-------------------------------------------------------------------------------
""")
                    if (attempts is not None) and (attempts >= 1):
                        if delta is None:
                            msg = f"{msg}\n\nRetrying {attempts} more time(s)..."
                        else:
                            msg = f"{msg}\n\nRetrying {attempts} more time(s) after {delta}..."
                    to_logger(logger).error(msg)
                error = CalledProcessError(
                    return_code, args, output=stdout_text, stderr=stderr_text
                )
                if (attempts is None) or (attempts <= 0):
                    raise error
                if delta is not None:
                    sleep(to_seconds(delta))
                return run(
                    cmd,
                    *cmds_or_args,
                    user=user,
                    executable=executable,
                    shell=shell,
                    cwd=cwd,
                    env=env,
                    input=input,
                    print=print,
                    print_stdout=print_stdout,
                    print_stderr=print_stderr,
                    return_=return_,
                    return_stdout=return_stdout,
                    return_stderr=return_stderr,
                    retry=(attempts - 1, delta),
                    logger=logger,
                )
            case never:
                assert_never(never)


@contextmanager
def _run_yield_write(input_: IO[str], /, *outputs: IO[str]) -> Iterator[None]:
    thread = Thread(target=_run_daemon_target, args=(input_, *outputs), daemon=True)
    thread.start()
    try:
        yield
    finally:
        thread.join()


def _run_daemon_target(input_: IO[str], /, *outputs: IO[str]) -> None:
    with input_:
        for text in iter(input_.readline, ""):
            _run_write_to_streams(text, *outputs)


def _run_write_to_streams(text: str, /, *outputs: IO[str]) -> None:
    for output in outputs:
        _ = output.write(text)


def set_hostname_cmd(hostname: str, /) -> list[str]:
    return ["hostnamectl", "set-hostname", hostname]


@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None: ...
def ssh(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
    input: str | None = None,  # noqa: A002
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
) -> str | None:
    cmd_and_args = ssh_cmd(  # skipif-ci
        user,
        hostname,
        *cmd_and_cmds_or_args,
        batch_mode=batch_mode,
        host_key_algorithms=host_key_algorithms,
        strict_host_key_checking=strict_host_key_checking,
    )
    return run(  # skipif-ci
        *cmd_and_args,
        input=input,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        logger=logger,
    )


def ssh_cmd(
    user: str,
    hostname: str,
    /,
    *cmd_and_cmds_or_args: str,
    batch_mode: bool = True,
    host_key_algorithms: list[str] = _HOST_KEY_ALGORITHMS,
    strict_host_key_checking: bool = True,
) -> list[str]:
    args: list[str] = ["ssh"]
    if batch_mode:
        args.extend(["-o", "BatchMode=yes"])
    args.extend(["-o", f"HostKeyAlgorithms={','.join(host_key_algorithms)}"])
    if strict_host_key_checking:
        args.extend(["-o", "StrictHostKeyChecking=yes"])
    return [*args, "-T", f"{user}@{hostname}", *cmd_and_cmds_or_args]


def ssh_keygen_cmd(hostname: str, /) -> list[str]:
    return ["ssh-keygen", "-f", "~/.ssh/known_hosts", "-R", hostname]


def sudo_cmd(cmd: str, /, *args: str) -> list[str]:
    return ["sudo", cmd, *args]


def sudo_nopasswd_cmd(user: str, /) -> str:
    return f"{user} ALL=(ALL) NOPASSWD: ALL"


def symlink_cmd(src: PathLike, dest: PathLike, /) -> list[str]:
    return ["ln", "-s", str(src), str(dest)]


def touch_cmd(path: PathLike, /) -> list[str]:
    return ["touch", str(path)]


def uv_run_cmd(module: str, /, *args: str) -> list[str]:
    return [
        "uv",
        "run",
        "--no-dev",
        "--active",
        "--prerelease=disallow",
        "--managed-python",
        "python",
        "-m",
        module,
        *args,
    ]


@contextmanager
def yield_ssh_temp_dir(
    user: str,
    hostname: str,
    /,
    *,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    keep: bool = False,
) -> Iterator[Path]:
    path = Path(
        ssh(user, hostname, *MKTEMP_DIR_CMD, return_=True, retry=retry, logger=logger)
    )
    try:
        yield path
    finally:
        if keep:
            if logger is not None:
                to_logger(logger).info("Keeping temporary directory '%s'...", path)
        else:
            ssh(user, hostname, *rm_cmd(path), retry=retry, logger=logger)


__all__ = [
    "APT_UPDATE",
    "BASH_LC",
    "BASH_LS",
    "MKTEMP_DIR_CMD",
    "RESTART_SSHD",
    "UPDATE_CA_CERTIFICATES",
    "ChownCmdError",
    "chmod_cmd",
    "chown_cmd",
    "cp_cmd",
    "echo_cmd",
    "expand_path",
    "git_clone_cmd",
    "git_hard_reset_cmd",
    "maybe_sudo_cmd",
    "mkdir",
    "mkdir_cmd",
    "mv_cmd",
    "rm_cmd",
    "run",
    "set_hostname_cmd",
    "ssh",
    "ssh_cmd",
    "sudo_cmd",
    "sudo_nopasswd_cmd",
    "symlink_cmd",
    "touch_cmd",
    "uv_run_cmd",
    "yield_ssh_temp_dir",
]
