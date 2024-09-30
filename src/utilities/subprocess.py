from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from re import MULTILINE, escape, search
from subprocess import PIPE, CalledProcessError, CompletedProcess, Popen, check_output
from typing import IO, TYPE_CHECKING, TextIO

from typing_extensions import override

from utilities.functions import ensure_not_none
from utilities.iterables import OneEmptyError, OneNonUniqueError, one
from utilities.os import temp_environ
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from utilities.types import PathLike


def get_shell_output(
    cmd: str,
    /,
    *,
    cwd: PathLike = PWD,
    activate: PathLike | None = None,
    env: Mapping[str, str | None] | None = None,
) -> str:
    """Get the output of a shell call.

    Optionally, activate a virtual environment if necessary.
    """
    cwd = Path(cwd)
    if activate is not None:
        try:
            activate = one(cwd.rglob("activate"))
        except OneEmptyError:
            raise _GetShellOutputEmptyError(cwd=cwd) from None
        except OneNonUniqueError as error:
            raise _GetShellOutputNonUniqueError(
                cwd=cwd, first=error.first, second=error.second
            ) from None
        cmd = f"source {activate}; {cmd}"  # skipif-not-windows

    with temp_environ(env):  # skipif-not-windows
        return check_output(cmd, stderr=PIPE, shell=True, cwd=cwd, text=True)  # noqa: S602


@dataclass(kw_only=True, slots=True)
class GetShellOutputError(Exception):
    cwd: Path


@dataclass(kw_only=True, slots=True)
class _GetShellOutputEmptyError(GetShellOutputError):
    @override
    def __str__(self) -> str:
        return f"Path {str(self.cwd)!r} contains no 'activate' file"


@dataclass(kw_only=True, slots=True)
class _GetShellOutputNonUniqueError(GetShellOutputError):
    first: Path
    second: Path

    @override
    def __str__(self) -> str:
        return f"Path {str(self.cwd)!r} must contain exactly one 'activate' file; got {str(self.first)!r}, {str(self.second)!r} and perhaps more"


def run_accept_address_in_use(args: Sequence[str], /, *, exist_ok: bool) -> None:
    """Run a command, accepting the 'address already in use' error."""
    try:  # skipif-not-windows
        _ = check_output(list(args), stderr=PIPE, text=True)
    except CalledProcessError as error:  # skipif-not-windows
        pattern = _address_already_in_use_pattern()
        try:
            from loguru import logger
        except ModuleNotFoundError:
            info = exception = print
        else:
            info = logger.info
            exception = logger.exception
        if exist_ok and search(pattern, error.stderr, flags=MULTILINE):
            info("Address already in use")
        else:
            exception("Address already in use")
            raise


def _address_already_in_use_pattern() -> str:
    """Get the 'address_already_in_use' pattern."""
    text = "OSError: [Errno 98] Address already in use"
    escaped = escape(text)
    return f"^{escaped}$"


def stream_command(
    args: str | list[str],
    /,
    *,
    shell: bool = False,
    env: Mapping[str, str] | None = None,
    write_stdout: Callable[[str], None] | None = None,
    write_stderr: Callable[[str], None] | None = None,
) -> CompletedProcess[str]:
    """Mimic subprocess.run, while processing the command output in real time."""
    if write_stdout is None:  # skipif-not-windows
        from loguru import logger

        write_stdout_use = logger.info
    else:  # skipif-not-windows
        write_stdout_use = write_stdout
    if write_stderr is None:  # skipif-not-windows
        from loguru import logger

        write_stderr_use = logger.error
    else:  # skipif-not-windows
        write_stderr_use = write_stderr

    popen = Popen(  # skipif-not-windows
        args, stdout=PIPE, stderr=PIPE, shell=shell, env=env, text=True
    )
    buffer_stdout, buffer_stderr = StringIO(), StringIO()  # skipif-not-windows
    with (  # skipif-not-windows
        popen as process,
        ThreadPoolExecutor(2) as pool,  # two threads to handle the streams
    ):
        _ = pool.submit(
            _stream_command_write,
            ensure_not_none(process.stdout),
            write_stdout_use,
            buffer_stdout,
        )
        _ = pool.submit(
            _stream_command_write,
            ensure_not_none(process.stderr),
            write_stderr_use,
            buffer_stderr,
        )

    retcode = ensure_not_none(process.poll())  # skipif-not-windows
    if retcode == 0:  # skipif-not-windows
        return CompletedProcess(
            process.args,
            retcode,
            stdout=buffer_stdout.getvalue(),
            stderr=buffer_stderr.getvalue(),
        )
    raise CalledProcessError(  # skipif-not-windows
        retcode,
        process.args,
        output=buffer_stdout.getvalue(),
        stderr=buffer_stderr.getvalue(),
    )


def _stream_command_write(
    stream: IO[str], write_console: Callable[[str], None], buffer: TextIO, /
) -> None:
    """Write to console and buffer."""
    for line in stream:  # skipif-not-windows
        stripped = line.rstrip()
        write_console(stripped)
        _ = buffer.write(f"{stripped}\n")


__all__ = [
    "GetShellOutputError",
    "get_shell_output",
    "run_accept_address_in_use",
    "stream_command",
]
