from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import StringIO
from pathlib import Path
from re import MULTILINE, escape, search
from subprocess import PIPE, CalledProcessError, CompletedProcess, Popen, check_output
from typing import IO, TYPE_CHECKING, TextIO

from utilities.errors import redirect_error
from utilities.functions import ensure_not_none
from utilities.iterables import OneError, one
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
        with redirect_error(OneError, GetShellOutputError(f"{cwd=}")):
            activate = one(cwd.rglob("activate"))
        cmd = f"source {activate}; {cmd}"  # skipif-not-windows

    with temp_environ(env):  # pragma: no cover
        return check_output(cmd, stderr=PIPE, shell=True, cwd=cwd, text=True)  # noqa: S602


class GetShellOutputError(Exception): ...


def run_accept_address_in_use(args: Sequence[str], /, *, exist_ok: bool) -> None:
    """Run a command, accepting the 'address already in use' error."""
    try:  # pragma: no cover
        _ = check_output(list(args), stderr=PIPE, text=True)
    except CalledProcessError as error:  # pragma: no cover
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
    if write_stdout is None:
        from loguru import logger

        write_stdout_use = logger.info
    else:
        write_stdout_use = write_stdout
    if write_stderr is None:
        from loguru import logger

        write_stderr_use = logger.error
    else:
        write_stderr_use = write_stderr

    popen = Popen(args, stdout=PIPE, stderr=PIPE, shell=shell, env=env, text=True)
    stdout_out, stderr_out = StringIO(), StringIO()
    with (
        popen as process,
        ThreadPoolExecutor(2) as pool,  # two threads to handle the streams
    ):
        stdout_in = ensure_not_none(process.stdout)
        stderr_in = ensure_not_none(process.stderr)
        _stream_command_handle(stdout_in, pool, write_stdout_use, stdout_out)
        _stream_command_handle(stderr_in, pool, write_stderr_use, stderr_out)
    retcode = process.wait()
    if retcode == 0:
        return CompletedProcess(
            process.args,
            retcode,
            stdout=stdout_out.getvalue(),
            stderr=stderr_out.getvalue(),
        )
    raise CalledProcessError(retcode, process.args)


def _stream_command_handle(
    buffer_in: IO[str],
    pool: ThreadPoolExecutor,
    write_console: Callable[[str], None],
    buffer_out: TextIO,
    /,
) -> None:
    process = partial(
        _stream_command_write, write_console=write_console, buffer=buffer_out
    )
    _ = deque(pool.submit(process, line.rstrip("\n")) for line in buffer_in if line)


def _stream_command_write(
    line: str, /, *, write_console: Callable[[str], None], buffer: TextIO
) -> None:
    """Write to console and buffer."""
    write_console(line)
    _ = buffer.write(f"{line}\n")


__all__ = [
    "GetShellOutputError",
    "get_shell_output",
    "run_accept_address_in_use",
    "stream_command",
]
