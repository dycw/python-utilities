from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from subprocess import PIPE, CalledProcessError, CompletedProcess, Popen
from typing import IO, TYPE_CHECKING, TextIO

from utilities.functions import ensure_not_none

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


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


__all__ = ["stream_command"]
