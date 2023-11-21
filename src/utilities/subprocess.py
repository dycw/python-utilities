from __future__ import annotations

from collections.abc import Iterator, Mapping
from functools import partial
from itertools import chain, repeat, starmap
from pathlib import Path
from re import MULTILINE, escape, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import Any

from utilities.os import temp_environ
from utilities.pathlib import PathLike
from utilities.typing import IterableStrs


def get_shell_output(
    cmd: str,
    /,
    *,
    cwd: PathLike = Path.cwd(),
    activate: PathLike | None = None,
    env: Mapping[str, str | None] | None = None,
) -> str:
    """Get the output of a shell call.

    Optionally, activate a virtual environment if necessary.
    """
    cwd = Path(cwd)
    if activate is not None:
        activates = list(cwd.rglob("activate"))
        if (n := len(activates)) == 0:
            raise NoActivateError(cwd)
        if n == 1:
            cmd = f"source {activates[0]}; {cmd}"  # pragma: os-ne-windows
        else:
            raise MultipleActivateError(activates)
    with temp_environ(env):
        return check_output(
            cmd,
            stderr=PIPE,
            shell=True,  # noqa: S602
            cwd=cwd,
            text=True,
        )


class NoActivateError(Exception):
    """Raised when no `activate` script can be found."""


class MultipleActivateError(Exception):
    """Raised when multiple `activate` scripts can be found."""


def run_accept_address_in_use(args: IterableStrs, /, *, exist_ok: bool) -> None:
    """Run a command, accepting the 'address already in use' error."""
    try:  # pragma: no cover
        _ = check_output(list(args), stderr=PIPE, text=True)  # noqa: S603
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


def tabulate_called_process_error(error: CalledProcessError, /) -> str:
    """Tabulate the components of a CalledProcessError."""
    mapping = {  # pragma: os-ne-windows
        "cmd": error.cmd,
        "returncode": error.returncode,
        "stdout": error.stdout,
        "stderr": error.stderr,
    }
    max_key_len = max(map(len, mapping))  # pragma: os-ne-windows
    tabulate = partial(_tabulate, buffer=max_key_len + 1)  # pragma: os-ne-windows
    return "\n".join(starmap(tabulate, mapping.items()))  # pragma: os-ne-windows


def _tabulate(key: str, value: Any, /, *, buffer: int) -> str:
    template = f"{{:{buffer}}}{{}}"  # pragma: os-ne-windows

    def yield_lines() -> Iterator[str]:  # pragma: os-ne-windows
        keys = chain([key], repeat(buffer * " "))
        value_lines = str(value).splitlines()
        for k, v in zip(keys, value_lines, strict=False):
            yield template.format(k, v)

    return "\n".join(yield_lines())  # pragma: os-ne-windows


__all__ = [
    "get_shell_output",
    "MultipleActivateError",
    "NoActivateError",
    "run_accept_address_in_use",
    "tabulate_called_process_error",
]
