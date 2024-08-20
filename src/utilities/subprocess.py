from __future__ import annotations

from pathlib import Path
from re import MULTILINE, escape, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import TYPE_CHECKING

from utilities.errors import redirect_error
from utilities.iterables import OneError, one
from utilities.os import temp_environ
from utilities.pathlib import PWD

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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


__all__ = ["GetShellOutputError", "get_shell_output", "run_accept_address_in_use"]
