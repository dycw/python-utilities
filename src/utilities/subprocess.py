from collections.abc import Iterator, Mapping
from functools import partial
from itertools import chain, repeat, starmap
from pathlib import Path
from re import MULTILINE, escape, search
from subprocess import PIPE, CalledProcessError, check_output
from typing import Any, Optional

from beartype import beartype
from loguru import logger

from utilities.os import temp_environ
from utilities.pathlib import PathLike


@beartype
def get_shell_output(
    cmd: str,
    /,
    *,
    cwd: PathLike = Path.cwd(),
    activate: Optional[PathLike] = None,
    env: Optional[Mapping[str, Optional[str]]] = None,
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
            cmd = f"source {activates[0]}; {cmd}"
        else:
            raise MultipleActivateError(activates)
    with temp_environ(env):
        return check_output(cmd, stderr=PIPE, shell=True, cwd=cwd, text=True)


class NoActivateError(ValueError):
    """Raised when no `activate` script can be found."""


class MultipleActivateError(ValueError):
    """Raised when multiple `activate` scripts can be found."""


@beartype
def run_accept_address_in_use(args: list[str], /, *, exist_ok: bool) -> None:
    """Run a command, accepting the 'address already in use' error."""
    try:  # pragma: no cover
        _ = check_output(args, stderr=PIPE, text=True)
    except CalledProcessError as error:  # pragma: no cover
        pattern = _address_already_in_use_pattern()
        if exist_ok and search(pattern, error.stderr, flags=MULTILINE):
            logger.info("Address already in use")
        else:
            logger.exception("Address already in use")
            raise


@beartype
def _address_already_in_use_pattern() -> str:
    """Get the 'address_already_in_use' pattern."""
    text = "OSError: [Errno 98] Address already in use"
    escaped = escape(text)
    return f"^{escaped}$"


@beartype
def tabulate_called_process_error(error: CalledProcessError, /) -> str:
    """Tabulate the components of a CalledProcessError."""
    mapping = {
        "cmd": error.cmd,
        "returncode": error.returncode,
        "stdout": error.stdout,
        "stderr": error.stderr,
    }
    max_key_len = max(map(len, mapping))
    tabulate = partial(_tabulate, buffer=max_key_len + 1)
    return "\n".join(starmap(tabulate, mapping.items()))


@beartype
def _tabulate(key: str, value: Any, /, *, buffer: int) -> str:
    template = f"{{:{buffer}}}{{}}"

    @beartype
    def yield_lines() -> Iterator[str]:
        keys = chain([key], repeat(buffer * " "))
        value_lines = str(value).splitlines()
        for k, v in zip(keys, value_lines):
            yield template.format(k, v)

    return "\n".join(yield_lines())
