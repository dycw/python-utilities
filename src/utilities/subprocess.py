from collections.abc import Iterator
from collections.abc import Mapping
from functools import partial
from itertools import chain
from itertools import repeat
from itertools import starmap
from pathlib import Path
from subprocess import PIPE  # noqa: S404
from subprocess import CalledProcessError  # noqa: S404
from subprocess import check_output  # noqa: S404
from typing import Any

from beartype import beartype

from utilities.os import temp_environ
from utilities.pathlib import PathLike


@beartype
def get_shell_output(
    cmd: str,
    /,
    *,
    cwd: PathLike = Path.cwd(),
    activate: PathLike | None = None,
    env: Mapping[str, str | None] | None = None,
) -> str:
    """Get the output of a shell call. Activate a virtual environment if
    necessary.
    """

    cwd = Path(cwd)
    if activate is not None:
        activates = list(cwd.rglob("activate"))
        if (n := len(activates)) == 0:
            raise NoActivate(cwd)
        elif n == 1:
            cmd = f"source {activates[0]}; {cmd}"
        else:
            raise MultipleActivate(activates)
    with temp_environ(env):
        return check_output(
            cmd, stderr=PIPE, shell=True, cwd=cwd, text=True  # noqa: S602
        )


class NoActivate(ValueError):
    ...


class MultipleActivate(ValueError):
    ...


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
