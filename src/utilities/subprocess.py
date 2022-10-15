from collections.abc import Mapping
from pathlib import Path
from subprocess import PIPE  # noqa: S404
from subprocess import check_output  # noqa: S404

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
