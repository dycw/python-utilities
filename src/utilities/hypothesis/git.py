from __future__ import annotations

from subprocess import run

from hypothesis.strategies import DrawFn
from hypothesis.strategies import composite

from utilities.pathlib import temp_cwd
from utilities.tempfile import TemporaryDirectory

from .hypothesis import MaybeSearchStrategy  # noqa: TID252
from .hypothesis import lift_draw  # noqa: TID252
from .hypothesis import temp_dirs  # noqa: TID252


@composite
def git_repos(
    _draw: DrawFn, /, *, branch: MaybeSearchStrategy[str | None] = None
) -> TemporaryDirectory:
    draw = lift_draw(_draw)
    temp_dir = draw(temp_dirs())
    path = temp_dir.path
    with temp_cwd(path):
        _ = run(["git", "init"], check=True)  # noqa: S603, S607
        _ = run(
            ["git", "config", "user.name", "User"],  # noqa: S603, S607
            check=True,
        )
        _ = run(
            ["git", "config", "user.email", "a@z.com"],  # noqa: S603, S607
            check=True,
        )
        file = path.joinpath("file")
        file.touch()
        file_str = file.as_posix()
        _ = run(["git", "add", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "add"], check=True)  # noqa: S603, S607
        _ = run(["git", "rm", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "rm"], check=True)  # noqa: S603, S607
        if (branch := draw(branch)) is not None:
            _ = run(
                ["git", "checkout", "-b", branch],  # noqa: S603, S607
                check=True,
            )
    return temp_dir
