from __future__ import annotations

from pathlib import Path
from subprocess import run

from hypothesis.strategies import DrawFn, composite

from utilities.hypothesis.hypothesis import MaybeSearchStrategy, lift_draw, temp_paths
from utilities.pathlib import temp_cwd


@composite
def git_repos(
    _draw: DrawFn, /, *, branch: MaybeSearchStrategy[str | None] = None
) -> Path:
    draw = lift_draw(_draw)
    path = draw(temp_paths())
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
        file_str = str(file)
        _ = run(["git", "add", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "add"], check=True)  # noqa: S603, S607
        _ = run(["git", "rm", file_str], check=True)  # noqa: S603, S607
        _ = run(["git", "commit", "-m", "rm"], check=True)  # noqa: S603, S607
        if (branch := draw(branch)) is not None:
            _ = run(
                ["git", "checkout", "-b", branch],  # noqa: S603, S607
                check=True,
            )
    return path


__all__ = ["git_repos"]
