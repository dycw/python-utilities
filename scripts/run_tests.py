#!/usr/bin/env python
from __future__ import annotations

from logging import getLogger
from pathlib import Path
from shlex import quote
from subprocess import run

from utilities.git import get_repo_root
from utilities.logging import basic_config
from utilities.re import extract_group

_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config(obj=_LOGGER)
    path = get_repo_root().joinpath("src", "tests")
    for path_i in path.glob("test_*.py"):
        _run_test(path_i)


def _run_test(path: Path, /) -> None:
    group = extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")
    path = Path(".pytest_cache", group)
    if path.exists():
        return
    _LOGGER.info("Testing %r...", str(path))
    _ = run(
        [
            "uv",
            "run",
            f"--only-group={quote(group)}",
            "--only-group=hypothesis",
            "--only-group=pytest",
            "--no-dev",
            "--managed-python",
            "pytest",
            "-x",
            "-nauto",
            str(path),
        ],
        check=True,
    )
    path.touch()


if __name__ == "__main__":
    main()
