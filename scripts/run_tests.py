#!/usr/bin/env python
from __future__ import annotations

from logging import getLogger
from re import search
from shlex import quote
from subprocess import check_output, run
from typing import TYPE_CHECKING

from utilities.git import get_repo_root
from utilities.logging import basic_config
from utilities.re import extract_group

if TYPE_CHECKING:
    from pathlib import Path

_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config(obj=_LOGGER)
    path = get_repo_root().joinpath("src", "tests")
    for path_i in path.glob("test_*.py"):
        _run_test(path_i)


def _run_test(path: Path, /) -> None:
    group = extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")
    if not search("aiolimiter", path.name):
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
    if 0:
        check_output(
            f"uv sync --group={quote(group)} --active --no-dev --managed-python",
            shell=True,
        )


if __name__ == "__main__":
    main()
