#!/usr/bin/env python

from __future__ import annotations

from logging import getLogger
from pathlib import Path
from shlex import quote
from subprocess import check_call
from time import sleep

from utilities.git import get_repo_root
from utilities.logging import basic_config
from utilities.re import extract_group

_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config(obj=_LOGGER)
    path = get_repo_root().joinpath("src", "tests")
    for path_i in sorted(path.glob("test_*.py")):
        _run_test(path_i)


def _run_test(path: Path, /) -> None:
    group = extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")
    marker = Path(".pytest_cache", group)
    if marker.exists():
        return
    _LOGGER.info("Testing %r...", str(path))
    while True:
        code = check_call([
            "uv",
            "run",
            f"--only-group={quote(group)}",
            "--only-group=core",
            "--only-group=hypothesis",
            "--only-group=pytest",
            "--isolated",
            "--managed-python",
            "pytest",
            "-nauto",
            str(path),
        ])
        if code == 0:
            marker.touch()
            return
        sleep(1)


if __name__ == "__main__":
    main()
