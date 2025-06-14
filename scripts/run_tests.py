#!/usr/bin/env python
from __future__ import annotations

from logging import getLogger
from pathlib import Path
from shlex import quote
from subprocess import CalledProcessError, check_call
from time import sleep
from tomllib import loads
from typing import TYPE_CHECKING

from utilities.git import get_repo_root
from utilities.logging import basic_config
from utilities.re import extract_group

if TYPE_CHECKING:
    from collections.abc import Collection

_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config(obj=_LOGGER)
    root = get_repo_root()
    groups: list[str] = loads(root.joinpath("pyproject.toml").read_text())[
        "dependency-groups"
    ]
    path = root.joinpath("src", "tests")
    for path_i in sorted(path.glob("test_*.py")):
        _run_test(path_i, groups)


def _run_test(path: Path, groups: Collection[str], /) -> None:
    group = extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")
    marker = Path(".pytest_cache", group)
    if marker.exists():
        return
    _LOGGER.info("Testing %r...", str(path))
    cmd: list[str] = [
        "uv",
        "run",
        f"--only-group={quote(group)}",
        "--only-group=core",
        "--only-group=hypothesis",
        "--only-group=pytest",
        "--isolated",
        "--managed-python",
    ]
    if group in groups:
        cmd.append(f"--only-group={group}")
    if (test := f"{group}-test") in groups:
        cmd.append(f"--only-group={test}")
    cmd.extend(["pytest", "-nauto", str(path)])
    while True:
        try:
            code = check_call(cmd)
        except CalledProcessError:
            sleep(1)
        else:
            if code == 0:
                marker.touch()
                return
            sleep(1)


if __name__ == "__main__":
    main()
