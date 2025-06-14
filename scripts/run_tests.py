#!/usr/bin/env python
from __future__ import annotations

import datetime as dt
from logging import getLogger
from subprocess import CalledProcessError, check_call
from time import sleep
from tomllib import loads
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
    for path_i in sorted(path.glob("test_*.py")):
        _run_test(path_i)


def _run_test(path: Path, /) -> None:
    root = get_repo_root()
    hour = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
    group = extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")
    marker = root.joinpath(".pytest_cache", f"{hour:%Y%m%dT%H}-{group}")
    if (group == "pytest") or marker.exists():
        return
    _LOGGER.info("Testing %r...", str(path))
    cmd: list[str] = [
        "uv",
        "run",
        "--only-group=core",
        "--only-group=hypothesis",
        "--only-group=pytest",
        "--isolated",
        "--managed-python",
    ]
    while True:
        cmd_use: list[str] = cmd.copy()
        groups: list[str] = loads(root.joinpath("pyproject.toml").read_text())[
            "dependency-groups"
        ]
        if group in groups:
            cmd_use.append(f"--only-group={group}")
        if (test := f"{group}-test") in groups:
            cmd_use.append(f"--only-group={test}")
        cmd_use.extend(["pytest", "-nauto", str(path)])
        try:
            code = check_call(cmd_use)
        except CalledProcessError:
            sleep(1)
        else:
            if code == 0:
                marker.touch()
                return
            sleep(1)


if __name__ == "__main__":
    main()
