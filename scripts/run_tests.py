#!/usr/bin/env python
from __future__ import annotations

import datetime as dt
from logging import getLogger
from subprocess import CalledProcessError, check_call
from tomllib import TOMLDecodeError, loads
from typing import TYPE_CHECKING

from utilities.logging import basic_config
from utilities.os import temp_environ
from utilities.pathlib import get_repo_root
from utilities.re import extract_group
from utilities.time import sleep

if TYPE_CHECKING:
    from pathlib import Path


_LOGGER = getLogger(__name__)


def main() -> None:
    basic_config(obj=_LOGGER)
    path = get_repo_root().joinpath("src", "tests")
    for path_i in sorted(path.glob("test_*.py")):
        _run_test(path_i)


def _run_test(path: Path, /) -> None:
    group = _get_group(path)
    marker = _get_marker(group)
    if marker.exists():
        return
    _LOGGER.info("Testing %r...", str(path))
    while True:
        if _run_command(path):
            marker.touch()
            return
        sleep(1)


def _get_group(path: Path, /) -> str:
    return extract_group(r"^test_(\w+)$", path.stem).replace("_", "-")


def _get_marker(group: str, /) -> Path:
    hour = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
    return get_repo_root().joinpath(".pytest_cache", f"{hour:%Y%m%dT%H}-{group}")


def _run_command(path: Path, /) -> bool:
    cmd: list[str] = [
        "uv",
        "run",
        "--only-group=core",
        "--only-group=hypothesis",
        "--only-group=pytest",
        "--isolated",
        "--managed-python",
    ]
    text = get_repo_root().joinpath("pyproject.toml").read_text()
    try:
        loaded = loads(text)
    except TOMLDecodeError:
        _LOGGER.exception("Invalid TOML document")
        return False
    groups: list[str] = loaded["dependency-groups"]
    if (group := _get_group(path)) in groups:
        cmd.append(f"--only-group={group}")
    if (test := f"{group}-test") in groups:
        cmd.append(f"--only-group={test}")
    cmd.extend(["pytest", "-nauto", str(path)])
    with temp_environ(PYTEST_ADDOPTS=None):
        try:
            code = check_call(cmd)
        except CalledProcessError:
            return False
    if code == 0:
        return True
    _LOGGER.error("pytest failed")
    return False


if __name__ == "__main__":
    main()
