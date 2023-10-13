#!/usr/bin/env python
from __future__ import annotations

from logging import info
from pathlib import Path
from subprocess import run
from sys import path

from tomli import loads

path.insert(0, Path("src").resolve().as_posix())

from utilities.logging import basic_config  # noqa: E402

basic_config()


with Path("pyproject.toml").open() as fh:
    contents = fh.read()
groups: set[str] = set(loads(contents)["project"]["optional-dependencies"]) - {
    "dev",
    "test",
}


for group in groups:
    info("Compiling group %r...", group)
    requirements = f"requirements/{group}.txt"
    _ = run(
        [  # noqa: S603, S607
            "pip-compile",
            "--allow-unsafe",
            f"--extra={group}",
            "--extra=test",
            f"--output-file={requirements}",
            "--quiet",
            "--upgrade",
        ],
        check=True,
    )
    _ = run(["pip-sync", requirements], check=True)  # noqa: S603, S607
    module = f"test_{group.replace('-', '_')}.py"
    path = Path("src", "tests", module).as_posix()
    _ = run(["pytest", "--no-cov", path], check=True)  # noqa: S603, S607
