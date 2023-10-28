#!/usr/bin/env python
from __future__ import annotations

import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import cache
from itertools import chain
from logging import info
from pathlib import Path
from random import shuffle
from re import findall
from subprocess import run
from sys import stdlib_module_names

from tomli import loads

SRC = Path("src")
sys.path.insert(0, str(SRC.resolve()))

from utilities.logging import basic_config  # noqa: E402
from utilities.tempfile import TemporaryDirectory  # noqa: E402

basic_config()


class Kind(Enum):
    standard_library = auto()
    third_party = auto()
    skip = auto()


def identify(module: str, /) -> Kind:
    mapping = {
        Kind.standard_library: stdlib_module_names
        | {
            "class_name",
            "errors",
            "git",
            "iterables",
            "modules",
            "sentinel",
            "text",
            "timer",
        },
        Kind.third_party: {
            "airium",
            "atomicwrites",
            "attrs",
            "beartype",
            "bottleneck",
            "click",
            "cryptography",
            "cvxpy",
            "fastapi",
            "fastparquet",
            "fpdf2",
            "hatch",
            "holoviews",
            "hypothesis",
            "loguru",
            "luigi",
            "memory_profiler",
            "more_itertools",
            "numbagg",
            "numpy",
            "pandas",
            "pqdm",
            "pyinstrument",
            "pytest_check",
            "pytest",
            "scipy",
            "semver",
            "sqlalchemy",
            "tqdm",
            "typed_settings",
            "xarray",
            "zarr",
        },
        Kind.skip: {"clean_dir", "monitor_memory", "pypi_server", "server"},
    }
    try:
        (kind,) = {
            kind: modules for kind, modules in mapping.items() if module in modules
        }
    except ValueError:
        msg = f"Unable to identify the kind of {module!r}"
        raise TypeError(msg) from None
    return kind


def path_to_dependencies(path: Path, /) -> set[str]:
    deps: set[str] = set()
    if path.parts[-2:] == ("sqlalchemy", "test_sqlalchemy.py"):
        deps.add("sqlalchemy-dbs")
    return deps


@cache
def get_optional_dependencies() -> set[str]:
    with Path("pyproject.toml").open() as fh:
        contents = fh.read()
    return set(loads(contents)["project"]["optional-dependencies"])


def module_to_dependency(module: str, /) -> str:
    dependencies = get_optional_dependencies()
    try:
        (dep,) = {dep for dep in dependencies if dep.replace("-", "_") == module}
    except ValueError:
        msg = f"Missing dependency for {module!r}"
        raise TypeError(msg) from None
    return dep


@dataclass
class Unit:
    path: Path
    dependencies: set[str] = field(default_factory=set)


def yield_units() -> Iterator[Unit]:
    tests = SRC.joinpath("tests")
    for path in tests.rglob("**/test_*.py"):
        (module,) = findall(r"^test_(\w+)$", path.stem)
        parts = list(chain(path.relative_to(tests).parts[:-1], [module]))
        mapping = {part: identify(part) for part in parts}
        if all(kind is not Kind.skip for kind in mapping.values()):
            dependencies = path_to_dependencies(path) | {
                module_to_dependency(module)
                for module, kind in mapping.items()
                if kind is Kind.third_party
            }
            yield Unit(path=path, dependencies=dependencies)


def test_unit(unit: Unit, /) -> None:
    path, deps = unit.path, unit.dependencies
    if len(deps) >= 1:
        info("Testing %s with %s...", path, ",".join(sorted(deps)))
    else:
        info("Testing %s...", path)
    with TemporaryDirectory() as temp:
        requirements = temp.joinpath("requirements.txt")
        cmd = [
            "pip-compile",
            "--allow-unsafe",
            "--extra=test",
            f"--output-file={requirements}",
            "--quiet",
            "--upgrade",
        ]
        for extra in deps:
            cmd.extend([f"--extra={extra}"])
        _ = run(cmd, check=True)  # noqa: S603
        _ = run(["pip-sync", requirements], check=True)  # noqa: S603, S607
        _ = run(["pytest", "--no-cov", path], check=True)  # noqa: S603, S607


units = list(yield_units())
shuffle(units)
for unit in units:
    test_unit(unit)
