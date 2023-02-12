from collections.abc import Iterable
from os import environ

from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.python import Function
from beartype import beartype
from pytest import mark

from utilities.beartype import IterableStrs


@beartype
def add_pytest_addoption(parser: Parser, options: IterableStrs, /) -> None:
    """Add the `--slow`, etc options to pytest.

    Usage:

        def pytest_addoption(parser):
            add_pytest_addoption(parser, ["slow"])
    """
    for opt in options:
        parser.addoption(
            f"--{opt}",
            action="store_true",
            default=False,
            help=f"run tests marked {opt!r}",
        )


@beartype
def add_pytest_collection_modifyitems(
    config: Config,
    items: Iterable[Function],
    options: IterableStrs,
    /,
) -> None:
    """Add the @mark.skips as necessary.

    Usage:

        def pytest_collection_modifyitems(config, items):
            add_pytest_collection_modifyitems(config, items, ["slow"])
    """
    options = list(options)
    missing = {opt for opt in options if not config.getoption(f"--{opt}")}
    for item in items:
        opts_on_item = [opt for opt in options if opt in item.keywords]
        if len(missing & set(opts_on_item)) >= 1:
            flags = [f"--{opt}" for opt in opts_on_item]
            joined = " ".join(flags)
            item.add_marker(mark.skip(reason=f"pass {joined}"))


@beartype
def add_pytest_configure(
    config: Config,
    options: Iterable[tuple[str, str]],
    /,
) -> None:
    """Add the `--slow`, etc markers to pytest.

    Usage:
        def pytest_configure(config):
            add_pytest_configure(config, [("slow", "slow to run")])
    """
    for opt, desc in options:
        config.addinivalue_line("markers", f"{opt}: mark test as {desc}")


@beartype
def is_pytest() -> bool:
    """Check if pytest is currently running."""
    return "PYTEST_CURRENT_TEST" in environ
