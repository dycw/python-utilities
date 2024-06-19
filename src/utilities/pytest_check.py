from __future__ import annotations

from contextlib import contextmanager
from os import getenv
from typing import TYPE_CHECKING

from pytest_check import check as _check

if TYPE_CHECKING:
    from collections.abc import Iterator


@contextmanager
def check() -> Iterator[None]:
    """Context manager running `pytest_check`, but can be disabled."""
    if getenv("PYTEST_CHECK") == "0":
        yield
    else:
        with _check():
            yield


__all__ = ["check"]
