from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from typing import cast

from airium import Airium
from beartype import beartype


@contextmanager
@beartype
def yield_airium() -> Iterator[Airium]:
    """Yield an `Airium` object with the docstyle set to HTML."""
    airium = Airium()
    airium("<!DOCTYPE html>")
    with cast(Any, airium).html().body():
        yield airium
