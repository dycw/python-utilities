from __future__ import annotations

from collections.abc import Iterator
from os import getenv
from typing import TypedDict, cast

import pytest
from _pytest.logging import LogCaptureFixture
from hypothesis import Verbosity, settings
from loguru import logger


class Kwargs(TypedDict, total=False):
    deadline: None
    print_blob: bool
    report_multiple_bugs: bool


kwargs = cast(
    Kwargs, {"deadline": None, "print_blob": True, "report_multiple_bugs": False}
)
settings.register_profile("default", max_examples=100, **kwargs)
settings.register_profile("dev", max_examples=10, **kwargs)
settings.register_profile("ci", max_examples=1000, **kwargs)
settings.register_profile(
    "debug", max_examples=10, verbosity=Verbosity.verbose, **kwargs
)
settings.load_profile(getenv("HYPOTHESIS_PROFILE", "default"))


@pytest.fixture()
def caplog(*, caplog: pytest.LogCaptureFixture) -> Iterator[LogCaptureFixture]:
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)
