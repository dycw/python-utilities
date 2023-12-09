from __future__ import annotations

from collections.abc import Iterator
from os import getenv

from _pytest.fixtures import SubRequest
from loguru import logger
from pytest import LogCaptureFixture, fixture

from utilities.hypothesis import setup_hypothesis_profiles
from utilities.loguru import setup_loguru
from utilities.timer import Timer

setup_hypothesis_profiles()
setup_loguru()


@fixture()
def caplog(*, caplog: LogCaptureFixture) -> Iterator[LogCaptureFixture]:
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


@fixture(autouse=True)
def log_current_test(*, request: SubRequest) -> Iterator[None]:  # noqa: PT004
    """Log current test; usage:

    PYTEST_TIMER=1 pytest -s .
    """
    if getenv("PYTEST_TIMER") == "1":
        name = request.node.nodeid
        logger.info("[S ] {name}", name=name)
        with Timer() as timer:
            yield
        logger.info("[ F] {name} | {timer}", name=name, timer=timer)
    else:
        yield
