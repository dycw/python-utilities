from __future__ import annotations

from os import environ, getenv
from typing import TYPE_CHECKING, Any

from pytest import LogCaptureFixture, fixture, mark

from utilities.timer import Timer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from _pytest.fixtures import SubRequest
    from sqlalchemy import Engine, Table

FLAKY = mark.flaky(reruns=5, reruns_delay=1)
SKIPIF_CI = mark.skipif("CI" in environ, reason="Skipped for CI")


# hypothesis

try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles()


# loguru


try:
    from loguru import logger

    from utilities.loguru import setup_loguru
except ModuleNotFoundError:
    pass
else:
    setup_loguru()

    @fixture()
    def caplog(*, caplog: LogCaptureFixture) -> Iterator[LogCaptureFixture]:
        handler_id = logger.add(caplog.handler, format="{message}")
        yield caplog
        logger.remove(handler_id)

    @fixture(autouse=True)
    def log_current_test(*, request: SubRequest) -> Iterator[None]:  # noqa: PT004
        """Log current test.

        Usage:
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


# sqlalchemy


try:
    pass
except ModuleNotFoundError:
    pass
else:

    @fixture(scope="session")
    def create_postgres_engine() -> Callable[..., Engine]:
        """Create a Postgres engine."""

        def inner(*tables_or_mapped_classes: Table | type[Any]) -> Engine:
            from utilities.sqlalchemy import (
                create_engine,
                ensure_tables_created,
                ensure_tables_dropped,
            )

            engine = create_engine(
                "postgresql", host="localhost", port=5432, database="testing"
            )
            ensure_tables_dropped(engine, *tables_or_mapped_classes)
            ensure_tables_created(engine, *tables_or_mapped_classes)
            return engine

        return inner
