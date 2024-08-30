from __future__ import annotations

from os import environ
from typing import TYPE_CHECKING

from pytest import fixture, mark

from utilities.platform import IS_NOT_LINUX, IS_WINDOWS
from utilities.sqlalchemy import ensure_tables_dropped_async

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

    from utilities.asyncio import Coroutine1
    from utilities.sqlalchemy import TableOrMappedClass

FLAKY = mark.flaky(reruns=5, reruns_delay=1)
SKIPIF_CI = mark.skipif("CI" in environ, reason="Skipped for CI")
SKIPIF_CI_AND_WINDOWS = mark.skipif(
    ("CI" in environ) and IS_WINDOWS, reason="Skipped for CI/Windows"
)
SKIPIF_CI_AND_NOT_LINUX = mark.skipif(
    ("CI" in environ) and IS_NOT_LINUX, reason="Skipped for CI/non-Linux"
)


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles()


# sqlalchemy


try:
    pass
except ModuleNotFoundError:
    pass
else:

    @fixture(scope="session")
    def create_postgres_engine() -> Callable[..., Engine]:
        """Create a Postgres engine."""

        def inner(*tables_or_mapped_classes: TableOrMappedClass) -> Engine:
            from utilities.sqlalchemy import create_engine, ensure_tables_dropped

            engine = create_engine(
                "postgresql", host="localhost", port=5432, database="testing"
            )
            ensure_tables_dropped(engine, *tables_or_mapped_classes)
            return engine

        return inner

    @fixture(scope="session")
    def create_postgres_engine_async() -> Callable[..., Coroutine1[AsyncEngine]]:
        """Create a Postgres engine."""

        async def inner(*tables_or_mapped_classes: TableOrMappedClass) -> AsyncEngine:
            from utilities.sqlalchemy import create_engine

            engine = create_engine(
                "postgresql+asyncpg",
                host="localhost",
                port=5432,
                database="testing",
                async_=True,
            )
            await ensure_tables_dropped_async(engine, *tables_or_mapped_classes)
            return engine

        return inner
