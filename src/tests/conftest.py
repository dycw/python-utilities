from __future__ import annotations

from asyncio import sleep
from contextlib import AbstractContextManager, contextmanager, suppress
from logging import LogRecord, setLogRecordFactory
from os import environ
from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck
from pytest import fixture, mark, param
from whenever import PlainDateTime

from utilities.platform import IS_NOT_LINUX, IS_WINDOWS
from utilities.re import ExtractGroupError, extract_group
from utilities.tzlocal import LOCAL_TIME_ZONE_NAME
from utilities.whenever import MINUTE, get_now

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

    from _pytest.fixtures import SubRequest


FLAKY = mark.flaky(reruns=5, reruns_delay=1)
IS_CI = "CI" in environ
SKIPIF_CI = mark.skipif(IS_CI, reason="Skipped for CI")
IS_CI_AND_WINDOWS = IS_CI and IS_WINDOWS
SKIPIF_CI_AND_WINDOWS = mark.skipif(IS_CI_AND_WINDOWS, reason="Skipped for CI/Windows")
SKIPIF_CI_AND_NOT_LINUX = mark.skipif(
    IS_CI and IS_NOT_LINUX, reason="Skipped for CI/non-Linux"
)


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles(suppress_health_check={HealthCheck.differing_executors})


# fixture - logging


@fixture
def set_log_factory() -> AbstractContextManager[None]:
    @contextmanager
    def cm() -> Iterator[None]:
        try:
            yield
        finally:
            setLogRecordFactory(LogRecord)

    return cm()


# fixtures - sqlalchemy


@fixture(params=[param("sqlite"), param("postgresql", marks=SKIPIF_CI)])
async def test_engine(*, request: SubRequest, tmp_path: Path) -> Any:
    from sqlalchemy import text

    from utilities.sqlalchemy import create_async_engine

    dialect = request.param
    match dialect:
        case "sqlite":
            db_path = tmp_path / "db.sqlite"
            return create_async_engine("sqlite+aiosqlite", database=str(db_path))
        case "postgresql":
            engine = create_async_engine(
                "postgresql+asyncpg", host="localhost", port=5432, database="testing"
            )
            query = text("SELECT tablename FROM pg_tables")
            async with engine.begin() as conn:
                tables: Sequence[str] = (await conn.execute(query)).scalars().all()
            for table in tables:
                if _is_to_drop(table):
                    async with engine.begin() as conn:
                        with suppress(Exception):
                            _ = await conn.execute(
                                text(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                            )
                        await sleep(0.01)
            return engine
        case _:
            msg = f"Unsupported dialect: {dialect}"
            raise NotImplementedError(msg)


def _is_to_drop(table: str, /) -> bool:
    try:
        datetime_str = extract_group(r"^(\d{8}T\d{6})_", table)
    except ExtractGroupError:
        return True
    datetime = PlainDateTime.parse_common_iso(datetime_str).assume_tz(
        LOCAL_TIME_ZONE_NAME
    )
    now = get_now()
    return (now - datetime) >= MINUTE
