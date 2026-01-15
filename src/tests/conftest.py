from __future__ import annotations

from contextlib import AbstractContextManager, suppress
from logging import LogRecord, setLogRecordFactory
from typing import TYPE_CHECKING

from hypothesis import HealthCheck
from pytest import fixture, param, skip
from whenever import PlainDateTime

from utilities.asyncio import sleep
from utilities.constants import MINUTE
from utilities.contextlib import enhanced_context_manager
from utilities.pytest import IS_CI, IS_CI_AND_NOT_LINUX, skipif_ci
from utilities.re import ExtractGroupError, extract_group
from utilities.tempfile import TemporaryDirectory, TemporaryFile
from utilities.whenever import get_now_local_plain

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

    from _pytest.fixtures import SubRequest
    from redis.asyncio import Redis
    from sqlalchemy import Engine, TextClause
    from sqlalchemy.ext.asyncio import AsyncEngine


# hypothesis


try:
    from utilities.hypothesis import setup_hypothesis_profiles
except ModuleNotFoundError:
    pass
else:
    setup_hypothesis_profiles(suppress_health_check={HealthCheck.differing_executors})


# fixtures - docker


@fixture
def container() -> str:
    return "postgres"


# fixtures - logging


@fixture
def set_log_factory() -> AbstractContextManager[None]:
    @enhanced_context_manager
    def cm() -> Iterator[None]:
        try:
            yield
        finally:
            setLogRecordFactory(LogRecord)

    return cm()


# fixtures - pathlib


@fixture
def temp_dir_with_dir_and_file(*, tmp_path: Path) -> Iterator[Path]:
    with (
        TemporaryDirectory(dir=tmp_path) as outer,
        TemporaryDirectory(dir=outer) as inner,
        TemporaryFile(dir=inner) as temp_file,
    ):
        temp_file.touch()
        yield outer


@fixture
def temp_dir_with_file(*, tmp_path: Path) -> Iterator[Path]:
    with (
        TemporaryDirectory(dir=tmp_path) as temp_dir,
        TemporaryFile(dir=temp_dir) as temp_file,
    ):
        temp_file.touch()
        yield temp_dir


@fixture
def temp_dir_with_files(*, tmp_path: Path) -> Iterator[Path]:
    with (
        TemporaryDirectory(dir=tmp_path) as temp_dir,
        TemporaryFile(dir=temp_dir) as temp_file1,
        TemporaryFile(dir=temp_dir) as temp_file2,
    ):
        temp_file1.touch()
        temp_file2.touch()
        yield temp_dir


@fixture
def temp_dir_with_nothing(*, tmp_path: Path) -> Iterator[Path]:
    with TemporaryDirectory(dir=tmp_path) as temp_dir:
        yield temp_dir


@fixture
def temp_dirs(*, tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    with (
        TemporaryDirectory(dir=tmp_path) as temp1,
        TemporaryDirectory(dir=tmp_path) as temp2,
    ):
        yield temp1, temp2


@fixture
def temp_dirs_with_files(
    *, temp_dirs: tuple[Path, Path]
) -> Iterator[tuple[Path, Path]]:
    path1, path2 = temp_dirs
    with TemporaryFile(dir=path1), TemporaryFile(dir=path2):
        yield temp_dirs


@fixture
def temp_file(*, tmp_path: Path) -> Iterator[Path]:
    with TemporaryFile(dir=tmp_path) as temp:
        temp.touch()
        yield temp


@fixture
def temp_files(*, tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    with TemporaryFile(dir=tmp_path) as temp1, TemporaryFile(dir=tmp_path) as temp2:
        temp1.touch()
        temp2.touch()
        yield temp1, temp2


@fixture
def temp_path_not_exist(*, tmp_path: Path) -> Path:
    with TemporaryFile(dir=tmp_path) as temp:
        ...
    return temp


@fixture
def temp_path_nested_not_exist(*, tmp_path: Path, temp_path_not_exist: Path) -> Path:
    return tmp_path / temp_path_not_exist.name / temp_path_not_exist.name


# fixtures - redis


@fixture
async def test_redis() -> AsyncIterator[Redis]:
    if IS_CI_AND_NOT_LINUX:
        skip(reason="Skipped for CI/non-Linux")

    from utilities.redis import yield_redis

    async with yield_redis(db=15) as redis:
        yield redis


# fixtures - sqlalchemy


@fixture(params=[param("sqlite"), param("postgresql", marks=skipif_ci)])
def test_engine(*, request: SubRequest, tmp_path: Path) -> Engine:
    from sqlalchemy.exc import OperationalError

    from utilities.sqlalchemy import create_engine

    dialect = request.param
    match dialect:
        case "sqlite":
            db_path = tmp_path / "db.sqlite"
            return create_engine("sqlite", database=str(db_path))
        case "postgresql":
            engine = create_engine(
                "postgresql+psycopg",
                username="postgres",
                password="password",  # noqa: S106
                host="localhost",
                port=5432,
                database="testing",
            )
            try:
                with engine.begin() as conn:
                    tables: list[str] = list(
                        conn.execute(_select_tables()).scalars().all()
                    )
            except OperationalError:
                ...
            else:
                for table in filter(_is_to_drop, tables):
                    with engine.begin() as conn, suppress(Exception):
                        _ = conn.execute(_drop_table(table))
            return engine
        case _:
            msg = f"Unsupported dialect: {dialect}"
            raise NotImplementedError(msg)


@fixture(params=[param("sqlite"), param("postgresql", marks=skipif_ci)])
async def test_async_engine(
    *,
    request: SubRequest,
    test_async_sqlite_engine: AsyncEngine,
    test_async_postgres_engine: AsyncEngine,
) -> AsyncEngine:
    await sleep()
    dialect = request.param
    match dialect:
        case "sqlite":
            return test_async_sqlite_engine
        case "postgresql":
            return test_async_postgres_engine
        case _:
            msg = f"Unsupported dialect: {dialect}"
            raise NotImplementedError(msg)


@fixture
async def test_async_sqlite_engine(*, tmp_path: Path) -> AsyncEngine:
    from utilities.sqlalchemy import create_engine

    await sleep()
    db_path = tmp_path / "db.sqlite"
    return create_engine("sqlite+aiosqlite", database=str(db_path), async_=True)


@fixture
async def test_async_postgres_engine() -> AsyncEngine:
    from asyncpg.exceptions import InvalidCatalogNameError

    from utilities.sqlalchemy import create_engine

    if IS_CI:
        skip(reason="Skipped for CI")
    engine = create_engine(
        "postgresql+asyncpg",
        username="postgres",
        password="password",  # noqa: S106
        host="localhost",
        port=5432,
        database="testing",
        async_=True,
    )
    try:
        async with engine.begin() as conn:
            tables: list[str] = list(
                (await conn.execute(_select_tables())).scalars().all()
            )
    except InvalidCatalogNameError:
        ...
    else:
        for table in filter(_is_to_drop, tables):
            async with engine.begin() as conn:
                with suppress(Exception):
                    _ = await conn.execute(_drop_table(table))
    return engine


def _is_to_drop(table: str, /) -> bool:
    now = get_now_local_plain()
    try:
        text = extract_group(r"^(\d{8}T\d{2,})_", table)
    except ExtractGroupError:
        return True
    date_time = PlainDateTime.parse_iso(text)
    age = now.difference(date_time, ignore_dst=True)
    return age >= MINUTE


def _select_tables() -> TextClause:
    from sqlalchemy import text

    return text("SELECT tablename FROM pg_tables")


def _drop_table(table: str, /) -> TextClause:
    from sqlalchemy import text

    return text(f'DROP TABLE IF EXISTS "{table}" CASCADE')


# fixtures - subprocess


@fixture
def git_repo_url() -> str:
    return "https://github.com/CogWorksBWSI/GitPracticeRepo"


@fixture
def github_public_key() -> str:
    return "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"


@fixture
def ssh_user() -> str:
    return "root"


@fixture
def ssh_hostname() -> str:
    return "proxmox.main"


@fixture
def ssh_hostname_internal() -> str:
    return "proxmox"
