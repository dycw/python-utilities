from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DrawFn, booleans, composite, lists, none, sampled_from
from pytest import raises
from sqlalchemy import URL, Column, Integer, MetaData, Table

from utilities.hypothesis import integers, temp_paths, text_ascii
from utilities.postgres import (
    _PGDumpDatabaseError,
    _PGDumpFormat,
    _PGDumpHostError,
    _PGDumpPortError,
    _PGRestoreDatabaseError,
    _PGRestoreHostError,
    _PGRestorePortError,
    pg_dump,
    pg_restore,
)
from utilities.typing import get_literal_elements

if TYPE_CHECKING:
    from pathlib import Path


@composite
def tables(draw: DrawFn, /) -> list[Table]:
    metadata = MetaData()
    tables = draw(lists(text_ascii(min_size=1), max_size=5, unique=True))
    return [Table(t, metadata, Column("id", Integer, primary_key=True)) for t in tables]


@composite
def urls(draw: DrawFn, /) -> URL:
    username = draw(text_ascii(min_size=1) | none())
    password = draw(text_ascii(min_size=1) | none())
    host = draw(text_ascii(min_size=1))
    port = draw(integers(min_value=1))
    database = draw(text_ascii(min_size=1))
    return URL.create(
        drivername="postgres",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


class TestPGDump:
    @given(
        url=urls(),
        path=temp_paths(),
        format_=sampled_from(get_literal_elements(_PGDumpFormat)),
        jobs=integers(min_value=0) | none(),
        schemas=lists(text_ascii(min_size=1)) | none(),
        tables=tables() | none(),
        logger=text_ascii(min_size=1) | none(),
    )
    async def test_main(
        self,
        *,
        url: URL,
        path: Path,
        format_: _PGDumpFormat,
        jobs: int | None,
        schemas: list[str] | None,
        tables: list[Table] | None,
        logger: str | None,
    ) -> None:
        _ = await pg_dump(
            url,
            path,
            format_=format_,
            jobs=jobs,
            schemas=schemas,
            tables=tables,
            logger=logger,
            dry_run=True,
        )

    async def test_error_database(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres")
        with raises(
            _PGDumpDatabaseError, match="Expected URL to contain a 'database'; got .*"
        ):
            _ = await pg_dump(url, tmp_path, dry_run=True)

    async def test_error_host(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres", database="database")
        with raises(_PGDumpHostError, match="Expected URL to contain a 'host'; got .*"):
            _ = await pg_dump(url, tmp_path, dry_run=True)

    async def test_error_port(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres", database="database", host="host")
        with raises(_PGDumpPortError, match="Expected URL to contain a 'port'; got .*"):
            _ = await pg_dump(url, tmp_path, dry_run=True)


class TestPGRestore:
    @given(
        url=urls(),
        path=temp_paths(),
        database=text_ascii(min_size=1) | none(),
        data_only=booleans(),
        jobs=integers(min_value=0) | none(),
        schemas=lists(text_ascii(min_size=1)) | none(),
        tables=tables() | none(),
        logger=text_ascii(min_size=1) | none(),
    )
    async def test_main(
        self,
        *,
        url: URL,
        path: Path,
        database: str | None,
        data_only: bool,
        jobs: int | None,
        schemas: list[str] | None,
        tables: list[Table] | None,
        logger: str | None,
    ) -> None:
        _ = await pg_restore(
            url,
            path,
            database=database,
            data_only=data_only,
            jobs=jobs,
            schemas=schemas,
            tables=tables,
            logger=logger,
            dry_run=True,
        )

    async def test_error_database(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres")
        with raises(
            _PGRestoreDatabaseError,
            match="Expected URL to contain a 'database'; got .*",
        ):
            _ = await pg_restore(url, tmp_path, dry_run=True)

    async def test_error_host(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres", database="database")
        with raises(
            _PGRestoreHostError, match="Expected URL to contain a 'host'; got .*"
        ):
            _ = await pg_restore(url, tmp_path, dry_run=True)

    async def test_error_port(self, *, tmp_path: Path) -> None:
        url = URL.create("postgres", database="database", host="host")
        with raises(
            _PGRestorePortError, match="Expected URL to contain a 'port'; got .*"
        ):
            _ = await pg_restore(url, tmp_path, dry_run=True)
