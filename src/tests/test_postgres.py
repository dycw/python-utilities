from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DrawFn, booleans, composite, lists, none, sampled_from
from pytest import raises
from sqlalchemy import URL, Column, Integer, MetaData, Table

from utilities.hypothesis import integers, temp_paths, text_ascii
from utilities.postgres import (
    _build_pg_dump,
    _build_pg_restore_or_psql,
    _extract_url,
    _ExtractURLDatabaseError,
    _ExtractURLHostError,
    _ExtractURLPortError,
    _PGDumpFormat,
    pg_dump,
    restore,
)
from utilities.typing import get_literal_elements

if TYPE_CHECKING:
    from pathlib import Path


@composite
def tables(draw: DrawFn, /) -> list[Table | str]:
    metadata = MetaData()
    names = draw(lists(text_ascii(min_size=1), max_size=5, unique=True))
    tables = [
        Table(n, metadata, Column("id", Integer, primary_key=True)) for n in names
    ]
    return [draw(sampled_from([n, t])) for n, t in zip(names, tables, strict=True)]


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
    @given(url=urls(), path=temp_paths(), logger=text_ascii(min_size=1) | none())
    async def test_main(self, *, url: URL, path: Path, logger: str | None) -> None:
        _ = await pg_dump(url, path, dry_run=True, logger=logger)

    @given(
        url=urls(),
        path=temp_paths(),
        format_=sampled_from(get_literal_elements(_PGDumpFormat)),
        jobs=integers(min_value=0) | none(),
        schemas=lists(text_ascii(min_size=1)) | none(),
        schemas_exc=lists(text_ascii(min_size=1)) | none(),
        tables=tables() | none(),
        tables_exc=tables() | none(),
        inserts=booleans(),
        on_conflict_do_nothing=booleans(),
        docker=text_ascii(min_size=1) | none(),
    )
    def test_build(
        self,
        *,
        url: URL,
        path: Path,
        format_: _PGDumpFormat,
        jobs: int | None,
        schemas: list[str] | None,
        schemas_exc: list[str] | None,
        tables: list[Table | str] | None,
        tables_exc: list[Table | str] | None,
        inserts: bool,
        on_conflict_do_nothing: bool,
        docker: str | None,
    ) -> None:
        _ = _build_pg_dump(
            url,
            path,
            format_=format_,
            jobs=jobs,
            schemas=schemas,
            schemas_exc=schemas_exc,
            tables=tables,
            tables_exc=tables_exc,
            inserts=inserts,
            on_conflict_do_nothing=on_conflict_do_nothing,
            docker=docker,
        )


class TestRestore:
    @given(url=urls(), path=temp_paths(), logger=text_ascii(min_size=1) | none())
    async def test_main(self, *, url: URL, path: Path, logger: str | None) -> None:
        _ = await restore(url, path, dry_run=True, logger=logger)

    @given(
        url=urls(),
        path=temp_paths(),
        psql=booleans(),
        database=text_ascii(min_size=1) | none(),
        data_only=booleans(),
        jobs=integers(min_value=0) | none(),
        schemas=lists(text_ascii(min_size=1)) | none(),
        schemas_exc=lists(text_ascii(min_size=1)) | none(),
        tables=tables() | none(),
        docker=text_ascii(min_size=1) | none(),
    )
    def test_build(
        self,
        *,
        url: URL,
        path: Path,
        psql: bool,
        database: str | None,
        data_only: bool,
        jobs: int | None,
        schemas: list[str] | None,
        schemas_exc: list[str] | None,
        tables: list[Table | str] | None,
        docker: str | None,
    ) -> None:
        _ = _build_pg_restore_or_psql(
            url,
            path,
            psql=psql,
            database=database,
            data_only=data_only,
            jobs=jobs,
            schemas=schemas,
            schemas_exc=schemas_exc,
            tables=tables,
            docker=docker,
        )


class TestExtractURL:
    def test_database(self) -> None:
        url = URL.create("postgres")
        with raises(
            _ExtractURLDatabaseError,
            match="Expected URL to contain a 'database'; got .*",
        ):
            _ = _extract_url(url)

    def test_host(self) -> None:
        url = URL.create("postgres", database="database")
        with raises(
            _ExtractURLHostError, match="Expected URL to contain a 'host'; got .*"
        ):
            _ = _extract_url(url)

    def test_port(self) -> None:
        url = URL.create("postgres", database="database", host="host")
        with raises(
            _ExtractURLPortError, match="Expected URL to contain a 'port'; got .*"
        ):
            _ = _extract_url(url)
