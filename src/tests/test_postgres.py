from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DrawFn, composite, lists, none, sampled_from
from sqlalchemy import URL, Column, Integer, MetaData, Table

from utilities.hypothesis import integers, temp_paths, text_ascii
from utilities.postgres import _PGDumpFormat, pg_dump
from utilities.typing import get_literal_elements

if TYPE_CHECKING:
    from pathlib import Path


@composite
def urls(draw: DrawFn, /) -> URL:
    username = draw(text_ascii(min_size=1) | none())
    password = draw(text_ascii(min_size=1) | none())
    host = draw(text_ascii(min_size=1) | none())
    port = draw(integers(min_value=1) | none())
    database = draw(text_ascii(min_size=1) | none())
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
        tables=lists(text_ascii(min_size=1), unique=True) | none(),
        logger=text_ascii(min_size=1) | none(),
    )
    def test_main(
        self,
        *,
        url: URL,
        path: Path,
        format_: _PGDumpFormat,
        jobs: int | None,
        schemas: list[str] | None,
        tables: list[str] | None,
        logger: str | None,
    ) -> None:
        metadata = MetaData()
        tables_use = (
            None
            if tables is None
            else [
                Table(t, metadata, Column("id", Integer, primary_key=True))
                for t in tables
            ]
        )
        _ = pg_dump(
            url,
            path,
            format_=format_,
            jobs=jobs,
            schemas=schemas,
            tables=tables_use,
            logger=logger,
            dry_run=True,
        )
