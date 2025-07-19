from __future__ import annotations

from typing import TYPE_CHECKING

from hypothesis import assume, given
from hypothesis.strategies import DrawFn, booleans, composite, lists, none, sampled_from
from pytest import raises
from sqlalchemy import URL, Column, Integer, MetaData, Table

from utilities.hypothesis import integers, temp_paths, text_ascii, urls
from utilities.postgres import (
    _build_pg_dump,
    _build_pg_restore_or_psql,
    _path_pg_dump,
    _PGDumpFormat,
    _resolve_data_only_and_clean,
    _ResolveDataOnlyAndCleanError,
    pg_dump,
    restore,
)
from utilities.typing import get_literal_elements

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import MaybeListStr


@composite
def tables(draw: DrawFn, /) -> list[Table | str]:
    metadata = MetaData()
    names = draw(lists(text_ascii(min_size=1), max_size=5, unique=True))
    tables = [
        Table(n, metadata, Column("id", Integer, primary_key=True)) for n in names
    ]
    return [draw(sampled_from([n, t])) for n, t in zip(names, tables, strict=True)]


class TestPGDump:
    @given(
        url=urls(all_=True), path=temp_paths(), logger=text_ascii(min_size=1) | none()
    )
    async def test_main(self, *, url: URL, path: Path, logger: str | None) -> None:
        _ = await pg_dump(url, path, dry_run=True, logger=logger)

    @given(
        url=urls(all_=True),
        path=temp_paths(),
        format_=sampled_from(get_literal_elements(_PGDumpFormat)),
        jobs=integers(min_value=0) | none(),
        create=booleans(),
        extension=lists(text_ascii(min_size=1)) | none(),
        extension_exc=lists(text_ascii(min_size=1)) | none(),
        schema=lists(text_ascii(min_size=1)) | none(),
        schema_exc=lists(text_ascii(min_size=1)) | none(),
        table=tables() | none(),
        table_exc=tables() | none(),
        inserts=booleans(),
        on_conflict_do_nothing=booleans(),
        role=text_ascii(min_size=1) | none(),
        docker=text_ascii(min_size=1) | none(),
    )
    def test_build(
        self,
        *,
        url: URL,
        path: Path,
        format_: _PGDumpFormat,
        jobs: int | None,
        create: bool,
        extension: MaybeListStr | None,
        extension_exc: MaybeListStr | None,
        schema: list[str] | None,
        schema_exc: list[str] | None,
        table: list[Table | str] | None,
        table_exc: list[Table | str] | None,
        inserts: bool,
        on_conflict_do_nothing: bool,
        role: str | None,
        docker: str | None,
    ) -> None:
        _ = _build_pg_dump(
            url,
            path,
            format_=format_,
            jobs=jobs,
            create=create,
            extension=extension,
            extension_exc=extension_exc,
            schema=schema,
            schema_exc=schema_exc,
            table=table,
            table_exc=table_exc,
            inserts=inserts,
            on_conflict_do_nothing=on_conflict_do_nothing,
            role=role,
            docker=docker,
        )

    @given(path=temp_paths(), format_=sampled_from(get_literal_elements(_PGDumpFormat)))
    def test_path(self, *, path: Path, format_: _PGDumpFormat) -> None:
        path = _path_pg_dump(path, format_=format_)
        assert path.suffix in [".sql", ".pgdump", "", ".tar"]


class TestResolveDataOnlyAndClean:
    @given(data_only=booleans(), clean=booleans())
    def test_main(self, *, data_only: bool, clean: bool) -> None:
        _ = assume(not (data_only and clean))
        _ = _resolve_data_only_and_clean(data_only=data_only, clean=clean)

    def test_erorr(self) -> None:
        with raises(
            _ResolveDataOnlyAndCleanError,
            match="Cannot use '--data-only' and '--clean' together",
        ):
            _ = _resolve_data_only_and_clean(data_only=True, clean=True)


class TestRestore:
    @given(
        url=urls(all_=True), path=temp_paths(), logger=text_ascii(min_size=1) | none()
    )
    async def test_main(self, *, url: URL, path: Path, logger: str | None) -> None:
        _ = await restore(url, path, dry_run=True, logger=logger)

    @given(
        url=urls(all_=True),
        path=temp_paths(),
        psql=booleans(),
        create=booleans(),
        jobs=integers(min_value=0) | none(),
        schema=lists(text_ascii(min_size=1)) | none(),
        schema_exc=lists(text_ascii(min_size=1)) | none(),
        table=tables() | none(),
        role=text_ascii(min_size=1) | none(),
        docker=text_ascii(min_size=1) | none(),
    )
    def test_build(
        self,
        *,
        url: URL,
        path: Path,
        psql: bool,
        create: bool,
        jobs: int | None,
        schema: list[str] | None,
        schema_exc: list[str] | None,
        table: list[Table | str] | None,
        role: str | None,
        docker: str | None,
    ) -> None:
        _ = _build_pg_restore_or_psql(
            url,
            path,
            psql=psql,
            create=create,
            jobs=jobs,
            schema=schema,
            schema_exc=schema_exc,
            table=table,
            role=role,
            docker=docker,
        )
