from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, Literal, assert_never, override

from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase

from utilities.asyncio import stream_command
from utilities.iterables import always_iterable
from utilities.logging import get_logger
from utilities.os import temp_environ
from utilities.pathlib import ensure_suffix
from utilities.sqlalchemy import get_table_name
from utilities.timer import Timer
from utilities.types import PathLike

if TYPE_CHECKING:
    from sqlalchemy import URL

    from utilities.sqlalchemy import TableOrORMInstOrClass
    from utilities.types import LoggerOrName, MaybeListStr, MaybeSequence, PathLike


type _PGDumpFormat = Literal["plain", "custom", "directory", "tar"]


async def pg_dump(
    url: URL,
    path: PathLike,
    /,
    *,
    format_: _PGDumpFormat = "plain",
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    schemas_exc: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    tables_exc: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    inserts: bool = False,
    on_conflict_do_nothing: bool = False,
    docker: str | None = None,
    dry_run: bool = False,
    logger: LoggerOrName | None = None,
) -> None:
    """Run `pg_dump`."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cmd = _build_pg_dump(
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
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run %r", str(cmd))
        return
    with temp_environ(PGPASSWORD=url.password), Timer() as timer:  # pragma: no cover
        try:
            output = await stream_command(cmd)
        except KeyboardInterrupt:
            if logger is not None:
                get_logger(logger=logger).info(
                    "Cancelled backup to %r after %s", str(path), timer
                )
            rmtree(path, ignore_errors=True)
        else:
            match output.return_code:
                case 0:
                    if logger is not None:
                        get_logger(logger=logger).info(
                            "Backup to %r finished after %s", str(path), timer
                        )
                case _:
                    if logger is not None:
                        get_logger(logger=logger).exception(
                            "Backup to %r failed after %s\nstderr:\n%s",
                            str(path),
                            timer,
                            output.stderr,
                        )
                    rmtree(path, ignore_errors=True)


def _build_pg_dump(
    url: URL,
    path: PathLike,
    /,
    *,
    format_: _PGDumpFormat = "plain",
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    schemas_exc: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    tables_exc: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    inserts: bool = False,
    on_conflict_do_nothing: bool = False,
    docker: str | None = None,
) -> str:
    database, host, port = _extract_url(url)
    match format_:
        case "plain":
            suffix = ".sql"
        case "custom":
            suffix = ".pgdump"
        case "directory":
            suffix = None
        case "tar":
            suffix = ".tar"
        case _ as never:
            assert_never(never)
    file = Path(path)
    if suffix is not None:
        file = ensure_suffix(file, suffix)
    parts: list[str] = [
        "pg_dump",
        # general options
        f"--dbname={database}",
        f"--file={str(file)!r}",
        f"--format={format_}",
        "--verbose",
        # output options
        "--large-objects",
        "--clean",
        "--no-owner",
        "--no-privileges",
        "--if-exists",
        # connection options
        f"--host={host}",
        f"--port={port}",
        "--no-password",
    ]
    if (format_ == "directory") and (jobs is not None):
        parts.append(f"--jobs={jobs}")
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schemas)])
    if schemas_exc is not None:
        parts.extend([f"--exclude-schema={s}" for s in always_iterable(schemas_exc)])
    if tables is not None:
        parts.extend([f"--table={_get_table_name(t)}" for t in always_iterable(tables)])
    if tables_exc is not None:
        parts.extend([
            f"--exclude-table={_get_table_name(t)}" for t in always_iterable(tables_exc)
        ])
    if inserts:
        parts.append("--inserts")
    if on_conflict_do_nothing:
        parts.append("--on-conflict-do-nothing")
    if url.username is not None:
        parts.append(f"--username={url.username}")
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    return " ".join(parts)


##


async def restore(
    url: URL,
    path: PathLike,
    /,
    *,
    psql: bool = False,
    database: str | None = None,
    data_only: bool = False,
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    schemas_exc: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    docker: str | None = None,
    dry_run: bool = False,
    logger: LoggerOrName | None = None,
) -> None:
    """Run `pg_restore`/`psql`."""
    cmd = _build_pg_restore_or_psql(
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
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run %r", str(cmd))
        return
    with temp_environ(PGPASSWORD=url.password), Timer() as timer:  # pragma: no cover
        try:
            output = await stream_command(cmd)
        except KeyboardInterrupt:
            if logger is not None:
                get_logger(logger=logger).info(
                    "Cancelled restore from %r after %s", str(path), timer
                )
        else:
            match output.return_code:
                case 0:
                    if logger is not None:
                        get_logger(logger=logger).info(
                            "Restore from %r finished after %s", str(path), timer
                        )
                case _:
                    if logger is not None:
                        get_logger(logger=logger).exception(
                            "Restore from %r failed after %s\nstderr:\n%s",
                            str(path),
                            timer,
                            output.stderr,
                        )


##


def _build_pg_restore_or_psql(
    url: URL,
    path: PathLike,
    /,
    *,
    psql: bool = False,
    database: str | None = None,
    data_only: bool = False,
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    schemas_exc: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    docker: str | None = None,
) -> str:
    path = Path(path)
    if (path.suffix == ".sql") or psql:
        return _build_psql(url, path, database=database, docker=docker)
    return _build_pg_restore(
        url,
        path,
        database=database,
        data_only=data_only,
        jobs=jobs,
        schemas=schemas,
        schemas_exc=schemas_exc,
        tables=tables,
        docker=docker,
    )


def _build_pg_restore(
    url: URL,
    path: PathLike,
    /,
    *,
    database: str | None = None,
    data_only: bool = False,
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    schemas_exc: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass | str] | None = None,
    docker: str | None = None,
) -> str:
    """Run `pg_restore`."""
    url_database, host, port = _extract_url(url)
    database_use = url_database if database is None else database
    parts: list[str] = [
        "pg_restore",
        # general options
        f"--dbname={database_use}",
        "--verbose",
        # restore options
        "--exit-on-error",
        "--no-owner",
        "--no-privileges",
        # connection options
        f"--host={host}",
        f"--port={port}",
        "--no-password",
    ]
    if data_only:
        parts.append("--data-only")
    else:
        parts.extend(["--clean", "--if-exists"])
    if jobs is not None:
        parts.append(f"--jobs={jobs}")
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schemas)])
    if schemas_exc is not None:
        parts.extend([f"--exclude-schema={s}" for s in always_iterable(schemas_exc)])
    if tables is not None:
        parts.extend([f"--table={_get_table_name(t)}" for t in always_iterable(tables)])
    if url.username is not None:
        parts.append(f"--username={url.username}")
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    parts.append(str(path))
    return " ".join(parts)


def _build_psql(
    url: URL,
    path: PathLike,
    /,
    *,
    database: str | None = None,
    docker: str | None = None,
) -> str:
    """Run `psql`."""
    url_database, host, port = _extract_url(url)
    database_use = url_database if database is None else database
    parts: list[str] = [
        "psql",
        # general options
        f"--dbname={database_use}",
        f"--file={str(path)!r}",
        # connection options
        f"--host={host}",
        f"--port={port}",
        "--no-password",
    ]
    if url.username is not None:
        parts.append(f"--username={url.username}")
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    return " ".join(parts)


##


def _extract_url(url: URL, /) -> tuple[str, str, int]:
    if url.database is None:
        raise _ExtractURLDatabaseError(url=url)
    if url.host is None:
        raise _ExtractURLHostError(url=url)
    if url.port is None:
        raise _ExtractURLPortError(url=url)
    return url.database, url.host, url.port


@dataclass(kw_only=True, slots=True)
class ExtractURLError(Exception):
    url: URL


@dataclass(kw_only=True, slots=True)
class _ExtractURLDatabaseError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'database'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLHostError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'host'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _ExtractURLPortError(ExtractURLError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'port'; got {self.url}"


def _get_table_name(obj: TableOrORMInstOrClass | str, /) -> str:
    match obj:
        case Table() | DeclarativeBase() | type() as table_or_orm:
            return get_table_name(table_or_orm)
        case str() as name:
            return name
        case _ as never:
            assert_never(never)


def _wrap_docker(parts: list[str], container: str, /) -> list[str]:
    return ["docker", "exec", "-it", container, *parts]


__all__ = ["ExtractURLError", "pg_dump", "restore"]
