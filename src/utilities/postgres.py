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
from utilities.sqlalchemy import extract_url, get_table_name
from utilities.timer import Timer
from utilities.types import PathLike

if TYPE_CHECKING:
    from sqlalchemy import URL

    from utilities.sqlalchemy import TableOrORMInstOrClass
    from utilities.types import (
        LoggerOrName,
        MaybeCollection,
        MaybeCollectionStr,
        PathLike,
    )


type _PGDumpFormat = Literal["plain", "custom", "directory", "tar"]


async def pg_dump(
    url: URL,
    path: PathLike,
    /,
    *,
    format_: _PGDumpFormat = "plain",
    jobs: int | None = None,
    data_only: bool = False,
    clean: bool = False,
    create: bool = False,
    extension: MaybeCollectionStr | None = None,
    extension_exc: MaybeCollectionStr | None = None,
    schema: MaybeCollectionStr | None = None,
    schema_exc: MaybeCollectionStr | None = None,
    table: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    table_exc: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    inserts: bool = False,
    on_conflict_do_nothing: bool = False,
    role: str | None = None,
    docker: str | None = None,
    dry_run: bool = False,
    logger: LoggerOrName | None = None,
) -> None:
    """Run `pg_dump`."""
    path = _path_pg_dump(path, format_=format_)
    path.parent.mkdir(parents=True, exist_ok=True)
    cmd = _build_pg_dump(
        url,
        path,
        format_=format_,
        jobs=jobs,
        data_only=data_only,
        clean=clean,
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
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run:\n\t%r", str(cmd))
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
    data_only: bool = False,
    clean: bool = False,
    create: bool = False,
    extension: MaybeCollectionStr | None = None,
    extension_exc: MaybeCollectionStr | None = None,
    schema: MaybeCollectionStr | None = None,
    schema_exc: MaybeCollectionStr | None = None,
    table: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    table_exc: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    inserts: bool = False,
    on_conflict_do_nothing: bool = False,
    role: str | None = None,
    docker: str | None = None,
) -> str:
    extracted = extract_url(url)
    path = _path_pg_dump(path, format_=format_)
    parts: list[str] = [
        "pg_dump",
        # general options
        f"--file={str(path)!r}",
        f"--format={format_}",
        "--verbose",
        # output options
        *_resolve_data_only_and_clean(data_only=data_only, clean=clean),
        "--large-objects",
        "--no-owner",
        "--no-privileges",
        # connection options
        f"--dbname={extracted.database}",
        f"--host={extracted.host}",
        f"--port={extracted.port}",
        f"--username={extracted.username}",
        "--no-password",
    ]
    if (format_ == "directory") and (jobs is not None):
        parts.append(f"--jobs={jobs}")
    if create:
        parts.append("--create")
    if extension is not None:
        parts.extend([f"--extension={e}" for e in always_iterable(extension)])
    if extension_exc is not None:
        parts.extend([
            f"--exclude-extension={e}" for e in always_iterable(extension_exc)
        ])
    if schema is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schema)])
    if schema_exc is not None:
        parts.extend([f"--exclude-schema={s}" for s in always_iterable(schema_exc)])
    if table is not None:
        parts.extend([f"--table={_get_table_name(t)}" for t in always_iterable(table)])
    if table_exc is not None:
        parts.extend([
            f"--exclude-table={_get_table_name(t)}" for t in always_iterable(table_exc)
        ])
    if inserts:
        parts.append("--inserts")
    if on_conflict_do_nothing:
        parts.append("--on-conflict-do-nothing")
    if role is not None:
        parts.append(f"--role={role}")
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    return " ".join(parts)


def _path_pg_dump(path: PathLike, /, *, format_: _PGDumpFormat = "plain") -> Path:
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
    path = Path(path)
    if suffix is not None:
        path = ensure_suffix(path, suffix)
    return path


##


async def restore(
    url: URL,
    path: PathLike,
    /,
    *,
    psql: bool = False,
    data_only: bool = False,
    clean: bool = False,
    create: bool = False,
    jobs: int | None = None,
    schema: MaybeCollectionStr | None = None,
    schema_exc: MaybeCollectionStr | None = None,
    table: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    role: str | None = None,
    docker: str | None = None,
    dry_run: bool = False,
    logger: LoggerOrName | None = None,
) -> None:
    """Run `pg_restore`/`psql`."""
    cmd = _build_pg_restore_or_psql(
        url,
        path,
        psql=psql,
        data_only=data_only,
        clean=clean,
        create=create,
        jobs=jobs,
        schema=schema,
        schema_exc=schema_exc,
        table=table,
        role=role,
        docker=docker,
    )
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run:\n\t%r", str(cmd))
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
    data_only: bool = False,
    clean: bool = False,
    create: bool = False,
    jobs: int | None = None,
    schema: MaybeCollectionStr | None = None,
    schema_exc: MaybeCollectionStr | None = None,
    table: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    role: str | None = None,
    docker: str | None = None,
) -> str:
    path = Path(path)
    if (path.suffix == ".sql") or psql:
        return _build_psql(url, path, docker=docker)
    return _build_pg_restore(
        url,
        path,
        data_only=data_only,
        clean=clean,
        create=create,
        jobs=jobs,
        schemas=schema,
        schemas_exc=schema_exc,
        tables=table,
        role=role,
        docker=docker,
    )


def _build_pg_restore(
    url: URL,
    path: PathLike,
    /,
    *,
    data_only: bool = False,
    clean: bool = False,
    create: bool = False,
    jobs: int | None = None,
    schemas: MaybeCollectionStr | None = None,
    schemas_exc: MaybeCollectionStr | None = None,
    tables: MaybeCollection[TableOrORMInstOrClass | str] | None = None,
    role: str | None = None,
    docker: str | None = None,
) -> str:
    """Run `pg_restore`."""
    extracted = extract_url(url)
    parts: list[str] = [
        "pg_restore",
        # general options
        "--verbose",
        # restore options
        *_resolve_data_only_and_clean(data_only=data_only, clean=clean),
        "--exit-on-error",
        "--no-owner",
        "--no-privileges",
        # connection options
        f"--host={extracted.host}",
        f"--port={extracted.port}",
        f"--username={extracted.username}",
        f"--dbname={extracted.database}",
        "--no-password",
    ]
    if create:
        parts.append("--create")
    if jobs is not None:
        parts.append(f"--jobs={jobs}")
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schemas)])
    if schemas_exc is not None:
        parts.extend([f"--exclude-schema={s}" for s in always_iterable(schemas_exc)])
    if tables is not None:
        parts.extend([f"--table={_get_table_name(t)}" for t in always_iterable(tables)])
    if role is not None:
        parts.append(f"--role={role}")
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    parts.append(str(path))
    return " ".join(parts)


def _build_psql(url: URL, path: PathLike, /, *, docker: str | None = None) -> str:
    """Run `psql`."""
    extracted = extract_url(url)
    parts: list[str] = [
        "psql",
        # general options
        f"--dbname={extracted.database}",
        f"--file={str(path)!r}",
        # connection options
        f"--host={extracted.host}",
        f"--port={extracted.port}",
        f"--username={extracted.username}",
        "--no-password",
    ]
    if docker is not None:
        parts = _wrap_docker(parts, docker)
    return " ".join(parts)


##


def _get_table_name(obj: TableOrORMInstOrClass | str, /) -> str:
    match obj:
        case Table() | DeclarativeBase() | type() as table_or_orm:
            return get_table_name(table_or_orm)
        case str() as name:
            return name
        case _ as never:
            assert_never(never)


def _resolve_data_only_and_clean(
    *, data_only: bool = False, clean: bool = False
) -> list[str]:
    match data_only, clean:
        case False, False:
            return []
        case True, False:
            return ["--data-only"]
        case False, True:
            return ["--clean", "--if-exists"]
        case True, True:
            raise _ResolveDataOnlyAndCleanError
        case _ as never:
            assert_never(never)


@dataclass(kw_only=True, slots=True)
class _ResolveDataOnlyAndCleanError(Exception):
    @override
    def __str__(self) -> str:
        return "Cannot use '--data-only' and '--clean' together"


def _wrap_docker(parts: list[str], container: str, /) -> list[str]:
    return ["docker", "exec", "-it", container, *parts]


__all__ = ["pg_dump", "restore"]
