from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, Literal, assert_never, override

from utilities.asyncio import stream_command
from utilities.iterables import always_iterable
from utilities.logging import get_logger
from utilities.os import temp_environ
from utilities.sqlalchemy import TableOrORMInstOrClass, get_table_name
from utilities.timer import Timer
from utilities.types import PathLike

if TYPE_CHECKING:
    from sqlalchemy import URL

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
    tables: MaybeSequence[TableOrORMInstOrClass] | None = None,
    logger: LoggerOrName | None = None,
    dry_run: bool = False,
) -> None:
    """Run `pg_dump`."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if url.database is None:
        raise _PGDumpDatabaseError(url=url)
    if url.host is None:
        raise _PGDumpHostError(url=url)
    if url.port is None:
        raise _PGDumpPortError(url=url)
    parts: list[str] = [
        "pg_dump",
        # general options
        f"--dbname={url.database}",
        f"--file={str(path)!r}",
        f"--format={format_}",
        "--verbose",
        # output options
        "--large-objects",
        "--clean",
        "--no-owner",
        "--no-privileges",
        "--if-exists",
        # connection options
        f"--host={url.host}",
        f"--port={url.port}",
        "--no-password",
    ]
    if (format_ == "directory") and (jobs is not None):
        parts.append(f"--jobs={jobs}")
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schemas)])
    if tables is not None:
        parts.extend([f"--table={get_table_name(t)}" for t in always_iterable(tables)])
    if url.username is not None:
        parts.append(f"--username={url.username}")
    cmd = " ".join(parts)
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run %r", str(path))
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


@dataclass(kw_only=True, slots=True)
class PGDumpError(Exception):
    url: URL


@dataclass(kw_only=True, slots=True)
class _PGDumpDatabaseError(PGDumpError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'database'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _PGDumpHostError(PGDumpError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'host'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _PGDumpPortError(PGDumpError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'port'; got {self.url}"


##


async def pg_restore(
    url: URL,
    path: PathLike,
    /,
    *,
    database: str | None = None,
    data_only: bool = False,
    jobs: int | None = None,
    schemas: MaybeListStr | None = None,
    tables: MaybeSequence[TableOrORMInstOrClass] | None = None,
    logger: LoggerOrName | None = None,
    dry_run: bool = False,
) -> None:
    """Run `pg_restore`."""
    match database, url.database:
        case str() as database_use, _:
            ...
        case None, str() as database_use:
            ...
        case None, None:
            raise _PGRestoreDatabaseError(url=url)
        case _ as never:
            assert_never(never)
    if url.host is None:
        raise _PGRestoreHostError(url=url)
    if url.port is None:
        raise _PGRestorePortError(url=url)
    parts: list[str] = [
        "pg_restore",
        # general options
        f"--dbname={database_use}",
        "--verbose",
        # restore options
        "--exit-on-error",
        "--no-owner",
        "--no-privileges",
        "--if-exists",
        # connection options
        f"--host={url.host}",
        f"--port={url.port}",
        "--no-password",
    ]
    if data_only:
        parts.append("--data-only")
    else:
        parts.append("--clean")
    if jobs is not None:
        parts.append(f"--jobs={jobs}")
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in always_iterable(schemas)])
    if tables is not None:
        parts.extend([f"--table={get_table_name(t)}" for t in always_iterable(tables)])
    if url.username is not None:
        parts.append(f"--username={url.username}")
    parts.append(str(path))
    cmd = " ".join(parts)
    if dry_run:
        if logger is not None:
            get_logger(logger=logger).info("Would run %r", str(path))
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


@dataclass(kw_only=True, slots=True)
class PGRestoreError(Exception):
    url: URL


@dataclass(kw_only=True, slots=True)
class _PGRestoreDatabaseError(PGRestoreError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'database'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _PGRestoreHostError(PGRestoreError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'host'; got {self.url}"


@dataclass(kw_only=True, slots=True)
class _PGRestorePortError(PGRestoreError):
    @override
    def __str__(self) -> str:
        return f"Expected URL to contain a 'port'; got {self.url}"


__all__ = ["PGDumpError", "PGRestoreError", "pg_dump", "pg_restore"]
