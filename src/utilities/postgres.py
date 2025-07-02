from __future__ import annotations

from asyncio import run
from pathlib import Path
from shutil import rmtree
from typing import TYPE_CHECKING, Literal

from utilities.asyncio import stream_command
from utilities.logging import get_logger
from utilities.sqlalchemy import TableOrORMInstOrClass, get_table_name
from utilities.timer import Timer
from utilities.types import PathLike

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import URL

    from utilities.types import LoggerOrName, PathLike


def pg_dump(
    url: URL,
    path: PathLike,
    /,
    *,
    format_: Literal["plain", "custom", "directory", "tar"] = "plain",
    jobs: int | None = None,
    schemas: Sequence[str] | None = None,
    tables: Sequence[TableOrORMInstOrClass] | None = None,
    logger: LoggerOrName | None = None,
) -> None:
    """Run `pg_dump`."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    parts: list[str] = ["pg_dump", f"--file={str(path)!r}", f"--format={format_}"]
    if (format_ == "directory") and (jobs is not None):
        parts.append(f"--jobs={jobs}")
    parts.extend(["--verbose", "--large-objects", "--clean"])
    if schemas is not None:
        parts.extend([f"--schema={s}" for s in schemas])
    parts.append("--no-owner")
    if tables is not None:
        parts.extend([f"--table={get_table_name(t)}" for t in tables])
    parts.extend([
        "--no-privileges",
        "--if-exists",
        "--no-password",
        repr(url.render_as_string(hide_password=False)),
    ])
    cmd = " ".join(parts)
    with Timer() as timer:
        try:
            output = run(stream_command(cmd))
        except KeyboardInterrupt:
            if logger is not None:
                get_logger(logger=logger).info(
                    "Cancelled backup to %r after %s", str(path), timer
                )
            rmtree(path, ignore_errors=True)
            return
        if output.return_code == 0:
            if logger is not None:
                get_logger(logger=logger).info(
                    "Backup to %r finished after %s", str(path), timer
                )
            return
        if logger is not None:
            get_logger(logger=logger).exception(
                "Backup to %r failed after %s", str(path), timer
            )
        rmtree(path, ignore_errors=True)


__all__ = ["pg_dump"]
