from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from utilities.errors import ImpossibleCaseError
from utilities.subprocess import (
    MKTEMP_DIR_CMD,
    bash_cmd_and_args,
    maybe_sudo_cmd,
    mkdir,
    mkdir_cmd,
    rm_cmd,
    run,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import LoggerLike, PathLike, StrStrMapping


@overload
def docker_cp(
    src: tuple[str, PathLike],
    dest: PathLike,
    /,
    *,
    sudo: bool = False,
    logger: LoggerLike | None = None,
) -> None: ...
@overload
def docker_cp(
    src: PathLike,
    dest: tuple[str, PathLike],
    /,
    *,
    sudo: bool = False,
    logger: LoggerLike | None = None,
) -> None: ...
def docker_cp(
    src: PathLike | tuple[str, PathLike],
    dest: PathLike | tuple[str, PathLike],
    /,
    *,
    sudo: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    match src, dest:
        case Path() | str(), (str() as cont, Path() | str() as dest_path):
            docker_exec(
                cont, *maybe_sudo_cmd(*mkdir_cmd(dest_path, parent=True), sudo=sudo)
            )
            run(*docker_cp_cmd(src, dest, sudo=sudo), logger=logger)
        case (str(), Path() | str()), Path() | str():
            mkdir(dest, parent=True, sudo=sudo)
            run(*docker_cp_cmd(src, dest, sudo=sudo), logger=logger)
        case _:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{src}", f"{dest=}"])


@overload
def docker_cp_cmd(
    src: tuple[str, PathLike], dest: PathLike, /, *, sudo: bool = False
) -> list[str]: ...
@overload
def docker_cp_cmd(
    src: PathLike, dest: tuple[str, PathLike], /, *, sudo: bool = False
) -> list[str]: ...
def docker_cp_cmd(
    src: PathLike | tuple[str, PathLike],
    dest: PathLike | tuple[str, PathLike],
    /,
    *,
    sudo: bool = False,
) -> list[str]:
    match src, dest:
        case (Path() | str()) as src_use, (
            str() as dest_cont,
            Path() | str() as dest_path,
        ):
            dest_use = f"{dest_cont}:{dest_path}"
        case (str() as src_cont, (Path() | str()) as src_path), (
            Path() | str() as dest_use
        ):
            src_use = f"{src_cont}:{src_path}"
        case _:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{src}", f"{dest=}"])
    parts: list[str] = ["docker", "cp", str(src_use), str(dest_use)]
    return maybe_sudo_cmd(*parts, sudo=sudo)


@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> None: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str | None: ...
def docker_exec(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str | None:
    cmd_and_args = docker_exec_cmd(  # skipif-ci
        container,
        cmd,
        *cmds_or_args,
        env=env,
        user=user,
        workdir=workdir,
        bash=bash,
        **env_kwargs,
    )
    return run(  # skipif-ci
        *cmd_and_args,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        logger=logger,
    )


def docker_exec_cmd(
    container: str,
    cmd: str,
    /,
    *cmds_or_args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    bash: bool = False,
    **env_kwargs: str,
) -> list[str]:
    """Build a command for `docker exec`."""
    args: list[str] = ["docker", "exec"]
    mapping: dict[str, str] = ({} if env is None else dict(env)) | env_kwargs
    for key, value in mapping.items():
        args.extend(["--env", f"{key}={value}"])
    if user is not None:
        args.extend(["--user", user])
    if workdir is not None:
        args.extend(["--workdir", str(workdir)])
    args.append(container)
    if bash:
        return [*args, "bash", "-l", "-c", "\n".join([cmd, *cmds_or_args])]
    return [*args, cmd, *cmds_or_args]


@contextmanager
def yield_docker_temp_dir(
    container: str, /, *, user: str | None = None, logger: LoggerLike | None = None
) -> Iterator[Path]:
    path = Path(  # skipif-ci
        docker_exec(container, *MKTEMP_DIR_CMD, user=user, return_=True, logger=logger)
    )
    try:  # skipif-ci
        yield path
    finally:  # skipif-ci
        docker_exec(container, *rm_cmd(path), user=user, logger=logger)


__all__ = ["docker_cp_cmd", "docker_exec", "docker_exec_cmd", "yield_docker_temp_dir"]
