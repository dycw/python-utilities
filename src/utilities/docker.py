from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from utilities.contextlib import enhanced_context_manager
from utilities.core import always_iterable, log_info, repr_str
from utilities.errors import ImpossibleCaseError
from utilities.subprocess import (
    MKTEMP_DIR_CMD,
    maybe_sudo_cmd,
    mkdir,
    mkdir_cmd,
    rm_cmd,
    run,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import (
        LoggerLike,
        MaybeIterable,
        PathLike,
        Retry,
        StrStrMapping,
    )


def docker_compose_down(
    *,
    files: MaybeIterable[PathLike] | None = None,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    """Stop and remove containers."""
    args = docker_compose_down_cmd(files=files)  # pragma: no cover
    run(  # pragma: no cover
        *args,
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        logger=logger,
    )


def docker_compose_down_cmd(
    *, files: MaybeIterable[PathLike] | None = None
) -> list[str]:
    """Command to use 'docker compose down' to stop and remove containers."""
    return _docker_compose_cmd("down", files=files)


def docker_compose_pull(
    *,
    files: MaybeIterable[PathLike] | None = None,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    """Pull service images."""
    args = docker_compose_pull_cmd(files=files)  # pragma: no cover
    run(  # pragma: no cover
        *args,
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        logger=logger,
    )


def docker_compose_pull_cmd(
    *, files: MaybeIterable[PathLike] | None = None
) -> list[str]:
    """Command to use 'docker compose pull' to pull service images."""
    return _docker_compose_cmd("pull", files=files)


def docker_compose_up(
    *,
    files: MaybeIterable[PathLike] | None = None,
    detach: bool = True,
    cwd: PathLike | None = None,
    env: StrStrMapping | None = None,
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    logger: LoggerLike | None = None,
) -> None:
    """Create and start containers."""
    args = docker_compose_up_cmd(files=files, detach=detach)  # pragma: no cover
    run(  # pragma: no cover
        *args,
        cwd=cwd,
        env=env,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        logger=logger,
    )


def docker_compose_up_cmd(
    *, files: MaybeIterable[PathLike] | None = None, detach: bool = True
) -> list[str]:
    """Command to use 'docker compose up' to create and start containers."""
    args: list[str] = []
    if detach:
        args.append("--detach")
    return _docker_compose_cmd("up", *args, files=files)


def _docker_compose_cmd(
    cmd: str, /, *args: str, files: MaybeIterable[PathLike] | None = None
) -> list[str]:
    all_args: list[str] = ["docker", "compose"]
    if files is not None:
        for file in always_iterable(files):
            all_args.extend(["--file", str(file)])
    return [*all_args, cmd, *args]


##


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
    """Copy between a container and the local file system."""
    match src, dest:  # skipif-ci
        case Path() | str(), (str() as cont, Path() | str() as dest_path):
            docker_exec(
                cont, *maybe_sudo_cmd(*mkdir_cmd(dest_path, parent=True), sudo=sudo)
            )
            run(*maybe_sudo_cmd(*docker_cp_cmd(src, dest), sudo=sudo), logger=logger)
        case (str(), Path() | str()), Path() | str():
            mkdir(dest, parent=True, sudo=sudo)
            run(*maybe_sudo_cmd(*docker_cp_cmd(src, dest), sudo=sudo), logger=logger)
        case _:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{src}", f"{dest=}"])


@overload
def docker_cp_cmd(src: tuple[str, PathLike], dest: PathLike, /) -> list[str]: ...
@overload
def docker_cp_cmd(src: PathLike, dest: tuple[str, PathLike], /) -> list[str]: ...
def docker_cp_cmd(
    src: PathLike | tuple[str, PathLike], dest: PathLike | tuple[str, PathLike], /
) -> list[str]:
    """Command to use 'docker cp' to copy between a container and the local file system."""
    args: list[str] = ["docker", "cp"]
    match src, dest:
        case ((Path() | str()), (str() as cont, Path() | str() as path)):
            return [*args, str(src), f"{cont}:{path}"]
        case (str() as cont, (Path() | str()) as path), (Path() | str() as dest):
            return [*args, f"{cont}:{path}", str(dest)]
        case _:  # pragma: no cover
            raise ImpossibleCaseError(case=[f"{src}", f"{dest=}"])


##


@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> None: ...
@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str | None: ...
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    input: str | None = None,  # noqa: A002
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    suppress: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    **env_kwargs: str,
) -> str | None:
    """Execute a command in a container."""
    run_cmd_and_args = docker_exec_cmd(  # skipif-ci
        container,
        cmd,
        *args,
        env=env,
        interactive=input is not None,
        user=user,
        workdir=workdir,
        **env_kwargs,
    )
    return run(  # skipif-ci
        *run_cmd_and_args,
        input=input,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        suppress=suppress,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
        retry=retry,
        logger=logger,
    )


def docker_exec_cmd(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    interactive: bool = False,
    user: str | None = None,
    workdir: PathLike | None = None,
    **env_kwargs: str,
) -> list[str]:
    """Command to use `docker exec` to execute a command in a container."""
    all_args: list[str] = ["docker", "exec"]
    mapping: dict[str, str] = ({} if env is None else dict(env)) | env_kwargs
    for key, value in mapping.items():
        all_args.extend(["--env", f"{key}={value}"])
    if interactive:
        all_args.append("--interactive")
    if user is not None:
        all_args.extend(["--user", user])
    if workdir is not None:
        all_args.extend(["--workdir", str(workdir)])
    return [*all_args, container, cmd, *args]


##


@enhanced_context_manager
def yield_docker_temp_dir(
    container: str,
    /,
    *,
    user: str | None = None,
    retry: Retry | None = None,
    logger: LoggerLike | None = None,
    keep: bool = False,
) -> Iterator[Path]:
    """Yield a temporary directory in a Docker container."""
    path = Path(  # skipif-ci
        docker_exec(
            container,
            *MKTEMP_DIR_CMD,
            user=user,
            return_=True,
            retry=retry,
            logger=logger,
        )
    )
    try:  # skipif-ci
        yield path
    finally:  # skipif-ci
        if keep:
            log_info(logger, "Keeping temporary directory %s...", repr_str(path))
        else:
            docker_exec(container, *rm_cmd(path), user=user, retry=retry, logger=logger)


__all__ = [
    "docker_compose_down",
    "docker_compose_down_cmd",
    "docker_compose_pull",
    "docker_compose_pull_cmd",
    "docker_compose_up",
    "docker_compose_up_cmd",
    "docker_cp",
    "docker_cp_cmd",
    "docker_exec",
    "docker_exec_cmd",
    "yield_docker_temp_dir",
]
