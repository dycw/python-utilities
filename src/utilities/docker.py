from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from utilities.subprocess import run

if TYPE_CHECKING:
    from utilities.types import PathLike, StrStrMapping


@overload
def docker_exec(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[True],
    return_stdout: bool = False,
    return_stderr: bool = False,
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
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: Literal[True],
    return_stderr: bool = False,
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
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: Literal[True],
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
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: Literal[False] = False,
    return_stdout: Literal[False] = False,
    return_stderr: Literal[False] = False,
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
    print: bool = False,
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
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
    print: bool = False,  # noqa: A002
    print_stdout: bool = False,
    print_stderr: bool = False,
    return_: bool = False,
    return_stdout: bool = False,
    return_stderr: bool = False,
    **env_kwargs: str,
) -> str | None:
    cmd_use = docker_exec_cmd(  # skipif-ci
        container, cmd, *args, env=env, user=user, workdir=workdir, **env_kwargs
    )
    return run(
        *cmd_use,
        print=print,
        print_stdout=print_stdout,
        print_stderr=print_stderr,
        return_=return_,
        return_stdout=return_stdout,
        return_stderr=return_stderr,
    )


def docker_exec_cmd(
    container: str,
    cmd: str,
    /,
    *args: str,
    env: StrStrMapping | None = None,
    user: str | None = None,
    workdir: PathLike | None = None,
    **env_kwargs: str,
) -> list[str]:
    """Build a command for `docker exec`."""
    parts: list[str] = ["docker", "exec"]
    mapping: dict[str, str] = ({} if env is None else dict(env)) | env_kwargs
    for key, value in mapping.items():
        parts.extend(["--env", f"{key}={value}"])
    if user is not None:
        parts.extend(["--user", user])
    if workdir is not None:
        parts.extend(["--workdir", str(workdir)])
    return [*parts, container, cmd, *args]


__all__ = ["docker_exec", "docker_exec_cmd"]
