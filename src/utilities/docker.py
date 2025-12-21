from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from utilities.types import PathLike, StrStrMapping


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


__all__ = ["docker_exec_cmd"]
