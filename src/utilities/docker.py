from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


def docker_exec(
    container: str,
    /,
    *cmd: str,
    env: Mapping[str, str] | None = None,
    **env_kwargs: str | None,
) -> list[str]:
    """Run a command through `docker exec`."""
    full = ["docker", "exec"]
    mapping: dict[str, str | None] = ({} if env is None else dict(env)) | env_kwargs
    for key, value in mapping.items():
        full.append(f"--env={key}={value}")
    return [*full, "--interactive", container, *cmd]


__all__ = ["docker_exec"]
