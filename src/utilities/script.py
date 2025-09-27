from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, override

from pydantic import Field, create_model
from pydantic_settings import CliSettingsSource

from utilities.pathlib import get_repo_root
from utilities.pydantic_settings import (
    BaseSettings,
    CustomBaseSettings,
    PathLikeOrWithSection,
    load_settings,
    load_settings_cli,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_settings import PydanticBaseSettingsSource


class _Settings(CustomBaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
        get_repo_root().joinpath("config.toml")
    ]

    a: int
    b: int
    inner: _Inner


class _Inner(BaseSettings):
    c: int
    d: int


_SETTINGS = load_settings_cli(_Settings)


def main() -> None:
    print("script...")  # noqa: T201
    print(f"these are the settings:\n{_SETTINGS}")  # noqa: T201


if __name__ == "__main__":
    main()
