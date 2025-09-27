from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, override

from pydantic import Field, create_model
from pydantic_settings import CliSettingsSource

from utilities.pydantic_settings import (
    BaseSettings,
    CustomBaseSettings,
    PathLikeOrWithSection,
    load_settings,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pydantic_settings import PydanticBaseSettingsSource


class _Settings(CustomBaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
        Path(__file__).parent.joinpath("config.toml")
    ]

    aaa: int
    inner: _Inner


class _Inner(BaseSettings):
    bbb: int


_SETTINGS = load_settings(_Settings)
_DynamicSettings = create_model(
    "DynamicSettings",
    aaa=(int, Field(default=_SETTINGS.aaa)),
    inner=(type(_SETTINGS.inner), Field(default=_SETTINGS.inner)),
    __base__=type(_SETTINGS),
)


class _DynamicSettings2(_DynamicSettings):
    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (CliSettingsSource(settings_cls, cli_parse_args=True),)


_SETTINGS2 = load_settings(_DynamicSettings2)


def main() -> None:
    print("script...")  # noqa: T201
    print(f"these are the settings:\n{_SETTINGS2}")  # noqa: T201
    print(f"these are the settings:\n{_SETTINGS2.aaa=}")  # noqa: T201
    print(f"these are the settings:\n{_SETTINGS2.inner=}")  # noqa: T201
    print(f"these are the settings:\n{_SETTINGS2.inner.bbb=}")  # noqa: T201


if __name__ == "__main__":
    main()
