from __future__ import annotations

from typing import TYPE_CHECKING, override

from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import PathLike


class CustomizedBasedSettings(BaseSettings):
    # paths
    json_files: tuple[PathLike, ...]

    # config
    model_config = SettingsConfigDict(env_nested_delimiter="__")

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
        _ = (init_settings, dotenv_settings, file_secret_settings)
        return tuple(cls._yield_base_settings_sources(settings_cls, env_settings))

    @classmethod
    def _yield_base_settings_sources(
        cls,
        settings_cls: type[BaseSettings],
        env_settings: PydanticBaseSettingsSource,
        /,
    ) -> Iterator[PydanticBaseSettingsSource]:
        yield env_settings
        for file in cls.json_files:
            yield JsonConfigSettingsSource(settings_cls, json_file=file)


__all__ = ["CustomizedBasedSettings"]
