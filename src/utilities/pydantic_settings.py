from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, override

from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
)

from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import Iterator

    from utilities.types import MaybeIterable, PathLike


class CustomBaseSettings(BaseSettings):
    """Base settings for loading JSON files."""

    # paths
    json_files: ClassVar[MaybeIterable[PathLike]] = ()
    toml_files: ClassVar[MaybeIterable[PathLike]] = ()
    yaml_files: ClassVar[MaybeIterable[PathLike]] = ()

    # config
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_nested_delimiter="__"
    )

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
        for file in always_iterable(cls.json_files):
            yield JsonConfigSettingsSource(settings_cls, json_file=file)
        for file in always_iterable(cls.toml_files):
            yield TomlConfigSettingsSource(settings_cls, toml_file=file)
        for file in always_iterable(cls.yaml_files):
            yield YamlConfigSettingsSource(settings_cls, yaml_file=file)


def load_settings[T: BaseSettings](cls: type[T], /) -> T:
    """Load a set of settings."""
    return cls()


__all__ = ["CustomBaseSettings", "load_settings"]
