from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, assert_never, override

from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
)
from pydantic_settings.sources import DEFAULT_PATH

from utilities.iterables import always_iterable

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pydantic_settings.sources import PathType

    from utilities.types import MaybeSequenceStr, PathLike


type PathLikeOrWithSection = PathLike | tuple[PathLike, MaybeSequenceStr]


class CustomBaseSettings(BaseSettings):
    """Base settings for loading JSON files."""

    # paths
    json_files: ClassVar[Sequence[PathLikeOrWithSection]] = []
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = []
    yaml_files: ClassVar[Sequence[PathLikeOrWithSection]] = []

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
        for json in cls.json_files:
            match json:
                case Path() | str():
                    yield JsonConfigSettingsSource(settings_cls, json_file=json)
                case Path() | str() as file, str() | list() | tuple() as section:
                    yield JsonConfigSectionSettingsSource(
                        settings_cls, json_file=file, section=section
                    )
                case never:
                    assert_never(never)
        for toml in cls.toml_files:
            match toml:
                case Path() | str():
                    yield TomlConfigSettingsSource(settings_cls, toml_file=toml)
                case Path() | str() as file, str() | list() | tuple() as section:
                    yield TomlConfigSectionSettingsSource(
                        settings_cls, toml_file=file, section=section
                    )
                case never:
                    assert_never(never)
        for yaml in cls.yaml_files:
            match yaml:
                case Path() | str():
                    yield YamlConfigSettingsSource(settings_cls, yaml_file=yaml)
                case Path() | str() as file, str() | list() | tuple() as section:
                    yield YamlConfigSectionSettingsSource(
                        settings_cls, yaml_file=file, section=section
                    )
                case never:
                    assert_never(never)


def load_settings[T: BaseSettings](cls: type[T], /) -> T:
    """Load a set of settings."""
    return cls()


class JsonConfigSectionSettingsSource(JsonConfigSettingsSource):
    @override
    def __init__(
        self,
        settings_cls: type[BaseSettings],
        json_file: PathType | None = DEFAULT_PATH,
        json_file_encoding: str | None = None,
        *,
        section: MaybeSequenceStr,
    ) -> None:
        super().__init__(
            settings_cls, json_file=json_file, json_file_encoding=json_file_encoding
        )
        self.section = section

    @override
    def __call__(self) -> dict[str, Any]:
        return reduce(
            lambda acc, el: acc.get(el, {}),
            always_iterable(self.section),
            super().__call__(),
        )


class TomlConfigSectionSettingsSource(TomlConfigSettingsSource):
    @override
    def __init__(
        self,
        settings_cls: type[BaseSettings],
        toml_file: PathType | None = DEFAULT_PATH,
        *,
        section: MaybeSequenceStr,
    ) -> None:
        super().__init__(settings_cls, toml_file=toml_file)
        self.section = section

    @override
    def __call__(self) -> dict[str, Any]:
        return reduce(
            lambda acc, el: acc.get(el, {}),
            always_iterable(self.section),
            super().__call__(),
        )


class YamlConfigSectionSettingsSource(YamlConfigSettingsSource):
    @override
    def __init__(
        self,
        settings_cls: type[BaseSettings],
        yaml_file: PathType | None = DEFAULT_PATH,
        yaml_file_encoding: str | None = None,
        yaml_config_section: str | None = None,
        *,
        section: MaybeSequenceStr,
    ) -> None:
        super().__init__(
            settings_cls,
            yaml_file=yaml_file,
            yaml_file_encoding=yaml_file_encoding,
            yaml_config_section=yaml_config_section,
        )
        self.section = section

    @override
    def __call__(self) -> dict[str, Any]:
        return reduce(
            lambda acc, el: acc.get(el, {}),
            always_iterable(self.section),
            super().__call__(),
        )


__all__ = [
    "CustomBaseSettings",
    "JsonConfigSectionSettingsSource",
    "TomlConfigSectionSettingsSource",
    "YamlConfigSectionSettingsSource",
    "load_settings",
]
