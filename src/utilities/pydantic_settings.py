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

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pydantic_settings.sources import PathType

    from utilities.types import MaybeSequenceStr, PathLike


type PathLikeOrWithSection = PathLike | tuple[PathLike, MaybeSequenceStr]


class CustomBaseSettings(BaseSettings):
    """Base settings for loading JSON files."""

    # paths
    json_files: ClassVar[Sequence[PathLike]] = []
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = []
    yaml_files: ClassVar[Sequence[PathLike]] = []

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
        for file in cls.json_files:
            yield JsonConfigSettingsSource(settings_cls, json_file=file)
        for path_or_pair in cls.toml_files:
            match path_or_pair:
                case Path() | str() as file:
                    yield TomlConfigSettingsSource(settings_cls, toml_file=file)
                case Path() | str() as file, str() | list() | tuple() as section:
                    yield TomlConfigSectionSettingsSource(
                        settings_cls, toml_file=file, section=section
                    )
                case never:
                    assert_never(never)
        for file in cls.yaml_files:
            yield YamlConfigSettingsSource(settings_cls, yaml_file=file)


def load_settings[T: BaseSettings](cls: type[T], /) -> T:
    """Load a set of settings."""
    return cls()


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
        result = super().__call__()
        match self.section:
            case str():
                return result.get(self.section, {})
            case list() | tuple() as path:
                return reduce(lambda acc, el: acc.get(el, {}), path, result)
            case never:
                assert_never(never)


__all__ = ["CustomBaseSettings", "TomlConfigSectionSettingsSource", "load_settings"]
