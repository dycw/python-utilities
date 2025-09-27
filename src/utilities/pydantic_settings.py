from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, assert_never, cast, override

from pydantic import Field, create_model
from pydantic_settings import (
    BaseSettings,
    CliSettingsSource,
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

    from pydantic.fields import FieldInfo
    from pydantic_settings.sources import PathType

    from utilities.types import MaybeSequenceStr, PathLike


type PathLikeWithSection = tuple[PathLike, MaybeSequenceStr]
type PathLikeOrWithSection = PathLike | PathLikeWithSection


class CustomBaseSettings(BaseSettings):
    """Base settings for loading JSON/TOML/YAML files."""

    # paths
    json_files: ClassVar[Sequence[PathLikeOrWithSection]] = []
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = []
    yaml_files: ClassVar[Sequence[PathLikeOrWithSection]] = []

    # config
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        frozen=True, env_nested_delimiter="__"
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
        for file, section in map(_ensure_section, cls.json_files):
            yield JsonConfigSectionSettingsSource(
                settings_cls, json_file=file, section=section
            )
        for file, section in map(_ensure_section, cls.toml_files):
            yield TomlConfigSectionSettingsSource(
                settings_cls, toml_file=file, section=section
            )
        for file, section in map(_ensure_section, cls.yaml_files):
            yield YamlConfigSectionSettingsSource(
                settings_cls, yaml_file=file, section=section
            )


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
        return _get_section(super().__call__(), self.section)


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
        return _get_section(super().__call__(), self.section)


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
        return _get_section(super().__call__(), self.section)


def _ensure_section(file: PathLikeOrWithSection, /) -> PathLikeWithSection:
    match file:
        case Path() | str():
            return file, []
        case Path() | str() as path, str() | list() | tuple() as section:
            return path, section
        case never:
            assert_never(never)


def _get_section(
    mapping: dict[str, Any], section: MaybeSequenceStr, /
) -> dict[str, Any]:
    return reduce(lambda acc, el: acc.get(el, {}), always_iterable(section), mapping)


##


class HashableBaseSettings(BaseSettings):
    """Base settings for loading JSON files."""

    # config
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(frozen=True)


##


def load_settings_cli[T: BaseSettings](cls: type[T], /) -> T:
    """Load a set of settings."""
    _ = cls.model_rebuild()
    new_cls = _load_settings_cli_create_model(cls)

    @classmethod
    def settings_customise_sources(
        cls: type[BaseSettings],
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        parent = cast("Any", super(new_cls, cls)).settings_customise_sources(
            settings_cls=settings_cls,
            init_settings=init_settings,
            env_settings=env_settings,
            dotenv_settings=dotenv_settings,
            file_secret_settings=file_secret_settings,
        )
        return (
            *parent,
            CliSettingsSource(settings_cls, cli_parse_args=True, case_sensitive=False),
        )

    new_cls2 = type(
        new_cls.__name__,
        (new_cls,),
        {"settings_customise_sources": settings_customise_sources},
    )
    return load_settings(cast("type[T]", new_cls2))


def _load_settings_cli_create_model[T: BaseSettings](
    cls: type[T], /, *, values: T | None = None
) -> type[T]:
    values_use = cls() if values is None else values
    kwargs: dict[str, Any] = {}
    for name, field in cls.model_fields.items():
        if (ann := field.annotation) is None:
            raise LoadSettingsCLIError(field=field)
        value = getattr(values_use, name)
        if issubclass(ann, BaseSettings):
            kwargs[name] = _load_settings_cli_create_model(ann, values=value)
        else:
            kwargs[name] = (field.annotation, Field(default=value))
    return create_model(cls.__name__, __base__=cls, **kwargs)


@dataclass(kw_only=True, slots=True)
class LoadSettingsCLIError(Exception):
    field: FieldInfo

    @override
    def __str__(self) -> str:
        return f"Field {self.field} must have an annotation"


__all__ = [
    "CustomBaseSettings",
    "HashableBaseSettings",
    "JsonConfigSectionSettingsSource",
    "TomlConfigSectionSettingsSource",
    "YamlConfigSectionSettingsSource",
    "load_settings",
    "load_settings_cli",
]
