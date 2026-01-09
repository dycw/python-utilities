from __future__ import annotations

from logging import Filter, LogRecord, getLogger
from re import search
from typing import TYPE_CHECKING, ClassVar, override

from pydantic_settings.sources import DEFAULT_PATH
from pydantic_settings_sops import SOPSConfigSettingsSource

from utilities.pydantic_settings import (
    CustomBaseSettings,
    PathLikeOrWithSection,
    _ensure_section,
    _get_section,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
    from pydantic_settings.sources import PathType

    from utilities.types import MaybeSequenceStr, StrDict


class _SuppressDefaultConfigMessage(Filter):
    @override
    def filter(self, record: LogRecord) -> bool:
        return not search(
            r"^default config file does not exists '.*'$", record.getMessage()
        )


getLogger("sopsy.utils").addFilter(_SuppressDefaultConfigMessage())


class SopsBaseSettings(CustomBaseSettings):
    """Base settings for loading secrets using `sops/age`."""

    # paths
    secret_files: ClassVar[Sequence[PathLikeOrWithSection]] = []

    @classmethod
    @override
    def _yield_base_settings_sources(
        cls,
        settings_cls: type[BaseSettings],
        env_settings: PydanticBaseSettingsSource,
        /,
    ) -> Iterator[PydanticBaseSettingsSource]:
        yield from super()._yield_base_settings_sources(settings_cls, env_settings)
        for file, section in map(_ensure_section, cls.secret_files):
            yield SOPSConfigSectionSettingsSource(
                settings_cls, json_file=file, section=section
            )


class SOPSConfigSectionSettingsSource(SOPSConfigSettingsSource):
    @override
    def __init__(
        self,
        settings_cls: type[BaseSettings],
        json_file: PathType | None = DEFAULT_PATH,
        yaml_file: PathType | None = DEFAULT_PATH,
        *,
        section: MaybeSequenceStr,
    ) -> None:
        super().__init__(settings_cls, json_file=json_file, yaml_file=yaml_file)  # pyright: ignore[reportArgumentType]
        self.section = section

    @override
    def __call__(self) -> StrDict:
        return _get_section(super().__call__(), self.section)


__all__ = ["SOPSConfigSectionSettingsSource", "SopsBaseSettings"]
