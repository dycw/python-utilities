from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, override

from pydantic_settings_sops import SOPSConfigSettingsSource

from utilities.iterables import always_iterable
from utilities.pydantic_settings import CustomBaseSettings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

    from utilities.types import MaybeIterable, PathLike


class SopsBaseSettings(CustomBaseSettings):
    """Base settings for loading secrets using `sops/age`."""

    # paths
    secret_files: ClassVar[MaybeIterable[PathLike]] = ()

    @classmethod
    @override
    def _yield_base_settings_sources(
        cls,
        settings_cls: type[BaseSettings],
        env_settings: PydanticBaseSettingsSource,
        /,
    ) -> Iterator[PydanticBaseSettingsSource]:
        yield from super()._yield_base_settings_sources(settings_cls, env_settings)
        for file in always_iterable(cls.secret_files):
            yield SOPSConfigSettingsSource(
                settings_cls,  # pyright: ignore[reportArgumentType],
                json_file=file,
            )


__all__ = ["SopsBaseSettings"]
