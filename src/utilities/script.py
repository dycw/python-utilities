from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, override

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

config = Path(__file__).parent.joinpath("config.toml")
print(config)  # noqa: T201
assert config.exists()


class _Settings(CustomBaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [config]

    aaa: int

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
        return (
            *super().settings_customise_sources(
                settings_cls,
                init_settings,
                env_settings,
                dotenv_settings,
                file_secret_settings,
            ),
            CliSettingsSource(settings_cls, cli_parse_args=True),
        )


def main() -> None:
    print("script...")  # noqa: T201
    settings = load_settings(_Settings)
    print(settings)  # noqa: T201


if __name__ == "__main__":
    main()
