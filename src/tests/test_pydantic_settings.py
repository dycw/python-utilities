from __future__ import annotations

import json
from typing import TYPE_CHECKING, ClassVar

import tomlkit
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

from utilities.os import temp_environ
from utilities.pydantic_settings import CustomBaseSettings, load_settings

if TYPE_CHECKING:
    from pathlib import Path

    from utilities.types import MaybeIterable, PathLike


class TestCustomBaseSettings:
    def test_json(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.json")
        _ = file.write_text(json.dumps({"x": 1}))

        class Settings(CustomBaseSettings):
            json_files: ClassVar[MaybeIterable[PathLike]] = file
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_toml(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.toml")
        _ = file.write_text(tomlkit.dumps({"x": 1}))

        class Settings(CustomBaseSettings):
            toml_files: ClassVar[MaybeIterable[PathLike]] = file
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_yaml(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.yaml")
        _ = file.write_text(yaml.dump({"x": 1}))

        class Settings(CustomBaseSettings):
            yaml_files: ClassVar[MaybeIterable[PathLike]] = file
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_env_var(self) -> None:
        class Settings(CustomBaseSettings):
            x: int

        with temp_environ(x="1"):
            settings = load_settings(Settings)
        assert settings.x == 1

    def test_env_var_with_prefix(self) -> None:
        class Settings(CustomBaseSettings):
            model_config = SettingsConfigDict(env_prefix="test_")
            x: int

        with temp_environ(test_x="1"):
            settings = load_settings(Settings)
        assert settings.x == 1

    def test_env_var_with_nested(self) -> None:
        class Settings(CustomBaseSettings):
            inner: Inner

        class Inner(BaseSettings):
            x: int

        _ = Settings.model_rebuild()

        with temp_environ(inner__x="1"):
            settings = load_settings(Settings)
        assert settings.inner.x == 1

    def test_env_var_with_prefix_and_nested(self) -> None:
        class Settings(CustomBaseSettings):
            model_config = SettingsConfigDict(env_prefix="test__")
            inner: Inner

        class Inner(BaseSettings):
            x: int

        _ = Settings.model_rebuild()
        with temp_environ(test__inner__x="1"):
            settings = load_settings(Settings)
        assert settings.inner.x == 1

    def test_no_files(self) -> None:
        class Settings(CustomBaseSettings): ...

        _ = load_settings(Settings)
