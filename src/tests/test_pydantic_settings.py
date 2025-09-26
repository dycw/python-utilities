from __future__ import annotations

import json
from typing import TYPE_CHECKING, ClassVar

import tomlkit
import yaml
from hypothesis import HealthCheck, Phase, given, reproduce_failure, settings
from pydantic_settings import BaseSettings, SettingsConfigDict
from pytest import RaisesGroup, approx, fixture, mark, param, raises, skip
from pytest_benchmark.fixture import BenchmarkFixture
from pytest_lazy_fixtures import lf
from pytest_regressions.dataframe_regression import DataFrameRegressionFixture

from utilities.contextvars import set_global_breakpoint
from utilities.os import temp_environ
from utilities.pydantic_settings import (
    CustomBaseSettings,
    HashableBaseSettings,
    PathLikeOrWithSection,
    load_settings,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


class TestCustomBaseSettings:
    def test_hashable(self) -> None:
        class Settings(CustomBaseSettings):
            x: int = 1

        settings = load_settings(Settings)
        _ = hash(settings)

    def test_json(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.json")
        _ = file.write_text(json.dumps({"x": 1}))

        class Settings(CustomBaseSettings):
            json_files: ClassVar[Sequence[PathLikeOrWithSection]] = [file]
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_json_section_str(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.json")
        _ = file.write_text(json.dumps({"outer": {"x": 1}}))

        class Settings(CustomBaseSettings):
            json_files: ClassVar[Sequence[PathLikeOrWithSection]] = [(file, "outer")]
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_json_section_nested(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.json")
        _ = file.write_text(json.dumps({"outer": {"middle": {"x": 1}}}))

        class Settings(CustomBaseSettings):
            json_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
                (file, ["outer", "middle"])
            ]
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_toml(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.toml")
        _ = file.write_text(tomlkit.dumps({"x": 1}))

        class Settings(CustomBaseSettings):
            toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [file]
            x: int

        settings = load_settings(Settings)
        assert settings.x == 1

    def test_yaml(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("settings.yaml")
        _ = file.write_text(yaml.dump({"x": 1}))

        class Settings(CustomBaseSettings):
            yaml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [file]
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

    @mark.parametrize("inner_cls", [param(BaseSettings), param(HashableBaseSettings)])
    def test_env_var_with_nested(self, *, inner_cls: type[BaseSettings]) -> None:
        class Settings(CustomBaseSettings):
            inner: Inner

        class Inner(inner_cls):
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


class TestHashableBaseSettings:
    def test_hashable(self) -> None:
        class Settings(HashableBaseSettings):
            x: int = 1

        settings = load_settings(Settings)
        _ = hash(settings)
