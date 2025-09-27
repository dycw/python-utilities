from __future__ import annotations

import json
from stat import S_IXUSR
from subprocess import STDOUT, CalledProcessError, check_call, check_output
from typing import TYPE_CHECKING, ClassVar

import tomlkit
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from pytest import mark, param

from utilities.os import temp_environ
from utilities.pydantic_settings import (
    CustomBaseSettings,
    HashableBaseSettings,
    PathLikeOrWithSection,
    load_settings,
)
from utilities.text import strip_and_dedent

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

    @mark.parametrize("inner_cls", [param(BaseSettings), param(HashableBaseSettings)])
    def test_env_var_with_prefix_and_nested(
        self, *, inner_cls: type[BaseSettings]
    ) -> None:
        class Settings(CustomBaseSettings):
            model_config = SettingsConfigDict(env_prefix="test__")
            inner: Inner

        class Inner(inner_cls):
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


class TestLoadSettingsCLI:
    def test_main(self, *, tmp_path: Path) -> None:
        script = tmp_path.joinpath("script.py")
        _ = script.write_text("""\
#!/usr/bin/env python3
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings

from utilities.pydantic_settings import CustomBaseSettings, PathLikeOrWithSection, load_settings_cli

class _Settings(CustomBaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
        Path(__file__).parent.joinpath("config.toml")
    ]
    parse_cli: ClassVar[bool] = True

    a: int
    b: int
    inner: _Inner

class _Inner(BaseSettings):
    c: int
    d: int

_Settings.model_rebuild()

def main() -> None:
    settings = load_settings_cli(_Settings)
    print(f"{settings=}")


for name, field in _Settings.model_fields.items():
    print(name, field.annotation, field.default)


if __name__ == "__main__":
    main()
""")
        script.chmod(script.stat().st_mode | S_IXUSR)
        config = tmp_path.joinpath("config.toml")
        _ = config.write_text(
            """\
a = 1
b = 2

[inner]
c = 3
d = 4
"""
        )
        try:
            result = check_output([script, "-h"], stderr=STDOUT, text=True)
        except CalledProcessError as error:
            raise RuntimeError(error.stdout) from None
        expected = """settings=_Settings(a=1, b=2, inner=_Inner(c=3, d=4))\n"""
        assert result == expected

        try:
            result = check_output([script], stderr=STDOUT, text=True)
        except CalledProcessError as error:
            raise RuntimeError(error.stdout) from None
        expected = """settings=_Settings(a=1, b=2, inner=_Inner(c=3, d=4))\n"""
        assert result == expected
