from __future__ import annotations

import json
from stat import S_IXUSR
from subprocess import STDOUT, CalledProcessError, check_output
from typing import TYPE_CHECKING, ClassVar

import tomlkit
import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from pytest import mark, param

from tests.conftest import SKIPIF_CI_AND_WINDOWS
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


class TestLoadSettings:
    @mark.parametrize(
        ("args", "expected"),
        [
            param([], "settings=_Settings(a=1, b=2, inner=_Inner(c=3, d=4))"),
            param(["-a", "5"], "settings=_Settings(a=5, b=2, inner=_Inner(c=3, d=4))"),
            param(
                ["--inner.c", "5"],
                "settings=_Settings(a=1, b=2, inner=_Inner(c=5, d=4))",
            ),
            param(
                ["-h"],
                """
usage: script.py [-h] [-a int] [-b int] [--inner [JSON]] [--inner.c int]
                 [--inner.d int]

options:
  -h, --help      show this help message and exit
  -a int          (default: 1)
  -b int          (default: 2)

inner options:
  --inner [JSON]  set inner from JSON string (default: {})
  --inner.c int   (default: 3)
  --inner.d int   (default: 4)
""",
            ),
        ],
    )
    @SKIPIF_CI_AND_WINDOWS
    def test_cli(self, *, tmp_path: Path, args: list[str], expected: str) -> None:
        script = tmp_path.joinpath("script.py")
        _ = script.write_text("""\
#!/usr/bin/env python3
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings

from utilities.pydantic_settings import CustomBaseSettings, PathLikeOrWithSection, load_settings

class _Settings(CustomBaseSettings):
    toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [
        Path(__file__).parent.joinpath("config.toml")
    ]

    a: int
    b: int
    inner: _Inner

class _Inner(BaseSettings):
    c: int
    d: int

def main() -> None:
    settings = load_settings(_Settings, cli=True)
    print(f"{settings=}")

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
            result = check_output([script, *args], stderr=STDOUT, text=True).strip("\n")
        except CalledProcessError as error:
            raise RuntimeError(error.stdout) from None
        assert result == expected.strip("\n")

    def test_cli_coverage(self, *, tmp_path: Path) -> None:
        config = tmp_path.joinpath("config.toml")
        _ = config.write_text("""
a = 1

[inner]
b = 2""")

        class Example(CustomBaseSettings):
            toml_files: ClassVar[Sequence[PathLikeOrWithSection]] = [config]

            a: int
            inner: _Inner

        class _Inner(BaseSettings):
            b: int

        _ = Example.model_rebuild()
        _ = load_settings(Example, cli=True)
