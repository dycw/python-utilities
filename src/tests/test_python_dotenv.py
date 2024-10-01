from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

from hypothesis import given
from hypothesis.strategies import DataObject, data, integers, sampled_from
from pytest import raises

from utilities.hypothesis import git_repos, settings_with_reduced_examples, text_ascii
from utilities.python_dotenv import (
    _LoadSettingsEmptyError,
    _LoadSettingsFileNotFoundError,
    _LoadSettingsNonUniqueError,
    _LoadSettingsTypeError,
    load_settings,
)
from utilities.sentinel import Sentinel


class TestLoadSettings:
    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_main(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=str(value))
        assert settings == expected

    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_upper_case_dotenv(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"KEY = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=str(value))
        assert settings == expected

    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_upper_case_key(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            KEY: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(KEY=str(value))
        assert settings == expected

    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_extra_key(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")
            _ = fh.write(f"other = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=str(value))
        assert settings == expected

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_int(self, *, root: Path, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: int

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=value)
        assert settings == expected

    @given(data=data(), root=git_repos())
    @settings_with_reduced_examples()
    def test_enum(self, *, data: DataObject, root: Path) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Truth

        value = data.draw(sampled_from(Truth))
        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value.name}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=value)
        assert settings == expected

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_error_file_not_found(self, *, root: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            KEY: str

        with raises(_LoadSettingsFileNotFoundError, match=r"Path '.*' must exist"):
            _ = load_settings(Settings, cwd=root)

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_error_field_missing(self, *, root: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        root.joinpath(".env").touch()

        with raises(_LoadSettingsEmptyError, match=r"Field 'key' must exist"):
            _ = load_settings(Settings, cwd=root)

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_error_field_duplicated(self, *, root: Path, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")
            _ = fh.write(f"KEY = {value}\n")

        with raises(
            _LoadSettingsNonUniqueError,
            match=r"Field 'key' must exist exactly once; got .*",
        ):
            _ = load_settings(Settings, cwd=root)

    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_error_type(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Sentinel

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        with raises(
            _LoadSettingsTypeError, match=r"Field 'key' has unsupported type .*"
        ):
            _ = load_settings(Settings, cwd=root)
