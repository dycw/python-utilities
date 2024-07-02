from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.hypothesis import git_repos, settings_with_reduced_examples
from utilities.python_dotenv import LoadSettingsError, load_settings

if TYPE_CHECKING:
    from pathlib import Path


class TestLoadSettings:
    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_main(self, *, root: Path, value: int) -> None:
        @dataclass(frozen=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=str(value))
        assert settings == expected

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_upper_case_dotenv(self, *, root: Path, value: int) -> None:
        @dataclass(frozen=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"KEY = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=str(value))
        assert settings == expected

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_upper_case_key(self, *, root: Path, value: int) -> None:
        @dataclass(frozen=True)
        class Settings:
            KEY: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(KEY=str(value))
        assert settings == expected

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_error(self, *, root: Path) -> None:
        @dataclass(frozen=True)
        class Settings:
            KEY: str

        with raises(
            LoadSettingsError, match="Unable to load settings; path '.*' must exist"
        ):
            _ = load_settings(Settings, cwd=root)
