from __future__ import annotations

import re
from dataclasses import dataclass
from re import DOTALL
from typing import TYPE_CHECKING

from hypothesis import given
from hypothesis.strategies import DataObject, booleans, data, integers, sampled_from
from pytest import raises

from utilities.errors import ImpossibleCaseError
from utilities.hypothesis import git_repos, settings_with_reduced_examples, text_ascii
from utilities.os import temp_environ
from utilities.python_dotenv import (
    _LoadSettingsDuplicateKeysError,
    _LoadSettingsFileNotFoundError,
    _LoadSettingsMissingKeysError,
    load_settings,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestLoadSettings:
    @given(
        data=data(),
        root=git_repos(),
        key_file=sampled_from(["key", "KEY"]),
        value_file=text_ascii(),
        use_env=booleans(),
    )
    @settings_with_reduced_examples()
    def test_main(
        self,
        *,
        data: DataObject,
        root: Path,
        key_file: str,
        value_file: str,
        use_env: bool,
    ) -> None:
        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"{key_file} = {value_file}\n")

        @dataclass(kw_only=True, slots=True)
        class SettingsLower:
            key: str

        @dataclass(kw_only=True, slots=True)
        class SettingsUpper:
            KEY: str

        SettingsUse = data.draw(sampled_from([SettingsLower, SettingsUpper]))  # noqa: N806
        if use_env:
            key_env = data.draw(sampled_from(["key", "KEY"]))
            value_env = data.draw(text_ascii())
            with temp_environ({key_env: value_env}):
                settings = load_settings(SettingsUse, cwd=root)
            exp_value = value_env
        else:
            settings = load_settings(SettingsUse, cwd=root)
            exp_value = value_file

        if SettingsUse is SettingsLower:
            expected = SettingsLower(key=exp_value)
        elif SettingsUse is SettingsUpper:
            expected = SettingsUpper(KEY=exp_value)
        else:
            raise ImpossibleCaseError(case=[f"{SettingsUse=}"])
        assert settings == expected

    @given(root=git_repos(), value=text_ascii())
    @settings_with_reduced_examples()
    def test_file_extra_key(self, *, root: Path, value: str) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")
            _ = fh.write(f"other = {value}\n")

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

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_error_duplicate_keys(self, *, root: Path, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")
            _ = fh.write(f"KEY = {value}\n")

        with raises(
            _LoadSettingsDuplicateKeysError,
            match=re.compile(
                r"Mapping .* keys must not contain duplicates \(modulo case\); got .*",
                flags=DOTALL,
            ),
        ):
            _ = load_settings(Settings, cwd=root)

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_error_missing_keys(self, *, root: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: str

        root.joinpath(".env").touch()

        with raises(
            _LoadSettingsMissingKeysError,
            match=r"Unable to load '.*'; missing value\(s\) for 'key'",
        ):
            _ = load_settings(Settings, cwd=root)
