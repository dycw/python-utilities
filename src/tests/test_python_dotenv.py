from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from re import DOTALL
from typing import TYPE_CHECKING, Literal

from hypothesis import given
from hypothesis.strategies import DataObject, booleans, data, integers, sampled_from
from pytest import raises

from utilities.errors import ImpossibleCaseError
from utilities.hypothesis import (
    git_repos,
    paths,
    settings_with_reduced_examples,
    text_ascii,
)
from utilities.os import temp_environ
from utilities.python_dotenv import (
    _LoadSettingsDuplicateKeysError,
    _LoadSettingsEmptyError,
    _LoadSettingsFileNotFoundError,
    _LoadSettingsInvalidBoolError,
    _LoadSettingsInvalidEnumError,
    _LoadSettingsInvalidIntError,
    _LoadSettingsTypeError,
    load_settings,
)
from utilities.sentinel import Sentinel

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

    @given(data=data(), root=git_repos(), value=booleans())
    @settings_with_reduced_examples()
    def test_bool_value(self, *, data: DataObject, root: Path, value: bool) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: bool

        value_write = data.draw(
            sampled_from([
                int(value),
                str(value),
                str(value).lower(),
                str(value).upper(),
            ])
        )
        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value_write}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=value)
        assert settings == expected

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_bool_value_error(self, *, root: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: bool

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write("key = '...'\n")

        with raises(
            _LoadSettingsInvalidBoolError,
            match=r"Field 'key' must contain a valid boolean; got '...'",
        ):
            _ = load_settings(Settings, cwd=root)

    @given(root=git_repos(), value=integers())
    @settings_with_reduced_examples()
    def test_int_value(self, *, root: Path, value: int) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: int

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root)
        expected = Settings(key=value)
        assert settings == expected

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_int_value_error(self, *, root: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: int

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write("key = '...'\n")

        with raises(
            _LoadSettingsInvalidIntError,
            match=r"Field 'key' must contain a valid integer; got '...'",
        ):
            _ = load_settings(Settings, cwd=root)

    @given(data=data(), root=git_repos())
    @settings_with_reduced_examples()
    def test_enum_value(self, *, data: DataObject, root: Path) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Truth

        value = data.draw(sampled_from(Truth))
        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value.name}\n")

        settings = load_settings(Settings, cwd=root, localns=locals())
        expected = Settings(key=value)
        assert settings == expected

    @given(root=git_repos())
    @settings_with_reduced_examples()
    def test_enum_value_error(self, *, root: Path) -> None:
        class Truth(Enum):
            true = auto()
            false = auto()

        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Truth

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write("key = ...\n")

        with raises(
            _LoadSettingsInvalidEnumError,
            match=r"Field '.*' must contain a valid member of '.*'; got '...'",
        ):
            _ = load_settings(Settings, cwd=root, localns=locals())

    @given(root=git_repos(), value=sampled_from(["true", "false"]))
    @settings_with_reduced_examples()
    def test_literal_value(
        self, *, root: Path, value: Literal["true", "false"]
    ) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Literal["true", "false"]

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

        settings = load_settings(Settings, cwd=root, localns={"Literal": Literal})
        expected = Settings(key=value)
        assert settings == expected

    @given(root=git_repos(), value=paths())
    @settings_with_reduced_examples()
    def test_path_value(self, *, root: Path, value: Path) -> None:
        @dataclass(kw_only=True, slots=True)
        class Settings:
            key: Path

        with root.joinpath(".env").open(mode="w") as fh:
            _ = fh.write(f"key = {value}\n")

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

        with raises(
            _LoadSettingsEmptyError, match=r"Field 'key' must exist \(modulo case\)"
        ):
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
            _ = load_settings(Settings, cwd=root, localns={"Sentinel": Sentinel})
