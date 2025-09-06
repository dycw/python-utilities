from __future__ import annotations

import re
from json import dumps
from subprocess import check_call
from typing import TYPE_CHECKING, ClassVar

from tests.conftest import SKIPIF_CI
from utilities.iterables import one
from utilities.os import temp_environ
from utilities.pydantic_settings import PathLikeOrWithSection, load_settings
from utilities.pydantic_settings_sops import SopsBaseSettings
from utilities.re import extract_group

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


class TestSOPSBaseSettings:
    @SKIPIF_CI
    def test_main(self, *, tmp_path: Path) -> None:
        unencrypted_file = tmp_path.joinpath("unencrypted.json")
        _ = unencrypted_file.write_text(dumps({"x": 1, "y": 2}))
        key_file = tmp_path.joinpath("keys.txt")
        with key_file.open(mode="w") as file:
            _ = check_call(["age-keygen"], stdout=file)
        pattern = re.compile(r"^# public key: (age.+)$")
        public_line = one(
            line for line in key_file.read_text().splitlines() if pattern.search(line)
        )
        public_key = extract_group(pattern, public_line)
        encrypted_file = tmp_path.joinpath("encrypted.json")
        with (
            temp_environ(SOPS_AGE_RECIPIENTS=public_key),
            encrypted_file.open(mode="w") as file,
        ):
            _ = check_call(["sops", "encrypt", str(unencrypted_file)], stdout=file)

        class Settings(SopsBaseSettings):
            secret_files: ClassVar[Sequence[PathLikeOrWithSection]] = [encrypted_file]
            x: int
            y: int

        with temp_environ(SOPS_AGE_KEY_FILE=str(key_file)):
            settings = load_settings(Settings)
        assert settings.x == 1
        assert settings.y == 2
