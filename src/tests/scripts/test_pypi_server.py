from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import integers

from utilities.hypothesis import temp_paths, text_ascii
from utilities.pathlib import ensure_path, temp_cwd
from utilities.scripts.pypi_server import _check_password_file, _get_args, main

if TYPE_CHECKING:
    from pathlib import Path


class TestPypiServer:
    def test_check_password_file_success(self, *, tmp_path: Path) -> None:
        (path_password := ensure_path(tmp_path, "password")).touch()
        _check_password_file(path_password=path_password)

    def test_check_password_file_error(self, *, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            _check_password_file(path_password=ensure_path(tmp_path, "password"))

    @given(
        port=integers(),
        root=temp_paths(),
        password=text_ascii(min_size=1),
        packages=text_ascii(min_size=1),
    )
    def test_get_args(
        self, *, port: int, root: Path, password: str, packages: str
    ) -> None:
        _ = _get_args(
            port=port,
            path_password=ensure_path(root, password),
            path_packages=ensure_path(root, packages),
        )

    def test_dry_run(self, *, tmp_path: Path) -> None:
        with temp_cwd(tmp_path):
            path = ensure_path("password")
            path.touch()
            runner = CliRunner()
            args = ["--path-password", str(path), "--dry-run"]
            result = runner.invoke(main, args)
        assert result.exit_code == 0
