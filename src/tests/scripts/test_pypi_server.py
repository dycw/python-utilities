from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.hypothesis import temp_paths, text_ascii
from utilities.pathlib import temp_cwd
from utilities.scripts.pypi_server import _check_password_file, _get_args, main


class TestPypiServer:
    def test_check_password_file_success(self, *, tmp_path: Path) -> None:
        (path_password := Path(tmp_path, "password")).touch()
        _check_password_file(path_password=path_password)

    def test_check_password_file_error(self, *, tmp_path: Path) -> None:
        with raises(FileNotFoundError):
            _check_password_file(path_password=Path(tmp_path, "password"))

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
            path_password=Path(root, password),
            path_packages=Path(root, packages),
        )

    def test_dry_run(self, *, tmp_path: Path) -> None:
        with temp_cwd(tmp_path):
            path = Path("password")
            path.touch()
            runner = CliRunner()
            args = ["--path-password", str(path), "--dry-run"]
            result = runner.invoke(main, args)
        assert result.exit_code == 0
