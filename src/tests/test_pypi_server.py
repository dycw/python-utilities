from pathlib import Path

from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import integers
from pytest import raises

from utilities.hypothesis import temp_paths
from utilities.hypothesis import text_ascii
from utilities.pypi_server import _check_password_file
from utilities.pypi_server import _get_args
from utilities.pypi_server import main


class TestPypiServer:
    def test_check_password_file_success(self, tmp_path: Path) -> None:
        (path_password := tmp_path.joinpath("password")).touch()
        _check_password_file(path_password=path_password)

    def test_check_password_file_error(self, tmp_path: Path) -> None:
        with raises(FileNotFoundError):
            _check_password_file(path_password=tmp_path.joinpath("password"))

    @given(
        port=integers(),
        root=temp_paths(),
        password=text_ascii(min_size=1),
        packages=text_ascii(min_size=1),
    )
    def test_get_args(
        self,
        port: int,
        root: Path,
        password: str,
        packages: str,
    ) -> None:
        _ = _get_args(
            port=port,
            path_password=root.joinpath(password),
            path_packages=root.joinpath(packages),
        )

    def test_dry_run(self, tmp_path: Path) -> None:
        (path_password := tmp_path.joinpath("password")).touch()
        runner = CliRunner()
        args = ["--path-password", path_password.as_posix(), "--dry-run"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0
