from pathlib import Path
from re import search
from subprocess import CalledProcessError, check_call

from pytest import raises

from utilities.subprocess import (
    MultipleActivateError,
    NoActivateError,
    _address_already_in_use_pattern,
    get_shell_output,
    tabulate_called_process_error,
)
from utilities.text import strip_and_dedent


class TestGetShellOutput:
    def test_main(self) -> None:
        output = get_shell_output("ls")
        assert any(line == "pyproject.toml" for line in output.splitlines())

    def test_activate(self, tmp_path: Path) -> None:
        venv = tmp_path.joinpath(".venv")
        activate = venv.joinpath("activate")
        activate.parent.mkdir(parents=True)
        activate.touch()
        _ = get_shell_output("ls", cwd=venv, activate=venv)

    def test_no_activate(self, tmp_path: Path) -> None:
        venv = tmp_path.joinpath(".venv")
        with raises(NoActivateError):
            _ = get_shell_output("ls", cwd=venv, activate=venv)

    def test_multiple_activates(self, tmp_path: Path) -> None:
        venv = tmp_path.joinpath(".venv")
        for i in range(2):
            activate = venv.joinpath(str(i), "activate")
            activate.parent.mkdir(parents=True)
            activate.touch()
        with raises(MultipleActivateError):
            _ = get_shell_output("ls", cwd=venv, activate=venv)


class TestAddressAlreadyInUsePattern:
    def test_pattern(self) -> None:
        pattern = _address_already_in_use_pattern()
        text = "OSError: [Errno 98] Address already in use"
        assert search(pattern, text) is not None


class TestTabulateCalledProcessError:
    def test_main(self) -> None:
        def which() -> None:
            _ = check_call(["which"], text=True)

        try:
            which()
        except CalledProcessError as error:
            result = tabulate_called_process_error(error)
            expected = """
                cmd        ['which']
                returncode 1
                stdout     None
                stderr     None
            """
            assert result == strip_and_dedent(expected)
        else:
            with raises(CalledProcessError):
                which()
