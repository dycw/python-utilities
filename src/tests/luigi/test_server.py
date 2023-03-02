from pathlib import Path

from click.testing import CliRunner
from hypothesis import given
from hypothesis.strategies import integers

from utilities.hypothesis import text_ascii
from utilities.luigi.server import _get_args, main


class TestLuigiServer:
    @given(
        pid_file=text_ascii(min_size=1),
        log_dir=text_ascii(min_size=1),
        state_path=text_ascii(min_size=1),
        port=integers(),
    )
    def test_get_args(
        self, pid_file: Path, log_dir: str, state_path: str, port: int
    ) -> None:
        _ = _get_args(
            pid_file=pid_file, log_dir=log_dir, state_path=state_path, port=port
        )

    def test_dry_run(self) -> None:
        runner = CliRunner()
        args = ["--dry-run"]
        result = runner.invoke(main, args)
        assert result.exit_code == 0
