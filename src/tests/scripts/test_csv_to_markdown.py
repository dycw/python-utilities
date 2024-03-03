from __future__ import annotations

from csv import writer
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from utilities.pathlib import ensure_path
from utilities.platform import IS_WINDOWS
from utilities.scripts.csv_to_markdown import _csv_to_markdown, main
from utilities.text import strip_and_dedent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def path_csv(*, tmp_path: Path) -> Path:
    path = ensure_path(tmp_path, "input.csv")
    with path.open(mode="w", newline="" if IS_WINDOWS else None) as fh:
        csv_writer = writer(fh)
        csv_writer.writerow(["Foo", "Bar"])
        csv_writer.writerow(["1,1", "1,2"])
        csv_writer.writerow(["2,1", "2,2"])
    return path


class TestCSVToMarkdown:
    def test_main(self, *, path_csv: Path) -> None:
        result = _csv_to_markdown(path=path_csv)
        expected = """
            |Foo|Bar|
            | :---: | :---: |
            |1,1|1,2|
            |2,1|2,2|
        """
        assert result == strip_and_dedent(expected)

    def test_stdout(self, *, path_csv: Path) -> None:
        runner = CliRunner()
        args = ["--path", str(path_csv)]
        result = runner.invoke(main, args)
        assert result.exit_code == 0
