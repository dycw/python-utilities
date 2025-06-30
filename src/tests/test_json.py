from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from tests.conftest import SKIPIF_CI
from utilities.json import run_prettier, write_formatted_json

if TYPE_CHECKING:
    from pathlib import Path


class TestRunPrettier:
    input_: ClassVar[str] = '{"foo":0,"bar":[1,2,3]}'
    output: ClassVar[str] = '{ "foo": 0, "bar": [1, 2, 3] }\n'

    @SKIPIF_CI
    def test_bytes(self) -> None:
        result = run_prettier(self.input_.encode())
        assert result == self.output.encode()

    @SKIPIF_CI
    def test_text(self) -> None:
        result = run_prettier(self.input_)
        assert result == self.output

    @SKIPIF_CI
    def test_path(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("file.json")
        _ = file.write_text(self.input_)
        run_prettier(file)
        contents = file.read_text()
        assert contents == self.output

    @SKIPIF_CI
    def test_file_as_str(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("file.json")
        _ = file.write_text(self.input_)
        _ = run_prettier(str(file))
        contents = file.read_text()
        assert contents == self.output


class TestWriteFormattedJSON:
    input_: ClassVar[str] = '{"foo":0,"bar":[1,2,3]}'
    output: ClassVar[str] = '{ "foo": 0, "bar": [1, 2, 3] }\n'

    @SKIPIF_CI
    def test_main(self, *, tmp_path: Path) -> None:
        file = tmp_path.joinpath("file.json")
        write_formatted_json(self.input_.encode(), file)
        contents = file.read_text()
        assert contents == self.output
