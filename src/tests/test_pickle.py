from __future__ import annotations

from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import booleans, floats, integers, none, text

from utilities.hypothesis import temp_paths
from utilities.pickle import read_pickle, write_pickle


class TestReadAndWritePickle:
    @given(
        obj=booleans() | integers() | floats(allow_nan=False) | text() | none(),
        root=temp_paths(),
    )
    def test_main(self, *, obj: Any, root: Path) -> None:
        write_pickle(obj, path := Path(root, "file"))
        result = read_pickle(path)
        assert result == obj
