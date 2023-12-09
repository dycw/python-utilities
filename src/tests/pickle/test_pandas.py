from __future__ import annotations

from pathlib import Path
from typing import Any

from hypothesis import given
from hypothesis.strategies import booleans, floats, integers, none, text
from pandas import read_pickle as _read_pickle

from utilities.hypothesis import temp_paths
from utilities.pathvalidate import valid_path
from utilities.pickle import write_pickle


class TestReadAndWritePickle:
    @given(
        obj=booleans() | integers() | floats(allow_nan=False) | text() | none(),
        root=temp_paths(),
    )
    def test_main(self, *, obj: Any, root: Path) -> None:
        write_pickle(obj, path := valid_path(root, "file"))
        result = _read_pickle(path, compression="gzip")  # noqa: S301
        assert result == obj
