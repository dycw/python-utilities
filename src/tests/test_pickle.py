from pathlib import Path
from typing import Any

import pandas as pd
from hypothesis import given
from hypothesis.strategies import booleans, floats, integers, none, text

from utilities.hypothesis import temp_paths
from utilities.pickle import read_pickle, write_pickle


class TestReadAndWritePickle:
    @given(
        obj=booleans() | integers() | floats(allow_nan=False) | text() | none(),
        root=temp_paths(),
    )
    def test_main(self, obj: Any, root: Path) -> None:
        write_pickle(obj, path := root.joinpath("file"))
        result = read_pickle(path)
        assert result == obj

    def test_pandas(self, tmp_path: Path) -> None:
        write_pickle(None, path := tmp_path.joinpath("file"))
        result = pd.read_pickle(path, compression="gzip")  # noqa: S301
        assert result is None
